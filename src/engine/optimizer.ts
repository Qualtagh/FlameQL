import { Field, Literal, LiteralType } from '../api/expression';
import { Constraint, Predicate } from './ast';
import { IndexManager } from './indexes/index-manager';
import { SortOrder } from './operators/operator';
import { toDNF } from './utils/predicate-utils';

export interface OptimizationResult {
  strategy: 'SINGLE_SCAN' | 'UNION_SCAN';
  scans: Predicate[]; // Each predicate represents the constraints for a single scan
}

export class Optimizer {
  constructor(private indexManager: IndexManager) { }

  optimize(predicate: Predicate, collectionPath: string, sortOrder?: SortOrder): OptimizationResult {
    // 1. Try DNF strategy (Union of Scans)
    const dnfPredicate = toDNF(predicate);
    const dnfScans = this.extractScansFromDNF(dnfPredicate);
    // For DNF, we can only apply sort order if ALL scans support it?
    // Or we do post-sort.
    // For now, let's score DNF without sort preference, or maybe with?
    // If we have sortOrder, and we do Union, we likely need to merge-sort the results.
    // This requires each scan to be sorted.
    const dnfScore = this.scoreScans(dnfScans, collectionPath, sortOrder);

    // 2. Try Single Scan strategy (CNF-like / Original)
    const singleScanConstraints = this.extractTopLevelConstraints(predicate);
    const singleScanScore = this.scoreScans([singleScanConstraints], collectionPath, sortOrder);

    // 3. Compare
    // Lower score is better.
    // If scores are equal, prefer Single Scan (simpler).
    if (dnfScore < singleScanScore) {
      return {
        strategy: 'UNION_SCAN',
        scans: dnfScans,
      };
    } else {
      return {
        strategy: 'SINGLE_SCAN',
        scans: [predicate],
      };
    }
  }

  private extractScansFromDNF(predicate: Predicate): Predicate[] {
    if (predicate.type === 'OR') {
      return predicate.conditions;
    }
    return [predicate];
  }

  private extractTopLevelConstraints(predicate: Predicate): Predicate {
    if (predicate.type === 'OR') {
      return { type: 'CONSTANT', value: true };
    }
    return predicate;
  }

  private scoreScans(scans: Predicate[], collectionPath: string, sortOrder?: SortOrder): number {
    let totalScore = 0;
    for (const scan of scans) {
      totalScore += this.scoreScan(scan, collectionPath, sortOrder);
    }
    return totalScore;
  }

  private scoreScan(predicate: Predicate, collectionPath: string, sortOrder?: SortOrder): number {
    // Convert predicate to constraints
    const constraints: Constraint[] = [];
    const nonIndexableCount = this.extractConstraints(predicate, constraints);

    // Use IndexManager to find best match
    const match = this.indexManager.match(collectionPath, constraints, sortOrder);

    let score = 0;

    if (match.type === 'exact') {
      score = 1;
    } else if (match.type === 'partial') {
      score = 10 - match.matchedFields;
      if (score < 1) score = 1;
      score += 5;
    } else {
      // No index match -> Full Scan
      score = 1000;
    }

    // Penalize for non-indexable predicates (e.g., nested ORs)
    // These require client-side filtering
    score += nonIndexableCount * 100;

    return score;
  }

  private extractConstraints(predicate: Predicate, constraints: Constraint[]): number {
    let nonIndexable = 0;

    if (predicate.type === 'AND') {
      for (const c of predicate.conditions) {
        nonIndexable += this.extractConstraints(c, constraints);
      }
    } else if (predicate.type === 'COMPARISON') {
      const parts = predicate.left.split('.');
      const source = parts.length > 1 ? parts[0] : null;
      const path = parts.length > 1 ? parts.slice(1) : parts;

      constraints.push({
        field: new Field(source, path),
        op: predicate.operation,
        value: new Literal(predicate.right, LiteralType.String),
      });
    } else if (predicate.type === 'OR' || predicate.type === 'NOT') {
      // OR and NOT at this level cannot be pushed to Firestore index
      nonIndexable = 1;
    }
    // CONSTANT is ignored

    return nonIndexable;
  }
}
