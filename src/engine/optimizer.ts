import { Predicate } from './ast';
import { Index } from './indexes/index-definitions';
import { IndexManager } from './indexes/index-manager';
import { toDNF } from './utils/predicate-utils';

export interface OptimizationResult {
  strategy: 'SINGLE_SCAN' | 'UNION_SCAN';
  scans: Predicate[]; // Each predicate represents the constraints for a single scan
}

export class Optimizer {
  constructor(private indexManager: IndexManager) { }

  optimize(predicate: Predicate, collectionPath: string): OptimizationResult {
    // 1. Try DNF strategy (Union of Scans)
    const dnfPredicate = toDNF(predicate);
    const dnfScans = this.extractScansFromDNF(dnfPredicate);
    const dnfScore = this.scoreScans(dnfScans, collectionPath);

    // 2. Try Single Scan strategy (CNF-like / Original)
    // For single scan, we treat the entire predicate as one set of constraints
    // We approximate this by finding the "best" AND-subset of the predicate that matches an index.
    const singleScanConstraints = this.extractTopLevelConstraints(predicate);
    const singleScanScore = this.scoreScans([singleScanConstraints], collectionPath);

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
        scans: [predicate], // Pass the original predicate; Executor will separate indexable vs filter
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
    // If top level is AND, we can potentially use all of it.
    // If top level is OR, we can't use it for a single scan unless we filter everything in memory.
    // In that case, we return a "TRUE" predicate (full scan) or maybe just one branch?
    // Actually, for single scan of an OR, we have to do a full scan (or scan by one branch and filter).
    // Let's assume full scan if it's an OR.
    if (predicate.type === 'OR') {
      return { type: 'CONSTANT', value: true };
    }
    return predicate;
  }

  private scoreScans(scans: Predicate[], collectionPath: string): number {
    let totalScore = 0;
    for (const scan of scans) {
      totalScore += this.scoreScan(scan, collectionPath);
    }
    return totalScore;
  }

  private scoreScan(predicate: Predicate, collectionPath: string): number {
    // 1. Extract fields involved in equality and inequality
    // indexableFields: fields that can be used in a single index (top-level ANDs)
    // allFields: all fields mentioned in the predicate
    const indexable = this.extractIndexableFields(predicate);
    const allFields = this.extractAllFields(predicate);

    // 2. Find best matching index using ONLY indexable fields
    const bestIndex = this.findBestIndex(indexable.equalityFields, indexable.inequalityFields, collectionPath);

    let score = 0;
    let matchedFieldsCount = 0;

    if (bestIndex) {
      // Base score for a scan
      score = 1;

      // Calculate how many fields were matched
      // (This logic duplicates findBestIndex slightly but we need the count)
      const queryEqualitySet = new Set(indexable.equalityFields);
      for (const field of bestIndex.fields) {
        if (queryEqualitySet.has(field.fieldPath)) {
          matchedFieldsCount++;
        } else {
          break;
        }
      }
      // TODO: Handle inequality match count
    } else {
      // Full scan penalty
      score = 1000;
    }

    // 3. Calculate residual penalty
    // Fields that are in allFields but not matched by the index
    // Note: This is a heuristic.
    const uniqueAllFields = new Set([...allFields.equalityFields, ...allFields.inequalityFields]);
    const residualCount = uniqueAllFields.size - matchedFieldsCount;

    // Penalty for each residual field (client-side filtering)
    // We make this high enough to prefer using indexes
    score += residualCount * 5;

    return score;
  }

  private stripAlias(fieldPath: string): string {
    // Strip alias prefix (e.g., "u.age" -> "age")
    const parts = fieldPath.split('.');
    return parts.length > 1 ? parts.slice(1).join('.') : fieldPath;
  }

  private extractIndexableFields(predicate: Predicate): { equalityFields: string[], inequalityFields: string[] } {
    const equalityFields: string[] = [];
    const inequalityFields: string[] = [];

    const visit = (p: Predicate) => {
      if (p.type === 'AND') {
        p.conditions.forEach(visit);
      } else if (p.type === 'COMPARISON') {
        const fieldName = this.stripAlias(p.left);
        if (p.operation === '==' || p.operation === 'array-contains') {
          equalityFields.push(fieldName);
        } else {
          inequalityFields.push(fieldName);
        }
      }
      // Stop at OR, NOT, etc.
    };

    visit(predicate);
    return { equalityFields, inequalityFields };
  }

  private extractAllFields(predicate: Predicate): { equalityFields: string[], inequalityFields: string[] } {
    const equalityFields: string[] = [];
    const inequalityFields: string[] = [];

    const visit = (p: Predicate) => {
      if (p.type === 'AND' || p.type === 'OR') {
        p.conditions.forEach(visit);
      } else if (p.type === 'NOT') {
        visit(p.operand);
      } else if (p.type === 'COMPARISON') {
        const fieldName = this.stripAlias(p.left);
        if (p.operation === '==' || p.operation === 'array-contains') {
          equalityFields.push(fieldName);
        } else {
          inequalityFields.push(fieldName);
        }
      }
    };

    visit(predicate);
    return { equalityFields, inequalityFields };
  }

  private findBestIndex(equalityFields: string[], inequalityFields: string[], collectionPath: string): Index | null {
    // Firestore Index Selection Rules:
    // 1. Equality fields can be in any order in the index (prefix).
    // 2. Inequality field must follow equality fields.
    // 3. Only one inequality field is allowed per query (but we might have multiple constraints on it).

    // We look for an index that covers as many equality fields as possible,
    // and optionally the inequality field.

    const indexes = this.indexManager.getIndexes(collectionPath);
    let bestIndex: Index | null = null;
    let maxMatchedFields = -1;

    for (const index of indexes) {
      // Check collection scope (simplified)
      // TODO: Handle collection groups properly
      // if (index.collectionGroup !== collectionPath) continue;

      let matchedCount = 0;
      let indexIdx = 0;

      // Match equality fields
      // In a real Firestore index, equality fields can be in any order at the start?
      // Actually, for a composite index (A, B), you can query A==1, B==2.
      // But you can't easily query B==2 if the index is (A, B) without A.
      // So the index prefix must match the set of equality fields.

      // However, Firestore query planner is smart enough to use an index (A, B) for A==1.
      // So we just need to check if the query's equality fields are a prefix of the index?
      // No, the query's equality fields must be a *subset* of the index's prefix?
      // Wait, if Index is (A, B), and Query is A==1, we can use it.
      // If Query is B==1, we cannot use (A, B).
      // So, the index fields must appear in the query's equality fields, in order, until we stop?
      // Actually, Firestore requires the index to match the query constraints.
      // If I have Index(A, B), I can satisfy A==1 && B==2.
      // I can also satisfy A==1.

      // Let's assume strict prefix matching for now.

      const queryEqualitySet = new Set(equalityFields);

      // Check how many initial fields of the index are in our equality set
      for (const field of index.fields) {
        if (queryEqualitySet.has(field.fieldPath)) {
          matchedCount++;
          indexIdx++;
        } else {
          // Stop at first non-matching field for equality
          break;
        }
      }

      // Now check if the next field in index matches our inequality field (if any)
      if (inequalityFields.length > 0) {
        // We can only handle one inequality field
        const ineqField = inequalityFields[0];
        if (indexIdx < index.fields.length && index.fields[indexIdx].fieldPath === ineqField) {
          matchedCount++;
        }
      }

      if (matchedCount > maxMatchedFields) {
        maxMatchedFields = matchedCount;
        bestIndex = index;
      }
    }

    // If we matched at least one field, return the index.
    // (Or should we require matching ALL query fields?
    // No, we want the *best* index. Even a partial match is better than none.)
    // But if we have fields A, B and Index A, we still have to filter B manually.
    // That's fine, score is still better than full scan.

    return maxMatchedFields > 0 ? bestIndex : null;
  }
}
