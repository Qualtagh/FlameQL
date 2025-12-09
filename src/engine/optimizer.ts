import { WhereFilterOp } from '@google-cloud/firestore';
import { Field, OrderBySpec, Predicate } from '../api/expression';
import { Constraint } from './ast';
import { IndexManager } from './indexes/index-manager';
import { SortOrder } from './operators/operator';
import { toDNF } from './utils/predicate-utils';

export interface PlannedScan {
  predicate: Predicate;
  constraints: ConstraintWithScore;
}

export interface ConstraintWithScore {
  constraints: Constraint[];
  nonIndexable: number;
}

export interface OptimizationResult {
  strategy: 'SINGLE_SCAN' | 'UNION_SCAN';
  scans: PlannedScan[];
  score: number;
}

export class Optimizer {
  constructor(private indexManager: IndexManager) { }

  optimize(predicate: Predicate, collectionPath: string, orderBy?: OrderBySpec[]): OptimizationResult {
    const dnfPredicate = toDNF(predicate);
    const dnfPlans = this.extractScansFromDNF(dnfPredicate).map(p => this.buildPlan(p));
    const singlePlan = this.buildPlan(predicate);

    const dnfScore = this.scoreScans(dnfPlans, collectionPath, orderBy);
    const singleScore = this.scoreScans([singlePlan], collectionPath, orderBy);

    if (dnfScore < singleScore) {
      return { strategy: 'UNION_SCAN', scans: dnfPlans, score: dnfScore };
    }

    return { strategy: 'SINGLE_SCAN', scans: [singlePlan], score: singleScore };
  }

  private extractScansFromDNF(predicate: Predicate): Predicate[] {
    if (predicate.type === 'OR') {
      return predicate.conditions;
    }
    return [predicate];
  }

  private buildPlan(predicate: Predicate): PlannedScan {
    const constraints: Constraint[] = [];
    const nonIndexable = this.extractConstraints(predicate, constraints);
    return { predicate, constraints: { constraints, nonIndexable } };
  }

  private scoreScans(scans: PlannedScan[], collectionPath: string, orderBy?: OrderBySpec[]): number {
    let total = 0;
    for (const scan of scans) {
      total += this.scoreScan(scan, collectionPath, orderBy);
    }
    return total;
  }

  private scoreScan(plan: PlannedScan, collectionPath: string, orderBy?: OrderBySpec[]): number {
    this.validateFirestoreGuardrails(plan.constraints.constraints, orderBy);
    const sortOrder = this.toSortOrder(orderBy);
    const match = this.indexManager.match(collectionPath, plan.constraints.constraints, sortOrder);

    let score = 0;
    if (match.type === 'exact') {
      score = 1;
    } else if (match.type === 'partial') {
      score = Math.max(1, 10 - match.matchedFields) + 5;
    } else {
      score = 1000;
    }

    score += plan.constraints.nonIndexable * 100;
    return score;
  }

  private toSortOrder(orderBy?: OrderBySpec[]): SortOrder | undefined {
    if (!orderBy || orderBy.length === 0) return undefined;
    const primary = orderBy[0];
    return {
      field: primary.field.path.join('.'),
      direction: primary.direction,
    };
  }

  private extractConstraints(predicate: Predicate, constraints: Constraint[]): number {
    let nonIndexable = 0;

    if (predicate.type === 'AND') {
      for (const c of predicate.conditions) {
        nonIndexable += this.extractConstraints(c, constraints);
      }
      return nonIndexable;
    }

    if (predicate.type === 'COMPARISON') {
      const field = this.asField(predicate.left);
      const literal = predicate.right.kind === 'Literal' ? predicate.right : null;
      if (field && literal) {
        constraints.push({
          field,
          op: predicate.operation,
          value: literal,
        });
      } else {
        nonIndexable += 1;
      }
      return nonIndexable;
    }

    if (predicate.type === 'OR' || predicate.type === 'NOT') {
      return 1;
    }

    return 0;
  }

  private asField(expr: any): Field | null {
    if (expr && typeof expr === 'object' && expr.kind === 'Field' && expr.source) {
      return expr as Field;
    }
    return null;
  }

  private validateFirestoreGuardrails(constraints: Constraint[], orderBy?: OrderBySpec[]) {
    const inequalityOps = new Set<WhereFilterOp>(['<', '<=', '>', '>=', '!=', 'not-in']);
    const inequalityFields = new Set<string>();

    for (const c of constraints) {
      if (inequalityOps.has(c.op)) {
        inequalityFields.add(c.field.path.join('.'));
      }
    }

    if (inequalityFields.size > 1) {
      throw new Error(`Firestore allows at most one inequality field per query (found: ${Array.from(inequalityFields).join(', ')}).`);
    }

    if (inequalityFields.size === 1 && orderBy && orderBy.length > 0) {
      const inequalityField = Array.from(inequalityFields)[0];
      const orderField = orderBy[0].field.path.join('.');
      if (orderField !== inequalityField) {
        throw new Error('When using an inequality filter, the first orderBy field must match the inequality field.');
      }
    }
  }
}
