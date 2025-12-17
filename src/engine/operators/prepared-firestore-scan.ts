import * as admin from 'firebase-admin';
import { and, constant } from '../../api/api';
import { Predicate } from '../../api/expression';
import { Constraint, ExecutionNode, FilterNode, NodeType, ScanNode } from '../ast';
import { evaluatePredicate } from '../evaluator';
import { buildFirestoreQuery, docToAliasedRow, FirestoreOrderBy, FirestoreWhereConstraint } from '../utils/firestore-utils';

export interface PreparedFirestoreScanPlan {
  scan: ScanNode;
  /**
   * Full right-side predicate to apply post-fetch for correctness.
   * This includes non-indexable FilterNode predicates if present.
   */
  postFilter: Predicate;
  /**
   * The ScanNode constraints compiled to Firestore-level field paths + raw values.
   */
  baseWhere: FirestoreWhereConstraint[];
}

export interface PreparedFirestoreCursorOptions {
  extraWhere?: FirestoreWhereConstraint[];
  includeBaseWhere?: boolean;
  orderBy?: FirestoreOrderBy[];
  includeScanOrderBy?: boolean;
  limit?: number;
  offset?: number;
  includeScanLimitOffset?: boolean;
}

export class PreparedFirestoreCursor {
  private iterator: AsyncIterator<admin.firestore.QueryDocumentSnapshot>;
  private exhausted = false;

  constructor(
    private plan: PreparedFirestoreScanPlan,
    query: admin.firestore.Query
  ) {
    const stream = query.stream() as AsyncIterable<admin.firestore.QueryDocumentSnapshot>;
    this.iterator = stream[Symbol.asyncIterator]();
  }

  async next(): Promise<any | null> {
    if (this.exhausted) return null;

    while (true) {
      const { value, done } = await this.iterator.next();
      if (done || !value) {
        this.exhausted = true;
        return null;
      }

      const row = docToAliasedRow(this.plan.scan.alias, value);
      if (!evaluatePredicate(this.plan.postFilter, row)) {
        continue;
      }

      return row;
    }
  }
}

export class PreparedFirestoreScan {
  readonly plan: PreparedFirestoreScanPlan;

  constructor(
    private db: admin.firestore.Firestore,
    node: ExecutionNode
  ) {
    this.plan = prepareFirestoreScanPlan(node);
  }

  createCursor(opts?: PreparedFirestoreCursorOptions): PreparedFirestoreCursor {
    const {
      extraWhere = [],
      includeBaseWhere = true,
      orderBy,
      includeScanOrderBy = false,
      limit,
      offset,
      includeScanLimitOffset = false,
    } = opts ?? {};

    const scan = this.plan.scan;
    const combinedWhere = [
      ...includeBaseWhere ? this.plan.baseWhere : [],
      ...extraWhere,
    ];

    // Firestore allows at most one membership filter per query; keep the first and evaluate the rest in-memory.
    const where: FirestoreWhereConstraint[] = [];
    let membershipUsed = false;
    for (const constraint of combinedWhere) {
      if (isMembershipConstraint(constraint)) {
        if (membershipUsed) continue;
        membershipUsed = true;
      }
      where.push(constraint);
    }

    const resolvedOrderBy: FirestoreOrderBy[] | undefined = orderBy ?? (
      includeScanOrderBy && scan.orderBy && scan.orderBy.length > 0
        ? scan.orderBy.map(o => ({ fieldPath: o.field.path.join('.'), direction: o.direction }))
        : undefined
    );

    const resolvedOffset = offset ?? (includeScanLimitOffset ? scan.offset : undefined);
    const resolvedLimit = limit ?? (includeScanLimitOffset ? scan.limit : undefined);

    const query = buildFirestoreQuery(this.db, {
      collectionPath: scan.collectionPath,
      collectionGroup: scan.collectionGroup,
      where,
      orderBy: resolvedOrderBy,
      offset: resolvedOffset,
      limit: resolvedLimit,
    });

    return new PreparedFirestoreCursor(this.plan, query);
  }
}

export function prepareFirestoreScanPlan(node: ExecutionNode): PreparedFirestoreScanPlan {
  if (node.type === NodeType.SCAN) {
    const scan = node as ScanNode;
    return {
      scan,
      postFilter: predicateFromConstraints(scan.constraints),
      baseWhere: compileConstraints(scan.constraints),
    };
  }

  if (node.type === NodeType.FILTER) {
    const filter = node as FilterNode;
    if (filter.source.type !== NodeType.SCAN) {
      throw new Error('PreparedFirestoreScan currently supports FILTER over SCAN only.');
    }
    const scan = filter.source as ScanNode;
    return {
      scan,
      postFilter: filter.predicate,
      baseWhere: compileConstraints(scan.constraints),
    };
  }

  throw new Error('PreparedFirestoreScan currently supports SCAN or FILTER->SCAN only.');
}

function compileConstraints(constraints: Constraint[]): FirestoreWhereConstraint[] {
  return constraints.map(c => ({
    fieldPath: c.field.path.join('.'),
    op: c.op,
    value: Array.isArray(c.value) ? c.value.map(v => v.value) : c.value.value,
  }));
}

function predicateFromConstraints(constraints: Constraint[]): Predicate {
  if (!constraints.length) return constant(true);
  const conditions: Predicate[] = constraints.map(c => ({
    type: 'COMPARISON',
    left: c.field,
    operation: c.op,
    right: c.value,
  }));
  return conditions.length === 1 ? conditions[0] : and(conditions);
}

function isMembershipConstraint(constraint: FirestoreWhereConstraint): boolean {
  return constraint.op === 'in' || constraint.op === 'not-in' || constraint.op === 'array-contains-any';
}
