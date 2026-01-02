import * as admin from 'firebase-admin';
import { Expression, Field, Predicate } from '../../api/expression';
import { Constraint, ExecutionNode, NodeType, PreparedScanNode, ScanNode } from '../ast';
import { evaluate, evaluatePredicate } from '../evaluator';
import { buildFirestoreQuery, FirestoreOrderBy, FirestoreWhereConstraint, getDocData } from '../utils/firestore-utils';
import { Operator, SortOrder } from './operator';

export interface PreparedFirestoreScanPlan {
  scan: ScanNode;
  /**
   * Full right-side predicate to apply post-fetch for correctness.
   * This includes non-indexable FilterNode predicates if present.
   */
  postFilter?: Predicate;
  /**
   * Raw ScanNode constraints to be compiled with parameters at execution time.
   */
  baseConstraints: Constraint[];
  driver?: {
    fieldPath: string;
    op: admin.firestore.WhereFilterOp;
  };
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

export class PreparedFirestoreScan implements Operator {
  readonly plan: PreparedFirestoreScanPlan;

  private iterator: AsyncIterator<admin.firestore.QueryDocumentSnapshot> | null = null;
  private exhausted = false;

  private driverField?: string;
  private driverOp?: admin.firestore.WhereFilterOp;

  private cursorOpts: PreparedFirestoreCursorOptions = {
    includeBaseWhere: true,
    includeScanOrderBy: false,
    includeScanLimitOffset: false,
  };

  constructor(
    private db: admin.firestore.Firestore,
    node: ExecutionNode,
    private parameters: Record<string, any>
  ) {
    this.plan = prepareFirestoreScanPlan(node);

    if (this.plan.driver) {
      this.driverField = this.plan.driver.fieldPath;
      this.driverOp = this.plan.driver.op;
    }
  }

  setOptions(opts: Partial<PreparedFirestoreCursorOptions>) {
    this.cursorOpts = { ...this.cursorOpts, ...opts };
  }

  getSortOrder(): SortOrder | undefined {
    // Prepared scan generally returns data in the order of the underlying scan,
    // unless overridden by specific index lookups or explicitly sorted.
    // For now, assume it preserves the scan's order.
    const orderBy = this.plan.scan.orderBy;
    if (orderBy && orderBy.length > 0) {
      const first = orderBy[0];
      if (first.field.kind === 'Field') {
        return {
          field: (first.field as Field).path.join('.'),
          direction: first.direction,
        };
      }
    }
    return undefined;
  }

  async next(drivingValue?: any): Promise<any | null> {
    // If a driving value is provided, we restart the scan with this value
    if (drivingValue !== undefined) {
      this.startScan(drivingValue);
    } else if (this.iterator === null && !this.exhausted) {
      this.startScan(undefined);
    }

    if (!this.iterator || this.exhausted) {
      return null;
    }

    while (true) {
      const { value, done } = await this.iterator.next();
      if (done || !value) {
        this.exhausted = true;
        this.iterator = null;
        return null;
      }

      const row = { [this.plan.scan.alias]: getDocData(value) };
      if (this.plan.postFilter && !evaluatePredicate(this.plan.postFilter, row, this.parameters)) {
        continue;
      }

      return row;
    }
  }

  private startScan(drivingValue: any) {
    const extraWhere: FirestoreWhereConstraint[] = [];

    if (this.driverField && this.driverOp) {
      extraWhere.push({
        fieldPath: this.driverField,
        op: this.driverOp,
        value: drivingValue,
      });
    }

    // Merge driver extraWhere with configured options
    const finalOpts: PreparedFirestoreCursorOptions = {
      ...this.cursorOpts,
      extraWhere: [
        ...this.cursorOpts.extraWhere ?? [],
        ...extraWhere,
      ],
    };

    const query = this.buildQuery(finalOpts);
    const stream = query.stream() as AsyncIterable<admin.firestore.QueryDocumentSnapshot>;
    this.iterator = stream[Symbol.asyncIterator]();
    this.exhausted = false;
  }

  // Helper to build query (extracted from previous createCursor logic)
  private buildQuery(opts: PreparedFirestoreCursorOptions): admin.firestore.Query {
    const {
      extraWhere = [],
      includeBaseWhere = true,
      orderBy,
      includeScanOrderBy = false,
      limit,
      offset,
      includeScanLimitOffset = false,
    } = opts;

    const scan = this.plan.scan;
    const baseWhere = compileConstraints(this.plan.baseConstraints, this.parameters);
    const combinedWhere = [
      ...includeBaseWhere ? baseWhere : [],
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
        ? scan.orderBy
          .filter(o => o.field.kind === 'Field')
          .map(o => ({ fieldPath: (o.field as Field).path.join('.'), direction: o.direction }))
        : undefined
    );

    const resolvedOffset = offset ?? (includeScanLimitOffset ? scan.offset : undefined);
    const resolvedLimit = limit ?? (includeScanLimitOffset ? scan.limit : undefined);

    return buildFirestoreQuery(this.db, {
      collectionPath: scan.collectionPath,
      collectionGroup: scan.collectionGroup,
      where,
      orderBy: resolvedOrderBy,
      offset: resolvedOffset,
      limit: resolvedLimit,
    });
  }
}

export function prepareFirestoreScanPlan(node: ExecutionNode): PreparedFirestoreScanPlan {
  if (node.type === NodeType.PREPARED_SCAN) {
    const p = node as PreparedScanNode;
    return {
      scan: p.scan,
      postFilter: p.postFilter,
      baseConstraints: p.baseConstraints,
      driver: p.driver,
    };
  }

  if (node.type === NodeType.SCAN) {
    const scan = node as ScanNode;
    return {
      scan,
      baseConstraints: scan.constraints,
    };
  }

  throw new Error('PreparedFirestoreScan currently supports PREPARED_SCAN or SCAN only.');
}

function compileConstraints(constraints: Constraint[], parameters: Record<string, any>): FirestoreWhereConstraint[] {
  const resolveValue = (v: Expression) => evaluate(v, {}, parameters);
  return constraints.map(c => ({
    fieldPath: c.field.path.join('.'),
    op: c.op,
    value: Array.isArray(c.value) ? c.value.map(resolveValue) : resolveValue(c.value),
  }));
}

function isMembershipConstraint(constraint: FirestoreWhereConstraint): boolean {
  return constraint.op === 'in' || constraint.op === 'not-in' || constraint.op === 'array-contains-any';
}
