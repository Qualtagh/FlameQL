import admin from 'firebase-admin';
import { ExecutionNode, JoinNode } from '../ast';
import { evaluate, evaluatePredicate, getValueFromField } from '../evaluator';
import { IndexManager } from '../indexes/index-manager';
import { DOC_PATH } from '../symbols';
import { IndexedNestedLoopLookupPlan, pickIndexedNestedLoopLookupPlan } from '../utils/indexed-nested-loop-utils';
import { Operator, SortOrder } from './operator';
import { PreparedFirestoreScan } from './prepared-firestore-scan';

/**
 * IndexedNestedLoopJoinOperator
 *
 * Uses Firestore index lookups on the RIGHT side, driven by values from the LEFT side.
 *
 * This implementation delegates all Firestore query construction + doc→row mapping
 * to a prepared scan (`PreparedFirestoreScan`) created from the RIGHT plan node.
 */
export class IndexedNestedLoopJoinOperator implements Operator {
  /**
   * Firestore allows at most 10 elements for `in` / `array-contains-any` queries.
   */
  private static readonly FIRESTORE_IN_MAX = 10;

  private readonly rightPrepared: PreparedFirestoreScan;
  private readonly driver: IndexedNestedLoopLookupPlan;

  private initialized = false;

  // Per-row lookup cache (used only when driver.mode === 'perRow')
  private perRowCache: Map<string, any[]> = new Map();

  private pendingLeftRow: any | null = null;
  private leftBatch: any[] = [];
  private leftBatchIndex = 0;

  private requestedKeySet: Set<string> = new Set();
  private rightMatchesByKey: Map<string, any[]> = new Map();

  private currentLeftRow: any | null = null;
  private currentRightMatches: any[] = [];
  private currentMatchIdx = 0;

  constructor(
    private db: admin.firestore.Firestore,
    private leftSource: Operator,
    rightPlan: ExecutionNode,
    private joinNode: JoinNode,
    private indexManager?: IndexManager
  ) {
    this.rightPrepared = new PreparedFirestoreScan(db, rightPlan);
    const driver = pickIndexedNestedLoopLookupPlan(
      joinNode.condition,
      this.rightPrepared.plan.scan,
      this.indexManager,
      { requireIndex: false }
    );
    if (!driver) {
      throw new Error('Indexed nested-loop join requires a conjunctive join predicate with at least one Field-vs-Field comparison.');
    }
    this.driver = driver;
  }

  async next(): Promise<any | null> {
    return this.driver.mode === 'batch'
      ? this.nextBatchMode()
      : this.nextPerRowMode();
  }

  getSortOrder(): SortOrder | undefined {
    // Preserves the order of the LEFT input stream.
    return this.leftSource.getSortOrder();
  }

  private async nextBatchMode(): Promise<any | null> {
    if (!this.initialized) {
      await this.loadNextBatch();
      this.initialized = true;
    }

    while (true) {
      // Emit remaining matches for current left row.
      if (this.currentLeftRow && this.currentMatchIdx < this.currentRightMatches.length) {
        const rightRow = this.currentRightMatches[this.currentMatchIdx++];
        const combinedRow = { ...this.currentLeftRow, ...rightRow };
        if (evaluatePredicate(this.joinNode.condition, combinedRow)) {
          return combinedRow;
        }
        continue;
      }

      // Move to next left row in the batch.
      this.currentLeftRow = null;
      this.currentRightMatches = [];
      this.currentMatchIdx = 0;

      while (this.leftBatchIndex < this.leftBatch.length) {
        const leftRow = this.leftBatch[this.leftBatchIndex++];
        const key = this.serializeKey(evaluate(this.driver.leftExpr, leftRow));
        const matches = key ? this.rightMatchesByKey.get(key) ?? [] : [];
        if (matches.length === 0) continue;

        this.currentLeftRow = leftRow;
        this.currentRightMatches = matches;
        this.currentMatchIdx = 0;
        break;
      }

      if (this.currentLeftRow) continue;

      // Batch exhausted. Load next batch or end.
      const hasMore = await this.loadNextBatch();
      if (!hasMore) return null;
    }
  }

  private async nextPerRowMode(): Promise<any | null> {
    while (true) {
      if (this.currentLeftRow && this.currentMatchIdx < this.currentRightMatches.length) {
        const rightRow = this.currentRightMatches[this.currentMatchIdx++];
        const combinedRow = { ...this.currentLeftRow, ...rightRow };
        if (evaluatePredicate(this.joinNode.condition, combinedRow)) {
          return combinedRow;
        }
        continue;
      }

      // Advance to next left row.
      const leftRow = await this.leftSource.next();
      if (!leftRow) return null;

      const leftVal = evaluate(this.driver.leftExpr, leftRow);
      const cacheKey = this.serializeKey(leftVal) ?? `null:${String(leftVal)}`;

      let rightRows = this.perRowCache.get(cacheKey);
      if (!rightRows) {
        rightRows = await this.fetchRightForPerRow(leftVal);
        // Simple cache guardrail.
        if (this.perRowCache.size > 1000) this.perRowCache.clear();
        this.perRowCache.set(cacheKey, rightRows);
      }

      this.currentLeftRow = leftRow;
      this.currentRightMatches = rightRows;
      this.currentMatchIdx = 0;
    }
  }

  private async loadNextBatch(): Promise<boolean> {
    this.leftBatch = [];
    this.leftBatchIndex = 0;
    this.requestedKeySet = new Set();
    this.rightMatchesByKey = new Map();

    const max = IndexedNestedLoopJoinOperator.FIRESTORE_IN_MAX;
    const values: any[] = [];

    const pushLeftRow = (row: any, joinValue: any) => {
      this.leftBatch.push(row);
      const key = this.serializeKey(joinValue);
      if (!key) return;
      if (!this.requestedKeySet.has(key)) {
        this.requestedKeySet.add(key);
        values.push(joinValue);
      }
    };

    // Start with a stashed row (if we had to stop early last time).
    if (this.pendingLeftRow) {
      const v = evaluate(this.driver.leftExpr, this.pendingLeftRow);
      pushLeftRow(this.pendingLeftRow, v);
      this.pendingLeftRow = null;
    }

    // Fill batch, respecting max unique lookup values.
    while (true) {
      const row = await this.leftSource.next();
      if (!row) break;

      const v = evaluate(this.driver.leftExpr, row);
      const key = this.serializeKey(v);

      // Null/undefined values can't be used reliably for index lookups; still keep the row (it will just produce no matches).
      if (!key) {
        this.leftBatch.push(row);
        continue;
      }

      // If new unique key would exceed batch limit, stash the row for the next batch.
      if (!this.requestedKeySet.has(key) && this.requestedKeySet.size >= max) {
        this.pendingLeftRow = row;
        break;
      }

      pushLeftRow(row, v);
    }

    if (this.leftBatch.length === 0) return false;
    if (values.length === 0) return true;

    // Fetch right rows for this batch via the prepared scan.
    const rightRows = await this.rightPrepared.fetch({
      includeBaseWhere: true,
      includeScanOrderBy: false,
      includeScanLimitOffset: false,
      extraWhere: [{
        fieldPath: this.driver.rightField.path.join('.'),
        op: this.driver.lookupOp,
        value: values,
      }],
    });

    // Index right rows by the join key(s) for fast matching.
    for (const rightRow of rightRows) {
      this.indexRightRow(rightRow);
    }

    return true;
  }

  private indexRightRow(rightRow: any) {
    if (this.driver.lookupOp === 'in') {
      const v = getValueFromField(rightRow, this.driver.rightField);
      const key = this.serializeKey(v);
      if (!key) return;
      if (!this.requestedKeySet.has(key)) return;
      const arr = this.rightMatchesByKey.get(key) ?? [];
      arr.push(rightRow);
      this.rightMatchesByKey.set(key, arr);
      return;
    }

    // array-contains-any lookup: index by each matched element
    const arrVal = getValueFromField(rightRow, this.driver.rightField);
    if (!Array.isArray(arrVal)) return;
    for (const element of arrVal) {
      const key = this.serializeKey(element);
      if (!key) continue;
      if (!this.requestedKeySet.has(key)) continue;
      const bucket = this.rightMatchesByKey.get(key) ?? [];
      bucket.push(rightRow);
      this.rightMatchesByKey.set(key, bucket);
    }
  }

  private async fetchRightForPerRow(leftVal: any): Promise<any[]> {
    const fieldPath = this.driver.rightField.path.join('.');
    const op = this.driver.lookupOp;

    // Ops that require an ARRAY constant.
    if (op === 'in' || op === 'array-contains-any' || op === 'not-in') {
      if (!Array.isArray(leftVal)) return [];
      const unique = uniqueNonNull(leftVal);
      if (unique.length === 0) return [];

      const alias = this.rightPrepared.plan.scan.alias;
      const seen = new Set<string>();
      const out: any[] = [];

      for (const chunk of chunkArray(unique, IndexedNestedLoopJoinOperator.FIRESTORE_IN_MAX)) {
        const rows = await this.rightPrepared.fetch({
          includeBaseWhere: true,
          includeScanOrderBy: false,
          includeScanLimitOffset: false,
          extraWhere: [{ fieldPath, op, value: chunk }],
        });

        for (const row of rows) {
          const path = row?.[alias]?.[DOC_PATH] as string | undefined;
          if (!path) {
            out.push(row);
            continue;
          }
          if (seen.has(path)) continue;
          seen.add(path);
          out.push(row);
        }
      }

      return out;
    }

    // Scalar ops.
    if (leftVal === undefined || leftVal === null) return [];

    return this.rightPrepared.fetch({
      includeBaseWhere: true,
      includeScanOrderBy: false,
      includeScanLimitOffset: false,
      extraWhere: [{ fieldPath, op, value: leftVal }],
    });
  }

  private serializeKey(value: any): string | null {
    if (value === undefined || value === null) return null;
    const t = typeof value;
    switch (t) {
      case 'string':
        return `s:${value}`;
      case 'number':
        return `n:${value}`;
      case 'boolean':
        return `b:${value}`;
      default:
        try {
          return `j:${JSON.stringify(value)}`;
        } catch {
          return null;
        }
    }
  }
}

function chunkArray<T>(values: T[], size: number): T[][] {
  if (size <= 0) return [values];
  const out: T[][] = [];
  for (let i = 0; i < values.length; i += size) {
    out.push(values.slice(i, i + size));
  }
  return out;
}

function uniqueNonNull(values: any[]): any[] {
  const seen = new Set<string>();
  const out: any[] = [];
  for (const v of values) {
    if (v === undefined || v === null) continue;
    const key = typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean'
      ? `${typeof v}:${String(v)}`
      : `j:${safeJson(v)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }
  return out;
}

function safeJson(v: any): string {
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}
