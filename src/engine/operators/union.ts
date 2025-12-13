import { UnionDistinctStrategy } from '../ast';
import { DOC_PATH } from '../symbols';
import { Operator, SortOrder } from './operator';

/**
 * Union operator that combines results from multiple input operators.
 *
 * Supports deduplication strategies defined by UnionDistinctStrategy.
 */
export class Union implements Operator {
  private inputs: Operator[];
  private strategy: UnionDistinctStrategy;
  private currentInputIndex: number = 0;

  private seenPaths: Set<string> = new Set();
  private seenRows: Set<string> = new Set();

  constructor(inputs: Operator[], strategy: UnionDistinctStrategy = UnionDistinctStrategy.None) {
    this.inputs = inputs;
    this.strategy = strategy;
  }

  async next(): Promise<any | null> {
    while (this.currentInputIndex < this.inputs.length) {
      const currentInput = this.inputs[this.currentInputIndex];
      const row = await currentInput.next();

      if (row === null) {
        // Current input exhausted, move to next
        this.currentInputIndex++;
        continue;
      }

      if (this.isDuplicate(row)) continue;

      return row;
    }

    // All inputs exhausted
    return null;
  }

  /**
   * Serializes row to a stable string for use as Set key.
   * Sorts object keys for deterministic output.
   */
  private serializeRow(row: any): string {
    return JSON.stringify(this.sortObjectKeys(row), this.jsonReplacer);
  }

  private jsonReplacer(_key: string, value: any): any {
    if (value instanceof Date) {
      return { __type: 'Date', value: value.toISOString() };
    }
    if (typeof value === 'bigint') {
      return { __type: 'BigInt', value: value.toString() };
    }
    return value;
  }

  private sortObjectKeys(obj: any): any {
    if (obj === null || typeof obj !== 'object') {
      return obj;
    }
    if (Array.isArray(obj)) {
      return obj.map(item => this.sortObjectKeys(item));
    }
    const sorted: Record<string, any> = {};
    for (const key of Object.keys(obj).sort()) {
      sorted[key] = this.sortObjectKeys(obj[key]);
    }
    return sorted;
  }

  /**
   * Extracts a stable doc-path-based deduplication key from a row.
   *
   * For scan results, the row is typically `{ alias: docData }`.
   * For join results, the row is typically `{ a: docA, b: docB, ... }`.
   *
   * We dedupe by the **full tuple** of doc paths present in the row so that
   * join rows that share the same left-side document but differ on the right
   * are not incorrectly treated as duplicates.
   */
  private extractDocPathKey(row: any): string | undefined {
    if (!row || typeof row !== 'object') return undefined;

    const pairs: Array<[string, string]> = [];

    // Flat doc row (defensive; current engine typically uses aliased rows)
    const top = row[DOC_PATH];
    if (typeof top === 'string') {
      pairs.push(['__root__', top]);
    }

    // Aliased structure: `{ alias: docData }`
    for (const key of Object.keys(row).sort()) {
      const nested = row[key];
      const path = nested && typeof nested === 'object' ? nested[DOC_PATH] : undefined;
      if (typeof path === 'string') {
        pairs.push([key, path]);
      }
    }

    if (pairs.length === 0) return undefined;
    return pairs.map(([k, p]) => `${k}:${p}`).join('|');
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }

  private isDuplicate(row: any): boolean {
    switch (this.strategy) {
      case UnionDistinctStrategy.DocPath: {
        const key = this.extractDocPathKey(row);
        if (!key) return false;
        if (this.seenPaths.has(key)) return true;
        this.seenPaths.add(key);
        return false;
      }
      case UnionDistinctStrategy.HashMap: {
        const key = this.serializeRow(row);
        if (this.seenRows.has(key)) return true;
        this.seenRows.add(key);
        return false;
      }
      case UnionDistinctStrategy.None:
      default:
        return false;
    }
  }
}
