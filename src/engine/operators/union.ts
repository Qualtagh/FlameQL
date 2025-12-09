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
   * Extracts DOC_PATH from row, handling both flat and aliased structures.
   */
  private extractDocPath(row: any): string | undefined {
    if (row[DOC_PATH]) {
      return row[DOC_PATH];
    }
    for (const key of Object.keys(row)) {
      const nested = row[key];
      if (nested && typeof nested === 'object' && nested[DOC_PATH]) {
        return nested[DOC_PATH];
      }
    }
    return undefined;
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }

  private isDuplicate(row: any): boolean {
    switch (this.strategy) {
      case UnionDistinctStrategy.DocPath: {
        const path = this.extractDocPath(row);
        if (!path) return false;
        if (this.seenPaths.has(path)) return true;
        this.seenPaths.add(path);
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
