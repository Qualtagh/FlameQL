import { OrderByDirection } from '@google-cloud/firestore';
import { DOC_PATH } from '../symbols';
import { Operator, SortOrder } from './operator';

/**
 * Union operator that combines results from multiple input operators.
 *
 * Supports two deduplication modes:
 * - `distinct`: SQL semantics - compares all field values using serialization
 * - `deduplicateByDocPath`: Uses DOC_PATH for identity (optimizer-safe only, when all inputs scan same fields)
 *
 * UNION ALL (no deduplication) is the default when both flags are false.
 */
export class Union implements Operator {
  private inputs: Operator[];
  private distinct: boolean;
  private deduplicateByDocPath: boolean;
  private currentInputIndex: number = 0;

  // For deduplicateByDocPath mode: track seen DOC_PATH values
  private seenPaths: Set<string> = new Set();

  // For distinct mode: track seen row serializations
  private seenRows: Set<string> = new Set();

  constructor(inputs: Operator[], distinct: boolean = false, deduplicateByDocPath: boolean = false) {
    this.inputs = inputs;
    this.distinct = distinct;
    this.deduplicateByDocPath = deduplicateByDocPath;
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

      // Check for duplicates based on mode
      if (this.deduplicateByDocPath) {
        const path = this.extractDocPath(row);
        if (path && this.seenPaths.has(path)) {
          continue; // Duplicate by DOC_PATH
        }
        if (path) {
          this.seenPaths.add(path);
        }
      } else if (this.distinct) {
        const key = this.serializeRow(row);
        if (this.seenRows.has(key)) {
          continue; // Duplicate by content
        }
        this.seenRows.add(key);
      }

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

  requestSort(_field: string, _direction: OrderByDirection): boolean {
    return false;
  }
}
