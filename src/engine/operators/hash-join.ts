import { JoinNode } from '../ast';
import { isHashJoinCompatible } from '../utils/operation-comparator';
import { Operator } from './operator';

/**
 * HashJoinOperator
 *
 * Builds an in-memory Hash Map (Index) of the RIGHT collection.
 * Streams the LEFT collection and looks up matches in the Hash Map.
 *
 * Complexity: O(N + M) where N is left size, M is right size.
 * Memory: O(M) - Right collection must fit in memory.
 * Requirement: Hash-compatible operations (==, in, array-contains, array-contains-any).
 */
export class HashJoinOperator implements Operator {
  private hashTable: Map<string, any[]> = new Map();
  private initialized = false;
  private currentLeftRow: any | null = null;
  private currentMatches: any[] | null = null;
  private matchIndex = 0;
  private leftField: string;
  private rightField: string;
  private operation: string;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    node: JoinNode
  ) {
    if (!isHashJoinCompatible(node.condition.operation)) {
      throw new Error(
        `HashJoin strategy requires hash-compatible operation (==, in, array-contains, array-contains-any), got: ${node.condition.operation}`
      );
    }

    this.operation = node.condition.operation;
    this.leftField = node.condition.left;
    this.rightField = node.condition.right;
  }

  async next(): Promise<any | null> {
    if (!this.initialized) {
      await this.buildHashTable();
      this.initialized = true;
    }

    while (true) {
      if (this.currentMatches && this.matchIndex < this.currentMatches.length) {
        const rightRow = this.currentMatches[this.matchIndex++];
        return { ...this.currentLeftRow, ...rightRow };
      }

      this.currentLeftRow = await this.leftSource.next();
      if (!this.currentLeftRow) return null;

      const leftValue = this.getValue(this.currentLeftRow, this.leftField);
      this.currentMatches = this.findMatches(leftValue);
      this.matchIndex = 0;
    }
  }

  private async buildHashTable() {
    let row;
    while (row = await this.rightSource.next()) {
      const val = this.getValue(row, this.rightField);

      if (val !== undefined && val !== null) {
        // For array-contains-any, the right value is an array, and we index each element
        if (this.operation === 'array-contains-any' && Array.isArray(val)) {
          for (const element of val) {
            const key = String(element);
            if (!this.hashTable.has(key)) {
              this.hashTable.set(key, []);
            }
            this.hashTable.get(key)!.push(row);
          }
        } else {
          // For ==, array-contains, and in (if supported), index by the value itself
          // Note: For 'array-contains', right side is a scalar value that we look up in left array
          const key = String(val);
          if (!this.hashTable.has(key)) {
            this.hashTable.set(key, []);
          }
          this.hashTable.get(key)!.push(row);
        }
      }
    }
  }

  private findMatches(leftValue: any): any[] | null {
    if (leftValue === undefined || leftValue === null) {
      return null;
    }

    switch (this.operation) {
      case '==':
        const key = String(leftValue);
        return this.hashTable.get(key) || null;

      case 'in':
        // For 'in', the right value should be an array
        // We need to check if leftValue is in any of the arrays in the hash table
        // This is actually tricky with hash joins - we'd need to build the hash differently
        // For now, let's keep it simple and assume the semantics are:
        // leftValue IN rightArray, so we look up each element of rightValue
        // Actually, this might not work well with hash join. Let's document this limitation.
        throw new Error('Hash join with "in" operation needs special handling - not yet implemented');

      case 'array-contains':
        //  leftArray contains rightValue
        // Hash table indexed by right values, look up leftValue
        if (Array.isArray(leftValue)) {
          const matches = new Set<any>();
          for (const element of leftValue) {
            const elementKey = String(element);
            const elementMatches = this.hashTable.get(elementKey);
            if (elementMatches) {
              elementMatches.forEach(m => matches.add(m));
            }
          }
          return matches.size > 0 ? Array.from(matches) : null;
        }
        return null;

      case 'array-contains-any':
        // Similar to array-contains but with different semantics
        if (Array.isArray(leftValue)) {
          const matches = new Set<any>();
          for (const element of leftValue) {
            const elementKey = String(element);
            const elementMatches = this.hashTable.get(elementKey);
            if (elementMatches) {
              elementMatches.forEach(m => matches.add(m));
            }
          }
          return matches.size > 0 ? Array.from(matches) : null;
        }
        return null;

      default:
        return null;
    }
  }

  private getValue(obj: any, path: string): any {
    return path.split('.').reduce((o, k) => (o || {})[k], obj);
  }
}
