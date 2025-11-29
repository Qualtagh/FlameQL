import { JoinNode } from '../ast';
import { Operator } from './operator';

/**
 * HashJoinOperator
 *
 * Builds an in-memory Hash Map (Index) of the RIGHT collection.
 * Streams the LEFT collection and looks up matches in the Hash Map.
 *
 * Complexity: O(N + M) where N is left size, M is right size.
 * Memory: O(M) - Right collection must fit in memory.
 * Requirement: Equality condition.
 */
export class HashJoinOperator implements Operator {
  private hashTable: Map<string, any[]> = new Map();
  private initialized = false;
  private currentLeftRow: any | null = null;
  private currentMatches: any[] | null = null;
  private matchIndex = 0;
  private leftKey: string;
  private rightKey: string;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    node: JoinNode
  ) {
    if (!this.isEqualityJoin(node.on)) {
      throw new Error('HashJoin strategy requires an equality condition');
    }
    this.leftKey = node.on.left;
    this.rightKey = node.on.right;
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

      const val = this.getValue(this.currentLeftRow, this.leftKey);

      if (val !== undefined && val !== null) {
        const key = String(val);
        if (this.hashTable.has(key)) {
          this.currentMatches = this.hashTable.get(key)!;
          this.matchIndex = 0;
        } else {
          this.currentMatches = null;
        }
      } else {
        this.currentMatches = null;
      }
    }
  }

  private async buildHashTable() {
    let row;
    while (row = await this.rightSource.next()) {
      const val = this.getValue(row, this.rightKey);
      if (val !== undefined && val !== null) {
        const key = String(val);
        if (!this.hashTable.has(key)) {
          this.hashTable.set(key, []);
        }
        this.hashTable.get(key)!.push(row);
      }
    }
  }

  private getValue(obj: any, path: string): any {
    return path.split('.').reduce((o, k) => (o || {})[k], obj);
  }

  private isEqualityJoin(on: any): on is { left: string, right: string } {
    return typeof on === 'object' && on !== null && 'left' in on && 'right' in on;
  }
}
