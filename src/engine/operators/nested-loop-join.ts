import { JoinNode } from '../ast';
import { Operator } from './operator';

/**
 * NestedLoopJoinOperator
 *
 * Loads the RIGHT collection into an in-memory buffer.
 * Streams the LEFT collection and iterates through the buffer for each row.
 *
 * Complexity: O(N * M)
 * Memory: O(M) - Right collection must fit in memory.
 * Requirement: None (supports any condition).
 */
export class NestedLoopJoinOperator implements Operator {
  private rightBuffer: any[] = [];
  private initialized = false;
  private currentLeftRow: any | null = null;
  private rightIndex = 0;
  private on: ((l: any, r: any) => boolean) | null;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    node: JoinNode
  ) {
    if (typeof node.on === 'function') {
      this.on = node.on;
    } else {
      this.on = null;
    }
  }

  async next(): Promise<any | null> {
    if (!this.initialized) {
      // Load right source into memory
      let row;
      while (row = await this.rightSource.next()) {
        this.rightBuffer.push(row);
      }
      this.initialized = true;
    }

    while (true) {
      if (!this.currentLeftRow) {
        this.currentLeftRow = await this.leftSource.next();
        if (!this.currentLeftRow) return null;
        this.rightIndex = 0;
      }

      while (this.rightIndex < this.rightBuffer.length) {
        const rightRow = this.rightBuffer[this.rightIndex++];

        let match = true;
        if (this.on) {
          match = this.on(this.currentLeftRow, rightRow);
        }

        if (match) {
          return { ...this.currentLeftRow, ...rightRow };
        }
      }

      this.currentLeftRow = null;
    }
  }
}
