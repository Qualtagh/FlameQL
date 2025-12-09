import { JoinNode } from '../ast';
import { evaluatePredicate } from '../evaluator';
import { Operator, SortOrder } from './operator';

/**
 * NestedLoopJoinOperator
 *
 * Loads the RIGHT collection into an in-memory buffer.
 * Streams the LEFT collection and iterates through the buffer for each row.
 *
 * Complexity: O(N * M)
 * Memory: O(M) - Right collection must fit in memory.
 * Requirement: None (supports any operation).
 */
export class NestedLoopJoinOperator implements Operator {
  private rightBuffer: any[] = [];
  private initialized = false;
  private currentLeftRow: any | null = null;
  private rightIndex = 0;
  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    private node: JoinNode
  ) { }

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

        const combinedRow = { ...this.currentLeftRow, ...rightRow };
        if (evaluatePredicate(this.node.condition, combinedRow)) {
          return combinedRow;
        }
      }

      this.currentLeftRow = null;
    }
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }
}
