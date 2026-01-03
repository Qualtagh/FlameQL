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
  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    private node: JoinNode,
    private parameters: Record<string, any>
  ) { }

  async *[Symbol.asyncIterator]() {
    const rightBuffer: any[] = [];

    // Load right source into memory
    for await (const row of this.rightSource) {
      rightBuffer.push(row);
    }

    // Stream left source and join with buffer
    for await (const leftRow of this.leftSource) {
      for (const rightRow of rightBuffer) {
        const combinedRow = { ...leftRow, ...rightRow };
        if (evaluatePredicate(this.node.condition, combinedRow, this.parameters)) {
          yield combinedRow;
        }
      }
    }
  }

  getSortOrder(): SortOrder | undefined {
    // Nested-loop join preserves the order of the LEFT input stream.
    return this.leftSource.getSortOrder();
  }
}
