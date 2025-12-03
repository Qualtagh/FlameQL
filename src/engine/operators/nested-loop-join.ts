import { OrderByDirection } from '@google-cloud/firestore';
import { JoinNode } from '../ast';
import { getValueFromPath } from '../evaluator';
import { createOperationComparator } from '../utils/operation-comparator';
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

        if (this.evaluatePredicate(this.node.condition, this.currentLeftRow, rightRow)) {
          return { ...this.currentLeftRow, ...rightRow };
        }
      }

      this.currentLeftRow = null;
    }
  }

  private evaluatePredicate(predicate: any, leftRow: any, rightRow: any): boolean {
    if (predicate.type === 'COMPARISON') {
      const leftValue = getValueFromPath(leftRow, predicate.left);
      const rightValue = getValueFromPath(rightRow, predicate.right);
      const comparator = createOperationComparator(predicate.operation);
      return comparator(leftValue, rightValue);
    } else if (predicate.type === 'AND') {
      return predicate.conditions.every((c: any) => this.evaluatePredicate(c, leftRow, rightRow));
    } else if (predicate.type === 'OR') {
      return predicate.conditions.some((c: any) => this.evaluatePredicate(c, leftRow, rightRow));
    } else if (predicate.type === 'NOT') {
      return !this.evaluatePredicate(predicate.operand, leftRow, rightRow);
    } else if (predicate.type === 'CONSTANT') {
      return predicate.value;
    }
    throw new Error(`Unknown predicate type: ${predicate.type}`);
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }

  requestSort(_field: string, _direction: OrderByDirection): boolean {
    return false;
  }
}
