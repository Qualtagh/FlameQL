import { ComparisonPredicate, Field } from '../../api/expression';
import { JoinNode } from '../ast';
import { getValueFromField } from '../evaluator';
import { isMergeJoinCompatible } from '../utils/operation-comparator';
import { compareValues } from '../utils/sort-utils';
import { Operator, SortOrder } from './operator';

/**
 * MergeJoinOperator
 *
 * Sorts both the LEFT and RIGHT collections by their join fields.
 * Uses a two-pointer algorithm to scan through both sorted collections in parallel.
 *
 * Complexity: O(N log N + M log M) for sorting, O(N + M) for merging with two pointers.
 * Memory: O(N + M) - Both collections must fit in memory.
 * Requirement: Comparison operations (==, <, <=, >, >=).
 */
export class MergeJoinOperator implements Operator {
  private leftField: Field;
  private rightField: Field;
  private operation: string;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    node: JoinNode
  ) {
    if (!isMergeJoinCompatible(node.condition)) {
      throw new Error(
        `MergeJoin strategy requires comparison operation (==, <, <=, >, >=), got: ${node.condition}`
      );
    }
    const condition = node.condition as ComparisonPredicate;
    this.operation = condition.operation;
    this.leftField = this.ensureField(condition.left);
    this.rightField = this.ensureField(condition.right);
  }

  async *[Symbol.asyncIterator]() {
    const leftBuffer: any[] = [];
    const rightBuffer: any[] = [];

    // 1. Build buffers
    for await (const row of this.leftSource) {
      leftBuffer.push(row);
    }
    for await (const row of this.rightSource) {
      rightBuffer.push(row);
    }

    // 2. Sort buffers if needed
    const leftSort = this.leftSource.getSortOrder();
    const expectedLeftField = `${this.leftField.source}.${this.leftField.path.join('.')}`;
    const leftSorted = leftSort && leftSort.field === expectedLeftField && leftSort.direction === 'asc';

    if (!leftSorted) {
      leftBuffer.sort((a, b) =>
        compareValues(
          getValueFromField(a, this.leftField),
          getValueFromField(b, this.leftField)
        )
      );
    }

    const rightSort = this.rightSource.getSortOrder();
    const expectedRightField = `${this.rightField.source}.${this.rightField.path.join('.')}`;
    const rightSorted = rightSort && rightSort.field === expectedRightField && rightSort.direction === 'asc';

    if (!rightSorted) {
      rightBuffer.sort((a, b) =>
        compareValues(
          getValueFromField(a, this.rightField),
          getValueFromField(b, this.rightField)
        )
      );
    }

    // 3. Merge Logic
    let leftIndex = 0;

    // Pointers for the sliding window on rightBuffer
    let idxGe = 0; // index of first element where rightValue >= leftValue
    let idxGt = 0; // index of first element where rightValue > leftValue

    while (leftIndex < leftBuffer.length) {
      const leftValue = getValueFromField(leftBuffer[leftIndex], this.leftField);

      // Collect all left rows with this same value (handle duplicates)
      const currentLeftMatches: any[] = [];
      while (
        leftIndex < leftBuffer.length &&
        compareValues(getValueFromField(leftBuffer[leftIndex], this.leftField), leftValue) === 0
      ) {
        currentLeftMatches.push(leftBuffer[leftIndex]);
        leftIndex++;
      }

      // Update idxGe: find first right element >= leftValue
      while (idxGe < rightBuffer.length) {
        const rightValue = getValueFromField(rightBuffer[idxGe], this.rightField);
        if (compareValues(rightValue, leftValue) >= 0) {
          break;
        }
        idxGe++;
      }

      // Update idxGt: find first right element > leftValue
      // Optimization: idxGt must be >= idxGe
      if (idxGt < idxGe) {
        idxGt = idxGe;
      }
      while (idxGt < rightBuffer.length) {
        const rightValue = getValueFromField(rightBuffer[idxGt], this.rightField);
        if (compareValues(rightValue, leftValue) > 0) {
          break;
        }
        idxGt++;
      }

      // Determine matching range based on operation
      let rightMatchStart = 0;
      let rightMatchEnd = 0;

      switch (this.operation) {
        case '==':
          // right == left  => [idxGe, idxGt)
          rightMatchStart = idxGe;
          rightMatchEnd = idxGt;
          break;
        case '<':
          // left < right <=> right > left => [idxGt, end)
          rightMatchStart = idxGt;
          rightMatchEnd = rightBuffer.length;
          break;
        case '<=':
          // left <= right <=> right >= left => [idxGe, end)
          rightMatchStart = idxGe;
          rightMatchEnd = rightBuffer.length;
          break;
        case '>':
          // left > right <=> right < left => [0, idxGe)
          rightMatchStart = 0;
          rightMatchEnd = idxGe;
          break;
        case '>=':
          // left >= right <=> right <= left => [0, idxGt)
          rightMatchStart = 0;
          rightMatchEnd = idxGt;
          break;
      }

      // Yield all combinations
      for (const leftRow of currentLeftMatches) {
        for (let r = rightMatchStart; r < rightMatchEnd; r++) {
          const rightRow = rightBuffer[r];
          yield { ...leftRow, ...rightRow };
        }
      }
    }
  }

  getSortOrder(): SortOrder | undefined {
    // MergeJoin produces output sorted by the join keys (ASC)
    // We can report it as sorted by the left field
    return { field: `${this.leftField.source}.${this.leftField.path.join('.')}`, direction: 'asc' };
  }

  private ensureField(expr: any): Field {
    if (expr && typeof expr === 'object' && expr.kind === 'Field' && expr.source) {
      return expr as Field;
    }
    throw new Error('Merge join requires Field operands.');
  }
}
