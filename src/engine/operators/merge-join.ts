import { OrderByDirection } from '@google-cloud/firestore';
import { JoinNode } from '../ast';
import { getValueFromPath } from '../evaluator';
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
  private leftBuffer: any[] = [];
  private rightBuffer: any[] = [];
  private initialized = false;
  private leftIndex = 0;

  // Pointers for the sliding window on rightBuffer
  // idxGe: index of first element where rightValue >= leftValue
  // idxGt: index of first element where rightValue > leftValue
  private idxGe = 0;
  private idxGt = 0;

  private currentLeftMatches: any[] = [];

  // Range of matching indices in rightBuffer [start, end)
  private rightMatchStart = 0;
  private rightMatchEnd = 0;

  private matchLeftIndex = 0;
  private matchRightIndex = 0;

  private leftField: string;
  private rightField: string;
  private operation: string;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    node: JoinNode
  ) {
    const supportedOps = ['==', '<', '<=', '>', '>='];
    if (!supportedOps.includes(node.condition.operation)) {
      throw new Error(
        `MergeJoin strategy requires comparison operation (==, <, <=, >, >=), got: ${node.condition.operation}`
      );
    }

    this.operation = node.condition.operation;
    this.leftField = node.condition.left;
    this.rightField = node.condition.right;
  }

  async next(): Promise<any | null> {
    if (!this.initialized) {
      await this.buildSortedBuffers();
      this.initialized = true;
    }

    while (true) {
      // If we're iterating through current matches, continue
      if (this.matchLeftIndex < this.currentLeftMatches.length) {
        if (this.matchRightIndex < this.rightMatchEnd) {
          const leftRow = this.currentLeftMatches[this.matchLeftIndex];
          const rightRow = this.rightBuffer[this.matchRightIndex++];
          return { ...leftRow, ...rightRow };
        }
        // Move to next left match and reset right index
        this.matchLeftIndex++;
        this.matchRightIndex = this.rightMatchStart;
        continue;
      }

      // Find next set of matches
      if (!this.findNextMatches()) {
        return null;
      }
    }
  }

  getSortOrder(): SortOrder | undefined {
    // MergeJoin produces output sorted by the join keys (ASC)
    // We can report it as sorted by the left field
    return { field: this.leftField, direction: 'asc' };
  }

  requestSort(field: string, direction: OrderByDirection): boolean {
    // We can only satisfy sort requests on the join key
    if (field === this.leftField && direction === 'asc') {
      return true;
    }
    return false;
  }

  private async buildSortedBuffers() {
    // Try to request sort from sources
    this.leftSource.requestSort(this.leftField, 'asc');
    this.rightSource.requestSort(this.rightField, 'asc');

    // Load left collection
    let row;
    while (row = await this.leftSource.next()) {
      this.leftBuffer.push(row);
    }

    // Load right collection
    while (row = await this.rightSource.next()) {
      this.rightBuffer.push(row);
    }

    // Check if left is already sorted
    const leftSort = this.leftSource.getSortOrder();
    const leftSorted = leftSort && leftSort.field === this.leftField && leftSort.direction === 'asc';

    if (!leftSorted) {
      this.leftBuffer.sort((a, b) =>
        this.compareValues(
          getValueFromPath(a, this.leftField),
          getValueFromPath(b, this.leftField)
        )
      );
    }

    // Check if right is already sorted
    const rightSort = this.rightSource.getSortOrder();
    const rightSorted = rightSort && rightSort.field === this.rightField && rightSort.direction === 'asc';

    if (!rightSorted) {
      this.rightBuffer.sort((a, b) =>
        this.compareValues(
          getValueFromPath(a, this.rightField),
          getValueFromPath(b, this.rightField)
        )
      );
    }
  }

  private findNextMatches(): boolean {
    this.currentLeftMatches = [];
    this.matchLeftIndex = 0;

    // Check if we've exhausted the left collection
    if (this.leftIndex >= this.leftBuffer.length) {
      return false;
    }

    const leftValue = getValueFromPath(this.leftBuffer[this.leftIndex], this.leftField);

    // Collect all left rows with this same value (handle duplicates)
    while (
      this.leftIndex < this.leftBuffer.length &&
      this.compareValues(getValueFromPath(this.leftBuffer[this.leftIndex], this.leftField), leftValue) === 0
    ) {
      this.currentLeftMatches.push(this.leftBuffer[this.leftIndex]);
      this.leftIndex++;
    }

    // Update idxGe: find first right element >= leftValue
    while (this.idxGe < this.rightBuffer.length) {
      const rightValue = getValueFromPath(this.rightBuffer[this.idxGe], this.rightField);
      if (this.compareValues(rightValue, leftValue) >= 0) {
        break;
      }
      this.idxGe++;
    }

    // Update idxGt: find first right element > leftValue
    // Optimization: idxGt must be >= idxGe
    if (this.idxGt < this.idxGe) {
      this.idxGt = this.idxGe;
    }
    while (this.idxGt < this.rightBuffer.length) {
      const rightValue = getValueFromPath(this.rightBuffer[this.idxGt], this.rightField);
      if (this.compareValues(rightValue, leftValue) > 0) {
        break;
      }
      this.idxGt++;
    }

    // Determine matching range based on operation
    switch (this.operation) {
      case '==':
        // right == left  => [idxGe, idxGt)
        this.rightMatchStart = this.idxGe;
        this.rightMatchEnd = this.idxGt;
        break;

      case '<':
        // left < right <=> right > left => [idxGt, end)
        this.rightMatchStart = this.idxGt;
        this.rightMatchEnd = this.rightBuffer.length;
        break;

      case '<=':
        // left <= right <=> right >= left => [idxGe, end)
        this.rightMatchStart = this.idxGe;
        this.rightMatchEnd = this.rightBuffer.length;
        break;

      case '>':
        // left > right <=> right < left => [0, idxGe)
        this.rightMatchStart = 0;
        this.rightMatchEnd = this.idxGe;
        break;

      case '>=':
        // left >= right <=> right <= left => [0, idxGt)
        this.rightMatchStart = 0;
        this.rightMatchEnd = this.idxGt;
        break;
    }

    this.matchRightIndex = this.rightMatchStart;

    // If no matches found in right buffer, try next left group immediately
    if (this.rightMatchStart >= this.rightMatchEnd) {
      return this.findNextMatches();
    }

    return true;
  }

  private compareValues(a: any, b: any): number {
    // Handle null/undefined
    if (a === null || a === undefined) {
      if (b === null || b === undefined) return 0;
      return -1;
    }
    if (b === null || b === undefined) {
      return 1;
    }

    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }
}
