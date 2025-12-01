import { JoinNode } from '../ast';
import { Operator } from './operator';

/**
 * MergeJoinOperator
 *
 * Sorts both the LEFT and RIGHT collections by their join fields.
 * Uses a two-pointer algorithm to scan through both sorted collections in parallel.
 *
 * Complexity: O(N log N + M log M) for sorting, O(N + M) for merging.
 * Memory: O(N + M) - Both collections must fit in memory.
 * Requirement: Equality operation (==) only.
 */
export class MergeJoinOperator implements Operator {
  private leftBuffer: any[] = [];
  private rightBuffer: any[] = [];
  private initialized = false;
  private leftIndex = 0;
  private rightIndex = 0;
  private currentLeftMatches: any[] = [];
  private currentRightMatches: any[] = [];
  private matchLeftIndex = 0;
  private matchRightIndex = 0;
  private leftField: string;
  private rightField: string;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    node: JoinNode
  ) {
    if (node.condition.operation !== '==') {
      throw new Error(
        `MergeJoin strategy requires equality operation (==), got: ${node.condition.operation}`
      );
    }

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
        if (this.matchRightIndex < this.currentRightMatches.length) {
          const leftRow = this.currentLeftMatches[this.matchLeftIndex];
          const rightRow = this.currentRightMatches[this.matchRightIndex++];
          return { ...leftRow, ...rightRow };
        }
        // Move to next left match and reset right index
        this.matchLeftIndex++;
        this.matchRightIndex = 0;
        continue;
      }

      // Find next set of matches
      if (!this.findNextMatches()) {
        return null;
      }
    }
  }

  private async buildSortedBuffers() {
    // Load left collection
    let row;
    while (row = await this.leftSource.next()) {
      this.leftBuffer.push(row);
    }

    // Load right collection
    while (row = await this.rightSource.next()) {
      this.rightBuffer.push(row);
    }

    // Sort both buffers by their join fields
    this.leftBuffer.sort((a, b) =>
      this.compareValues(
        this.getValue(a, this.leftField),
        this.getValue(b, this.leftField)
      )
    );

    this.rightBuffer.sort((a, b) =>
      this.compareValues(
        this.getValue(a, this.rightField),
        this.getValue(b, this.rightField)
      )
    );
  }

  private findNextMatches(): boolean {
    this.currentLeftMatches = [];
    this.currentRightMatches = [];
    this.matchLeftIndex = 0;
    this.matchRightIndex = 0;

    // Scan until we find matching values
    while (this.leftIndex < this.leftBuffer.length && this.rightIndex < this.rightBuffer.length) {
      const leftValue = this.getValue(this.leftBuffer[this.leftIndex], this.leftField);
      const rightValue = this.getValue(this.rightBuffer[this.rightIndex], this.rightField);

      const cmp = this.compareValues(leftValue, rightValue);

      if (cmp < 0) {
        // Left value is smaller, advance left pointer
        this.leftIndex++;
      } else if (cmp > 0) {
        // Right value is smaller, advance right pointer
        this.rightIndex++;
      } else {
        // Values match! Collect all rows with this value from both sides
        const matchValue = leftValue;

        // Collect all left rows with this value
        let tempLeftIndex = this.leftIndex;
        while (
          tempLeftIndex < this.leftBuffer.length &&
          this.compareValues(this.getValue(this.leftBuffer[tempLeftIndex], this.leftField), matchValue) === 0
        ) {
          this.currentLeftMatches.push(this.leftBuffer[tempLeftIndex]);
          tempLeftIndex++;
        }

        // Collect all right rows with this value
        let tempRightIndex = this.rightIndex;
        while (
          tempRightIndex < this.rightBuffer.length &&
          this.compareValues(this.getValue(this.rightBuffer[tempRightIndex], this.rightField), matchValue) === 0
        ) {
          this.currentRightMatches.push(this.rightBuffer[tempRightIndex]);
          tempRightIndex++;
        }

        // Advance indices past the matched groups
        this.leftIndex = tempLeftIndex;
        this.rightIndex = tempRightIndex;

        // Return true if we found matches
        if (this.currentLeftMatches.length > 0 && this.currentRightMatches.length > 0) {
          return true;
        }
      }
    }

    return false;
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

  private getValue(obj: any, path: string): any {
    return path.split('.').reduce((o, k) => (o || {})[k], obj);
  }
}
