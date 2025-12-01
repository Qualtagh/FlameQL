import { JoinNode } from '../ast';
import { Operator } from './operator';

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
  private rightIndex = 0;
  private currentLeftMatches: any[] = [];
  private currentRightMatches: any[] = [];
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

    // Check if we've exhausted the left collection
    if (this.leftIndex >= this.leftBuffer.length) {
      return false;
    }

    const leftValue = this.getValue(this.leftBuffer[this.leftIndex], this.leftField);

    // Collect all left rows with this same value (handle duplicates)
    let tempLeftIndex = this.leftIndex;
    while (
      tempLeftIndex < this.leftBuffer.length &&
      this.compareValues(this.getValue(this.leftBuffer[tempLeftIndex], this.leftField), leftValue) === 0
    ) {
      this.currentLeftMatches.push(this.leftBuffer[tempLeftIndex]);
      tempLeftIndex++;
    }

    // Use two-pointer technique to find matching right rows based on operation
    switch (this.operation) {
      case '==':
        // Advance right pointer until we find matching value or pass it
        while (this.rightIndex < this.rightBuffer.length) {
          const rightValue = this.getValue(this.rightBuffer[this.rightIndex], this.rightField);
          const cmp = this.compareValues(leftValue, rightValue);

          if (cmp < 0) {
            // Left value is smaller, no matches for this left value
            break;
          } else if (cmp > 0) {
            // Right value is smaller, advance right pointer
            this.rightIndex++;
          } else {
            // Values match! Collect all right rows with this value
            let tempRightIndex = this.rightIndex;
            while (tempRightIndex < this.rightBuffer.length) {
              const rightValue = this.getValue(this.rightBuffer[tempRightIndex], this.rightField);
              if (this.compareValues(rightValue, leftValue) !== 0) break;
              this.currentRightMatches.push(this.rightBuffer[tempRightIndex]);
              tempRightIndex++;
            }
            this.rightIndex = tempRightIndex;
            break;
          }
        }
        break;

      case '<':
        // Left < Right: find first right value > leftValue, collect all from there to end
        while (this.rightIndex < this.rightBuffer.length) {
          const rightValue = this.getValue(this.rightBuffer[this.rightIndex], this.rightField);
          if (this.compareValues(leftValue, rightValue) < 0) {
            // Found first right > left, collect all remaining
            for (let i = this.rightIndex; i < this.rightBuffer.length; i++) {
              this.currentRightMatches.push(this.rightBuffer[i]);
            }
            break;
          }
          this.rightIndex++;
        }
        break;

      case '<=':
        // Left <= Right: find first right value >= leftValue, collect all from there to end
        while (this.rightIndex < this.rightBuffer.length) {
          const rightValue = this.getValue(this.rightBuffer[this.rightIndex], this.rightField);
          if (this.compareValues(leftValue, rightValue) <= 0) {
            // Found first right >= left, collect all remaining
            for (let i = this.rightIndex; i < this.rightBuffer.length; i++) {
              this.currentRightMatches.push(this.rightBuffer[i]);
            }
            break;
          }
          this.rightIndex++;
        }
        break;

      case '>':
        // Left > Right: collect all right values < leftValue
        // Since we're scanning left in order, we collect from start up to first right >= leftValue
        let tempRightIndex = 0;
        while (tempRightIndex < this.rightBuffer.length) {
          const rightValue = this.getValue(this.rightBuffer[tempRightIndex], this.rightField);
          if (this.compareValues(leftValue, rightValue) > 0) {
            this.currentRightMatches.push(this.rightBuffer[tempRightIndex]);
            tempRightIndex++;
          } else {
            break;
          }
        }
        break;

      case '>=':
        // Left >= Right: collect all right values <= leftValue
        let tempRightIdx = 0;
        while (tempRightIdx < this.rightBuffer.length) {
          const rightValue = this.getValue(this.rightBuffer[tempRightIdx], this.rightField);
          if (this.compareValues(leftValue, rightValue) >= 0) {
            this.currentRightMatches.push(this.rightBuffer[tempRightIdx]);
            tempRightIdx++;
          } else {
            break;
          }
        }
        break;
    }

    // Advance left index past processed rows
    this.leftIndex = tempLeftIndex;

    // Return true if we found any matches
    return this.currentLeftMatches.length > 0 && this.currentRightMatches.length > 0;
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
