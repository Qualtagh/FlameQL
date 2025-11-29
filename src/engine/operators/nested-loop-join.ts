import { JoinNode } from '../ast';
import { Operator } from './operator';

export class NestedLoopJoin implements Operator {
  private leftBuffer: any[] = [];
  private rightBuffer: any[] = [];
  private leftIndex = 0;
  private rightIndex = 0;
  private initialized = false;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    private node: JoinNode
  ) { }

  async next(): Promise<any | null> {
    if (!this.initialized) {
      // Naive implementation: Load everything into memory
      // TODO: Implement streaming / chunking
      let row;
      while ((row = await this.leftSource.next())) {
        this.leftBuffer.push(row);
      }
      while ((row = await this.rightSource.next())) {
        this.rightBuffer.push(row);
      }
      this.initialized = true;
    }

    while (this.leftIndex < this.leftBuffer.length) {
      const leftRow = this.leftBuffer[this.leftIndex];

      while (this.rightIndex < this.rightBuffer.length) {
        const rightRow = this.rightBuffer[this.rightIndex];
        this.rightIndex++;

        // Merge rows
        const merged = { ...leftRow, ...rightRow };

        // Check join condition
        // For now, support function-based condition (for testing) or assume true if null
        let match = true;
        if (this.node.on && typeof this.node.on === 'function') {
          match = this.node.on(leftRow, rightRow);
        }

        if (match) {
          return merged;
        }
      }

      this.rightIndex = 0;
      this.leftIndex++;
    }

    return null;
  }
}
