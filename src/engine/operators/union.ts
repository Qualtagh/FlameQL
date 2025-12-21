import { UnionDistinctStrategy } from '../ast';
import { createUnionDeduplicator } from '../utils/union-utils';
import { Operator, SortOrder } from './operator';

/**
 * Union operator that combines results from multiple input operators.
 *
 * Supports deduplication strategies defined by UnionDistinctStrategy.
 */
export class Union implements Operator {
  private inputs: Operator[];
  private currentInputIndex: number = 0;

  private seenPaths: Set<string> = new Set();
  private seenRows: Set<string> = new Set();
  private isDuplicate: (row: any) => boolean;

  constructor(inputs: Operator[], strategy: UnionDistinctStrategy = UnionDistinctStrategy.None) {
    this.inputs = inputs;
    this.isDuplicate = createUnionDeduplicator(strategy, {
      seenPaths: this.seenPaths,
      seenRows: this.seenRows,
    });
  }

  async next(): Promise<any | null> {
    while (this.currentInputIndex < this.inputs.length) {
      const currentInput = this.inputs[this.currentInputIndex];
      const row = await currentInput.next();

      if (row === null) {
        // Current input exhausted, move to next
        this.currentInputIndex++;
        continue;
      }

      if (this.isDuplicate(row)) continue;

      return row;
    }

    // All inputs exhausted
    return null;
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }
}
