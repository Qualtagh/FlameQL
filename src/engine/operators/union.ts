import { UnionDistinctStrategy } from '../ast';
import { createUnionDeduplicator } from '../utils/union-utils';
import { Operator, SortOrder } from './operator';

/**
 * Union operator that combines results from multiple input operators.
 *
 * Supports deduplication strategies defined by UnionDistinctStrategy.
 */
export class Union extends Operator {
  private inputs: Operator[];
  private isDuplicate: (row: any) => boolean;

  constructor(inputs: Operator[], strategy: UnionDistinctStrategy = UnionDistinctStrategy.None) {
    super();
    this.inputs = inputs;
    this.isDuplicate = createUnionDeduplicator(strategy, {
      seenPaths: new Set(),
      seenRows: new Set(),
    });
  }

  async *[Symbol.asyncIterator]() {
    for (const input of this.inputs) {
      for await (const row of input) {
        if (!this.isDuplicate(row)) {
          yield row;
        }
      }
    }
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }
}
