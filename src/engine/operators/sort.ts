import { SortNode } from '../ast';
import { evaluate } from '../evaluator';
import { sortBuffer, SortComparator } from '../utils/sort-utils';
import { Operator } from './operator';

export class Sort implements Operator {
  constructor(
    private source: Operator,
    private node: SortNode,
    private parameters: Record<string, any>
  ) { }

  async *[Symbol.asyncIterator]() {
    const buffer: any[] = [];

    // Load all rows
    for await (const row of this.source) {
      buffer.push(row);
    }

    // Sort
    const comparators: SortComparator[] = this.node.orderBy.map(spec => ({
      getValue: (row: any) => evaluate(spec.field, row, this.parameters),
      direction: spec.direction,
    }));

    sortBuffer(buffer, comparators);

    // Yield
    for (const row of buffer) {
      yield row;
    }
  }
}
