import { SortNode } from '../ast';
import { evaluate } from '../evaluator';
import { sortBuffer, SortComparator } from '../utils/sort-utils';
import { Operator, SortOrder } from './operator';

export class Sort extends Operator {
  constructor(
    private source: Operator,
    private node: SortNode,
    private parameters: Record<string, any>
  ) { super(); }

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

  getSortOrder(): SortOrder | undefined {
    if (!this.node.orderBy.length) return undefined;
    const primary = this.node.orderBy[0];
    if (primary.field.kind !== 'Field') return undefined; // Only expose field-based sorts
    return { field: `${primary.field.source}.${primary.field.path.join('.')}`, direction: primary.direction };
  }

}
