import { SortNode } from '../ast';
import { evaluate } from '../evaluator';
import { sortBuffer, SortComparator } from '../utils/sort-utils';
import { Operator, SortOrder } from './operator';

export class Sort implements Operator {
  private buffer: any[] | null = null;
  private index = 0;

  constructor(
    private source: Operator,
    private node: SortNode,
    private parameters: Record<string, any>
  ) { }

  async next(): Promise<any | null> {
    if (!this.buffer) {
      await this.loadAndSort();
    }

    if (this.index < this.buffer!.length) {
      return this.buffer![this.index++];
    }

    return null;
  }

  getSortOrder(): SortOrder | undefined {
    if (!this.node.orderBy.length) return undefined;
    const primary = this.node.orderBy[0];
    if (primary.field.kind !== 'Field') return undefined; // Only expose field-based sorts
    return { field: `${primary.field.source}.${primary.field.path.join('.')}`, direction: primary.direction };
  }

  private async loadAndSort() {
    this.buffer = [];
    let row;
    while (row = await this.source.next()) {
      this.buffer.push(row);
    }

    const comparators: SortComparator[] = this.node.orderBy.map(spec => ({
      getValue: (row: any) => evaluate(spec.field, row, this.parameters),
      direction: spec.direction,
    }));

    sortBuffer(this.buffer, comparators);
  }
}
