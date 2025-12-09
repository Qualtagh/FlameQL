import { SortNode } from '../ast';
import { getValueFromField } from '../evaluator';
import { Operator, SortOrder } from './operator';

export class Sort implements Operator {
  private buffer: any[] | null = null;
  private index = 0;

  constructor(
    private source: Operator,
    private node: SortNode
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
    return { field: `${primary.field.source}.${primary.field.path.join('.')}`, direction: primary.direction };
  }

  private async loadAndSort() {
    this.buffer = [];
    let row;
    while (row = await this.source.next()) {
      this.buffer.push(row);
    }

    const comparators = this.node.orderBy.map(spec => ({
      ref: spec.field,
      dir: spec.direction === 'desc' ? -1 : 1,
    }));

    this.buffer.sort((a, b) => {
      for (const cmp of comparators) {
        const left = getValueFromField(a, cmp.ref);
        const right = getValueFromField(b, cmp.ref);
        if (left === right) continue;
        if (left === undefined || left === null) return -1 * cmp.dir;
        if (right === undefined || right === null) return 1 * cmp.dir;
        if (left < right) return -1 * cmp.dir;
        if (left > right) return 1 * cmp.dir;
      }
      return 0;
    });
  }
}
