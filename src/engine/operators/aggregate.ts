import { AggregateNode } from '../ast';
import { getValueFromField } from '../evaluator';
import { Operator, SortOrder } from './operator';

export class Aggregate extends Operator {
  constructor(
    private source: Operator,
    private node: AggregateNode
  ) { super(); }

  async *[Symbol.asyncIterator]() {
    const groups = new Map<string, any>();

    // Accumulate
    for await (const row of this.source) {
      const key = this.getGroupKey(row);
      if (!groups.has(key)) {
        groups.set(key, { key, count: 0, ...row }); // TODO: Initialize aggregates correctly
      }
      const group = groups.get(key);
      group.count++; // TODO: Implement actual aggregation logic
    }

    // Yield results
    for (const group of groups.values()) {
      yield group;
    }
  }

  private getGroupKey(row: any): string {
    return this.node.groupBy
      .map(field => {
        const value = getValueFromField(row, field);
        return value === undefined || value === null ? '' : String(value);
      })
      .join('_');
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }
}
