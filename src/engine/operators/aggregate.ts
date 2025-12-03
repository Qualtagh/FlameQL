import { OrderByDirection } from '@google-cloud/firestore';
import { AggregateNode } from '../ast';
import { Operator, SortOrder } from './operator';

export class Aggregate implements Operator {
  private groups: Map<string, any> = new Map();
  private initialized = false;
  private resultIterator: IterableIterator<any> | null = null;

  constructor(
    private source: Operator,
    private node: AggregateNode
  ) { }

  async next(): Promise<any | null> {
    if (!this.initialized) {
      let row;
      while (row = await this.source.next()) {
        const key = this.getGroupKey(row);
        if (!this.groups.has(key)) {
          this.groups.set(key, { key, count: 0, ...row }); // TODO: Initialize aggregates correctly
        }
        const group = this.groups.get(key);
        group.count++; // TODO: Implement actual aggregation logic
      }
      this.initialized = true;
      this.resultIterator = this.groups.values();
    }

    const next = this.resultIterator!.next();
    return next.done ? null : next.value;
  }

  private getGroupKey(row: any): string {
    return this.node.groupBy.map(field => {
      // Simple field access for now
      // TODO: Use evaluator
      return row[field.source!]?.[field.path[0]] || '';
    }).join('_');
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }

  requestSort(_field: string, _direction: OrderByDirection): boolean {
    return false;
  }
}
