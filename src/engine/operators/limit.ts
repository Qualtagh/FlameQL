import { LimitNode } from '../ast';
import { Operator, SortOrder } from './operator';

export class Limit implements Operator {
  private delivered = 0;
  private skipped = 0;

  constructor(
    private source: Operator,
    private node: LimitNode
  ) { }

  async next(): Promise<any | null> {
    // skip offset rows
    while (this.skipped < (this.node.offset ?? 0)) {
      const skipRow = await this.source.next();
      if (!skipRow) {
        return null;
      }
      this.skipped++;
    }

    if (this.delivered >= this.node.limit) {
      return null;
    }

    const row = await this.source.next();
    if (!row) return null;

    this.delivered++;
    return row;
  }

  getSortOrder(): SortOrder | undefined {
    return this.source.getSortOrder();
  }
}
