import { LimitNode } from '../ast';
import { Operator } from './operator';

export class Limit implements Operator {
  constructor(
    private source: Operator,
    private node: LimitNode
  ) { }

  async *[Symbol.asyncIterator]() {
    let skipped = 0;
    let delivered = 0;
    const limit = this.node.limit;
    const offset = this.node.offset ?? 0;

    for await (const row of this.source) {
      if (skipped < offset) {
        skipped++;
        continue;
      }

      if (delivered >= limit) {
        return;
      }

      delivered++;
      yield row;
    }
  }
}
