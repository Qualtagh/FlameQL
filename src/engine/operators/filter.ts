import { FilterNode } from '../ast';
import { evaluatePredicate } from '../evaluator';
import { Operator, SortOrder } from './operator';

export class Filter extends Operator {
  constructor(
    private source: Operator,
    private node: FilterNode,
    private parameters: Record<string, any>
  ) { super(); }

  async *[Symbol.asyncIterator]() {
    for await (const row of this.source) {
      if (evaluatePredicate(this.node.predicate, row, this.parameters)) {
        yield row;
      }
    }
  }

  getSortOrder(): SortOrder | undefined {
    return this.source.getSortOrder();
  }
}
