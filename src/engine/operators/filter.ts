import { FilterNode } from '../ast';
import { evaluatePredicate } from '../evaluator';
import { Operator } from './operator';

export class Filter implements Operator {
  constructor(
    private source: Operator,
    private node: FilterNode,
    private parameters: Record<string, any>
  ) { }

  async *[Symbol.asyncIterator]() {
    for await (const row of this.source) {
      if (evaluatePredicate(this.node.predicate, row, this.parameters)) {
        yield row;
      }
    }
  }
}
