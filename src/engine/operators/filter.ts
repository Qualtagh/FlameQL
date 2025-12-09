import { FilterNode } from '../ast';
import { evaluatePredicate } from '../evaluator';
import { Operator, SortOrder } from './operator';

export class Filter implements Operator {
  constructor(
    private source: Operator,
    private node: FilterNode
  ) { }

  async next(): Promise<any | null> {
    let row;
    while (row = await this.source.next()) {
      if (evaluatePredicate(this.node.predicate, row)) {
        return row;
      }
    }
    return null;
  }

  getSortOrder(): SortOrder | undefined {
    return this.source.getSortOrder();
  }
}
