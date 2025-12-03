import { OrderByDirection } from '@google-cloud/firestore';
import { FilterNode } from '../ast';
import { Operator, SortOrder } from './operator';

export class Filter implements Operator {
  constructor(
    private source: Operator,
    private node: FilterNode
  ) { }

  async next(): Promise<any | null> {
    let row;
    while (row = await this.source.next()) {
      // TODO: Evaluate predicate
      // For now, assume predicate is true or implement simple check
      if (this.evaluatePredicate(this.node.predicate, row)) {
        return row;
      }
    }
    return null;
  }

  private evaluatePredicate(_predicate: any, _row: any): boolean {
    // Placeholder for predicate evaluation
    return true;
  }

  getSortOrder(): SortOrder | undefined {
    return this.source.getSortOrder();
  }

  requestSort(field: string, direction: OrderByDirection): boolean {
    return this.source.requestSort(field, direction);
  }
}
