import { ProjectNode } from '../ast';
import { evaluate } from '../evaluator';
import { Operator, SortOrder } from './operator';

export class Project implements Operator {
  constructor(
    private source: Operator,
    private node: ProjectNode,
    private parameters: Record<string, any>
  ) { }

  async next(): Promise<any | null> {
    const row = await this.source.next();
    if (!row) return null;

    const result: any = {};
    for (const [key, expr] of Object.entries(this.node.fields)) {
      result[key] = evaluate(expr, row, this.parameters);
    }
    return result;
  }

  getSortOrder(): SortOrder | undefined {
    return this.source.getSortOrder();
  }
}
