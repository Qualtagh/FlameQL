import { ProjectNode } from '../ast';
import { evaluate } from '../evaluator';
import { Operator } from './operator';

export class Project implements Operator {
  constructor(
    private source: Operator,
    private node: ProjectNode,
    private parameters: Record<string, any>
  ) { }

  async *[Symbol.asyncIterator]() {
    for await (const row of this.source) {
      const result: any = {};
      for (const [key, expr] of Object.entries(this.node.fields)) {
        result[key] = evaluate(expr, row, this.parameters);
      }
      yield result;
    }
  }
}
