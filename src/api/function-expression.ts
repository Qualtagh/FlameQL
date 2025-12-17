import { type } from 'arktype';
import { ExpressionInput } from './predicate';

export const { functionExpression: functionExpressionType } = type.module({
  functionExpression: {
    kind: "'FunctionExpression'",
    input: 'unknown',
    fn: 'Function',
  },
});

export interface FunctionExpression {
  kind: 'FunctionExpression';
  input: ExpressionInput;
  fn: (value: any) => any;
}

export class FunctionExpression {
  constructor(input: ExpressionInput, fn: (value: any) => any) {
    const parsed = functionExpressionType.assert({ kind: 'FunctionExpression', input, fn });
    this.kind = parsed.kind;
    this.input = input;
    this.fn = fn;
  }
}

export function apply(input: ExpressionInput, fn: (value: any) => any): FunctionExpression {
  return new FunctionExpression(input, fn);
}
