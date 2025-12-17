import { type } from 'arktype';
import type { Expression } from './predicate';

export const { functionExpression: functionExpressionType } = type.module({
  functionExpression: {
    kind: "'FunctionExpression'",
    input: 'unknown',
    fn: 'Function',
  },
});

export interface FunctionExpression {
  kind: 'FunctionExpression';
  input: Expression;
  fn: (value: any) => any;
}

export class FunctionExpression {
  constructor(input: Expression, fn: (value: any) => any) {
    const parsed = functionExpressionType.assert({ kind: 'FunctionExpression', input, fn });
    this.kind = parsed.kind;
    this.input = input;
    this.fn = fn;
  }
}

export function apply(input: Expression, fn: (value: any) => any): FunctionExpression {
  return new FunctionExpression(input, fn);
}
