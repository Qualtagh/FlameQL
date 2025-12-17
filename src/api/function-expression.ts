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

export function concatenate(exprs: ExpressionInput[]): FunctionExpression {
  return apply(exprs, (values: any[]) => values.join(''));
}

export function lowercase(expr: ExpressionInput): FunctionExpression {
  return apply(expr, (s: string) => s.toLowerCase());
}

export function uppercase(expr: ExpressionInput): FunctionExpression {
  return apply(expr, (s: string) => s.toUpperCase());
}

export function add(a: ExpressionInput, b: ExpressionInput): FunctionExpression {
  return apply([a, b], ([x, y]: [number, number]) => x + y);
}

export function subtract(a: ExpressionInput, b: ExpressionInput): FunctionExpression {
  return apply([a, b], ([x, y]: [number, number]) => x - y);
}

export function multiply(a: ExpressionInput, b: ExpressionInput): FunctionExpression {
  return apply([a, b], ([x, y]: [number, number]) => x * y);
}

export function divide(a: ExpressionInput, b: ExpressionInput): FunctionExpression {
  return apply([a, b], ([x, y]: [number, number]) => x / y);
}
