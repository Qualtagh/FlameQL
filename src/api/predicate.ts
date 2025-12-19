import { WhereFilterOp } from '@google-cloud/firestore';
import { type } from 'arktype';
import { whereFilterOpType } from './external-types';
import { Field, fieldType } from './field';
import { functionExpressionType } from './function-expression';
import { literalType } from './literal';
import { paramType } from './param';

export const { expression: expressionType, predicate: predicateType } = type.module({
  field: fieldType,
  literal: literalType,
  param: paramType,
  functionExpression: functionExpressionType,
  whereFilterOp: whereFilterOpType,
  expression: 'field | literal | param | functionExpression',
  COMPARISON: {
    type: "'COMPARISON'",
    left: 'expression',
    right: 'expression | expression[]',
    operation: 'whereFilterOp',
  },
  AND: {
    type: "'AND'",
    conditions: 'predicate[]',
  },
  OR: {
    type: "'OR'",
    conditions: 'predicate[]',
  },
  NOT: {
    type: "'NOT'",
    operand: 'predicate',
  },
  CONSTANT: {
    type: "'CONSTANT'",
    value: 'boolean',
  },
  CUSTOM: {
    type: "'CUSTOM'",
    input: 'unknown',
    fn: 'Function',
    'metadata?': 'unknown',
  },
  predicate: 'COMPARISON | AND | OR | NOT | CONSTANT | CUSTOM',
});

export type Expression = typeof expressionType.infer;
export type ExpressionInput = Expression | ExpressionInput[];
export type Predicate = typeof predicateType.infer;
export type ComparisonPredicate = Extract<Predicate, { type: 'COMPARISON' }>;
export type CompositePredicate = Extract<Predicate, { type: 'AND' | 'OR' }>;
export type NotPredicate = Extract<Predicate, { type: 'NOT' }>;
export type ConstantPredicate = Extract<Predicate, { type: 'CONSTANT' }>;
export type CustomPredicate = Extract<Predicate, { type: 'CUSTOM' }>;

export function eq(left: Expression, right: Expression): ComparisonPredicate {
  return comparison('==', left, right);
}

export function ne(left: Expression, right: Expression): ComparisonPredicate {
  return comparison('!=', left, right);
}

export function lt(left: Expression, right: Expression): ComparisonPredicate {
  return comparison('<', left, right);
}

export function lte(left: Expression, right: Expression): ComparisonPredicate {
  return comparison('<=', left, right);
}

export function gt(left: Expression, right: Expression): ComparisonPredicate {
  return comparison('>', left, right);
}

export function gte(left: Expression, right: Expression): ComparisonPredicate {
  return comparison('>=', left, right);
}

export function arrayContains(left: Expression, right: Expression): ComparisonPredicate {
  return comparison('array-contains', left, right);
}

export function inList(left: Expression, right: Expression[] | Field): ComparisonPredicate {
  return comparison('in', left, right);
}

export function notInList(left: Expression, right: Expression[] | Field): ComparisonPredicate {
  return comparison('not-in', left, right);
}

export function arrayContainsAny(left: Expression, right: Expression[] | Field): ComparisonPredicate {
  return comparison('array-contains-any', left, right);
}

export function and(conditions: Predicate[]): CompositePredicate {
  return { type: 'AND', conditions };
}

export function or(conditions: Predicate[]): CompositePredicate {
  return { type: 'OR', conditions };
}

export function not(operand: Predicate): NotPredicate {
  return { type: 'NOT', operand };
}

export function constant(value: boolean): ConstantPredicate {
  return { type: 'CONSTANT', value };
}

function comparison(operation: WhereFilterOp, left: Expression, right: Expression | Expression[]): ComparisonPredicate {
  return { type: 'COMPARISON', operation, left, right };
}

export function compare(
  input: ExpressionInput,
  fn: (value: any) => boolean,
  metadata?: any
): CustomPredicate {
  const custom = { type: 'CUSTOM', input, fn } as CustomPredicate;

  if (metadata) {
    custom.metadata = metadata;
  }

  return custom;
}

export function like(left: Expression, pattern: Expression): CustomPredicate {
  const cache: { pattern?: string, regex?: RegExp } = {};
  return compare(
    [left, pattern],
    ([value, pat]) => {
      // Convert SQL LIKE pattern to regex
      // % -> .*, _ -> .
      // Escape special regex characters first
      if (cache.pattern === pat) {
        return cache.regex!.test(value);
      }
      const escaped = pat.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = escaped.replace(/%/g, '.*').replace(/_/g, '.');
      cache.regex = new RegExp(`^${regex}$`);
      cache.pattern = pat;
      return cache.regex.test(value);
    },
    { name: 'like' }
  );
}
