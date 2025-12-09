import { type } from 'arktype';
import { whereFilterOpType } from './external-types';
import { fieldType } from './field';
import { literalType } from './literal';
import { paramType } from './param';

export const { expression: expressionType, predicate: predicateType } = type.module({
  field: fieldType,
  literal: literalType,
  param: paramType,
  whereFilterOp: whereFilterOpType,
  expression: 'field | literal | param',
  COMPARISON: {
    type: "'COMPARISON'",
    left: 'expression',
    right: 'expression',
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
  predicate: 'COMPARISON | AND | OR | NOT | CONSTANT',
});

export type Expression = typeof expressionType.infer;
export type Predicate = typeof predicateType.infer;
export type ComparisonPredicate = Extract<Predicate, { type: 'COMPARISON' }>;
export type CompositePredicate = Extract<Predicate, { type: 'AND' | 'OR' }>;
export type NotPredicate = Extract<Predicate, { type: 'NOT' }>;
export type ConstantPredicate = Extract<Predicate, { type: 'CONSTANT' }>;
