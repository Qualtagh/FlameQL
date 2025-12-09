import { WhereFilterOp } from '@google-cloud/firestore';
import { z } from 'zod';
import { fieldSchema } from './field';
import { literalSchema } from './literal';
import { paramSchema } from './param';

export const expressionSchema = z.union([fieldSchema, literalSchema, paramSchema]);

export type Expression = z.infer<typeof expressionSchema>;

export interface ComparisonPredicate {
  type: 'COMPARISON';
  left: Expression;
  right: Expression;
  operation: WhereFilterOp;
}

export interface CompositePredicate {
  type: 'AND' | 'OR';
  conditions: Predicate[];
}

export interface NotPredicate {
  type: 'NOT';
  operand: Predicate;
}

export interface ConstantPredicate {
  type: 'CONSTANT';
  value: boolean;
}

export type Predicate = ComparisonPredicate | CompositePredicate | NotPredicate | ConstantPredicate;

export const predicateSchema: z.ZodType<Predicate> = z.lazy(() =>
  z.union([
    z.object({
      type: z.literal('COMPARISON'),
      left: expressionSchema,
      right: expressionSchema,
      operation: z.custom<WhereFilterOp>(op => typeof op === 'string'),
    }),
    z.object({
      type: z.literal('AND'),
      conditions: z.array(predicateSchema),
    }),
    z.object({
      type: z.literal('OR'),
      conditions: z.array(predicateSchema),
    }),
    z.object({
      type: z.literal('NOT'),
      operand: predicateSchema,
    }),
    z.object({
      type: z.literal('CONSTANT'),
      value: z.boolean(),
    }),
  ])
);
