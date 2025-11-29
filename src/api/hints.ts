import { z } from 'zod';

export enum JoinType {
  NestedLoop = 'NestedLoop',
  IndexNestedLoop = 'IndexNestedLoop',
  Hash = 'Hash',
  Merge = 'Merge',
}

export const queryHintsSchema = z.object({
  joinType: z.enum(JoinType).optional().describe('Force a specific join strategy for all joins in the query'),
  useIndex: z.boolean().optional().describe('Hint for index usage: true - force index, false - avoid index, undefined - let planner decide'),
});

type QueryHintsInput = z.infer<typeof queryHintsSchema>;

export interface QueryHints extends QueryHintsInput { }

export class QueryHints {
  constructor(opts: QueryHintsInput) {
    Object.assign(this, queryHintsSchema.parse(opts));
  }
}

export function queryHints(opts: QueryHintsInput): QueryHints {
  return new QueryHints(opts);
}
