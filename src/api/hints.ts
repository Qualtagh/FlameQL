import { z } from 'zod';

export enum PredicateMode {
  Respect = 'respect',
  Auto = 'auto',
}

export enum PredicateOrMode {
  Union = 'union',
  SingleScan = 'single-scan',
  Auto = 'auto',
}

export enum JoinStrategy {
  Hash = 'hash',
  Merge = 'merge',
  NestedLoop = 'nested-loop',
  IndexedNestedLoop = 'indexed-nested-loop',
  Auto = 'auto',
}

export enum OrderByStrategy {
  DbSide = 'db-side',
  PostFetchSort = 'post-fetch-sort',
  Auto = 'auto',
}

export const queryHintsSchema = z.object({
  predicateMode: z.enum(PredicateMode).optional(),
  predicateOrMode: z.enum(PredicateOrMode).optional(),
  join: z.enum(JoinStrategy).optional(),
  orderBy: z.enum(OrderByStrategy).optional(),
});
