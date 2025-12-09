import { type } from 'arktype';

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

export const { queryHints } = type.module({
  queryHints: {
    'predicateMode?': type.valueOf(PredicateMode),
    'predicateOrMode?': type.valueOf(PredicateOrMode),
    'join?': type.valueOf(JoinStrategy),
    'orderBy?': type.valueOf(OrderByStrategy),
  },
});
