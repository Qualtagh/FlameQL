import { OrderByDirection } from '@google-cloud/firestore';
import { UnionDistinctStrategy } from '../ast';
import { evaluatePredicate, getValue } from '../evaluator';
import { getDocData } from '../utils/firestore-utils';
import { JoinHashTable } from '../utils/hash-join-utils';
import { mergeJoin } from '../utils/merge-join-utils';
import { sortBuffer, SortComparator } from '../utils/sort-utils';
import { createUnionDeduplicator } from '../utils/union-utils';

// Re-export symbols for use in generated code
export { evaluatePredicate, getDocData, getValue, JoinHashTable, mergeJoin };

/**
 * Helper for UNION operator with deduplication.
 */
export async function* unionRows(
  generators: (() => AsyncGenerator<any, void, unknown>)[],
  distinctStrategy: UnionDistinctStrategy = UnionDistinctStrategy.None
): AsyncGenerator<any, void, unknown> {
  const isDuplicate = createUnionDeduplicator(distinctStrategy);
  for (const genFactory of generators) {
    const gen = genFactory();
    for await (const row of gen) {
      if (isDuplicate(row)) continue;
      yield row;
    }
  }
}

/**
 * Helper for SORT operator (requires in-memory buffering).
 */
export async function* sortRows(
  generator: AsyncGenerator<any, void, unknown>,
  orderBy: { selector: (row: any, params: Record<string, any>) => any; direction: OrderByDirection }[],
  params: Record<string, any>
): AsyncGenerator<any, void, unknown> {
  const buffer: any[] = [];
  for await (const row of generator) {
    buffer.push(row);
  }

  if (buffer.length === 0) return;

  const comparators: SortComparator[] = orderBy.map(spec => ({
    getValue: (row: any) => spec.selector(row, params),
    direction: spec.direction,
  }));

  sortBuffer(buffer, comparators);

  for (const row of buffer) {
    yield row;
  }
}

/**
 * Helper for batching rows from a generator.
 */
export async function* batchRows(
  generator: AsyncGenerator<any, void, unknown>,
  batchSize: number
): AsyncGenerator<any[], void, unknown> {
  let batch: any[] = [];
  for await (const row of generator) {
    batch.push(row);
    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }
  if (batch.length > 0) {
    yield batch;
  }
}
