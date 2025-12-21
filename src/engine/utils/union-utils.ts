import { UnionDistinctStrategy } from '../ast';
import { DOC_PATH } from '../symbols';

/**
 * Recursively sorts object keys to produce deterministic structures that can be
 * stringified in a stable way.
 */
function sortObjectKeys(obj: any): any {
  if (obj === null || typeof obj !== 'object') return obj;
  if (Array.isArray(obj)) return obj.map(sortObjectKeys);

  const sorted: Record<string, any> = {};
  for (const key of Object.keys(obj).sort()) {
    sorted[key] = sortObjectKeys(obj[key]);
  }
  return sorted;
}

function jsonReplacer(_key: string, value: any): any {
  if (value instanceof Date) {
    return { __type: 'Date', value: value.toISOString() };
  }
  if (typeof value === 'bigint') {
    return { __type: 'BigInt', value: value.toString() };
  }
  return value;
}

/**
 * Generates a stable string key for a row, handling Dates and BigInts so
 * JSON.stringify does not throw and remains deterministic.
 */
function serializeRow(row: any): string {
  return JSON.stringify(sortObjectKeys(row), jsonReplacer);
}

/**
 * Extracts a doc-path-based deduplication key from a row. The key accounts for
 * aliased join tuples so distinct rows with the same left-side doc are not
 * collapsed.
 */
function extractDocPathKey(row: any): string | undefined {
  if (!row || typeof row !== 'object') return undefined;

  const pairs: Array<[string, string]> = [];

  // Flat doc row (defensive; current engine typically uses aliased rows)
  const top = row[DOC_PATH];
  if (typeof top === 'string') {
    pairs.push(['__root__', top]);
  }

  // Aliased structure: `{ alias: docData }`
  for (const key of Object.keys(row).sort()) {
    const nested = row[key];
    const path = nested && typeof nested === 'object' ? nested[DOC_PATH] : undefined;
    if (typeof path === 'string') {
      pairs.push([key, path]);
    }
  }

  if (pairs.length === 0) return undefined;
  return pairs.map(([k, p]) => `${k}:${p}`).join('|');
}

/**
 * Builds a duplicate checker for union operations. It reuses provided sets when
 * passed (e.g., operator-scoped) or allocates internal ones for local use
 * (e.g., runtime helper).
 */
export function createUnionDeduplicator(
  distinctStrategy: UnionDistinctStrategy,
  options: { seenPaths?: Set<string>; seenRows?: Set<string> } = {}
): (row: any) => boolean {
  const seenPaths = options.seenPaths ?? new Set<string>();
  const seenRows = options.seenRows ?? new Set<string>();

  switch (distinctStrategy) {
    case UnionDistinctStrategy.DocPath:
      return (row: any) => {
        const key = extractDocPathKey(row);
        if (!key) return false;
        if (seenPaths.has(key)) return true;
        seenPaths.add(key);
        return false;
      };
    case UnionDistinctStrategy.HashMap:
      return (row: any) => {
        const key = serializeRow(row);
        if (seenRows.has(key)) return true;
        seenRows.add(key);
        return false;
      };
    case UnionDistinctStrategy.None:
      return () => false;
    default:
      distinctStrategy satisfies never;
      throw new Error(`Invalid distinct strategy: ${distinctStrategy}`);
  }
}
