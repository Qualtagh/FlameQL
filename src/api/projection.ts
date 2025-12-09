import { type as arkType } from 'arktype';
import { Field, fieldType } from './field';
import { queryHints } from './hints';
import { orderBy } from './order-by';
import { expressionType, predicateType } from './predicate';

const { projection: projectionType } = arkType.module({
  field: fieldType,
  expression: expressionType,
  predicate: predicateType,
  orderBy,
  queryHints,
  projection: {
    id: 'string',
    from: { '[string]': 'unknown' },
    'select?': { '[string]': 'expression' },
    'where?': 'predicate',
    'params?': { '[string]': 'unknown' },
    'materializeTo?': 'unknown',
    'key?': 'field',
    'uniqueBy?': 'field[]',
    'hints?': 'queryHints',
    'orderBy?': 'orderBy',
    'limit?': 'number',
    'offset?': 'number',
  },
});

type ProjectionInput = typeof projectionType.infer;

export interface Projection extends ProjectionInput { }

export class Projection {
  constructor(opts: ProjectionInput) {
    const parsed = projectionType.assert(opts);
    validateAliases(parsed);
    Object.assign(this, parsed);
  }
}

export function projection(opts: ProjectionInput): Projection {
  return new Projection(opts);
}

function validateAliases(projection: ProjectionInput) {
  const aliases = Object.keys(projection.from ?? {});
  if (!aliases.length) {
    throw new Error('Projection requires at least one source alias in `from`.');
  }

  const known = new Set(aliases);
  walk(projection, value => {
    if (!known.has(value.source)) {
      throw new Error(`Unknown alias "${value.source}" referenced in projection "${projection.id}".`);
    }
  });
}

function walk(value: any, onField: (value: Field) => void) {
  if (!value || typeof value !== 'object') return;

  if (value instanceof Field) {
    return onField(value);
  }

  for (const k in value) {
    walk(value[k], onField);
  }
}
