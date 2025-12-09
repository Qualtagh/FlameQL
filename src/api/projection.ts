import { z } from 'zod';
import { Field, fieldSchema } from './field';
import { queryHintsSchema } from './hints';
import { orderBySchema } from './order-by';
import { expressionSchema, predicateSchema } from './predicate';

const projectionSchema = z.object({
  id: z.string(),
  from: z.record(z.string(), z.unknown()),
  select: z.record(z.string(), expressionSchema).optional(),
  where: predicateSchema.optional(),
  params: z.record(z.string(), z.unknown()).optional(),
  materializeTo: z.unknown().optional(),
  key: fieldSchema.optional(),
  uniqueBy: z.array(fieldSchema).optional(),
  hints: queryHintsSchema.optional(),
  orderBy: orderBySchema.optional(),
  limit: z.number().int().nonnegative().optional(),
  offset: z.number().int().nonnegative().optional(),
});

type ProjectionInput = z.infer<typeof projectionSchema>;

export interface Projection extends ProjectionInput { }

export class Projection {
  constructor(opts: ProjectionInput) {
    const parsed = projectionSchema.parse(opts);
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
