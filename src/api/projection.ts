import { z } from 'zod';
import { queryHintsSchema } from './hints';

const projectionSchema = z.object({
  id: z.string(),
  from: z.record(z.string(), z.unknown()).optional(),
  key: z.unknown().optional(),
  select: z.unknown().optional(),
  where: z.unknown().optional(),
  params: z.record(z.string(), z.unknown()).optional(),
  materializeTo: z.unknown().optional(),
  uniqueBy: z.unknown().optional(),
  hints: queryHintsSchema.optional(),
});

type ProjectionInput = z.infer<typeof projectionSchema>;

export interface Projection extends ProjectionInput { }

export class Projection {
  constructor(opts: ProjectionInput) {
    Object.assign(this, projectionSchema.parse(opts));
  }
}

export function projection(opts: ProjectionInput): Projection {
  return new Projection(opts);
}
