import { z } from 'zod';
import { CollectionPathSegment } from './collection';

export const fieldSchema = z.object({
  kind: z.literal('Field').default('Field'),
  source: z.string(),
  path: z.array(z.string()).min(1),
});

type FieldInput = z.infer<typeof fieldSchema>;

export interface Field extends FieldInput { }

export class Field implements CollectionPathSegment {
  constructor(source: string, path: string[]) {
    Object.assign(this, fieldSchema.parse({ source, path }));
  }
}

export function field(ref: string): Field {
  if (typeof ref !== 'string') {
    throw new Error('field() expects a string in the form "alias.path".');
  }

  const segments = ref.split('.');
  if (segments.length < 2) {
    throw new Error(`Field reference "${ref}" must include alias and path.`);
  }

  const [source, ...path] = segments;
  return new Field(source, path);
}
