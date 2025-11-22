import { z } from 'zod';
import { CollectionPathSegment } from './collection';

const fieldSchema = z.object({
  // {j.jobId} => source = 'j'
  // {jobId} => source = null
  source: z.string().nullable(),
  path: z.array(z.string()),
});

type FieldInput = z.infer<typeof fieldSchema>;

export interface Field extends FieldInput { }

export class Field implements CollectionPathSegment {
  readonly kind = 'Field' as const;

  constructor(source: string | null, path: string[]) {
    Object.assign(this, fieldSchema.parse({ source, path }));
  }
}
