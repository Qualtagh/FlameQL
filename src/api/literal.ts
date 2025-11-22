import { z } from 'zod';
import { CollectionPathSegment } from './collection';

export enum LiteralType {
  String = 'String',
  Number = 'Number',
  Boolean = 'Boolean',
  Null = 'Null',
}

const literalSchema = z.object({
  value: z.unknown(),
  type: z.enum(LiteralType),
});

type LiteralInput = z.infer<typeof literalSchema>;

export interface Literal extends LiteralInput { }

export class Literal implements CollectionPathSegment {
  readonly kind = 'Literal' as const;

  constructor(value: unknown, type: LiteralType) {
    Object.assign(this, literalSchema.parse({ value, type }));
  }
}
