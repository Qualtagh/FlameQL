import { z } from 'zod';
import { CollectionPathSegment } from './collection';

export enum LiteralType {
  String = 'String',
  Number = 'Number',
  Boolean = 'Boolean',
  Null = 'Null',
}

export const literalSchema = z.object({
  kind: z.literal('Literal').default('Literal'),
  value: z.unknown(),
  type: z.enum(LiteralType),
});

type LiteralInput = z.infer<typeof literalSchema>;

export interface Literal extends LiteralInput { }

export class Literal implements CollectionPathSegment {
  constructor(value: unknown, type: LiteralType) {
    Object.assign(this, literalSchema.parse({ value, type }));
  }
}

export function literal(value: unknown): Literal {
  if (value === null) {
    return new Literal(value, LiteralType.Null);
  }

  if (typeof value === 'number') {
    return new Literal(value, LiteralType.Number);
  }

  if (typeof value === 'boolean') {
    return new Literal(value, LiteralType.Boolean);
  }

  return new Literal(value, LiteralType.String);
}
