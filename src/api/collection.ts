import { z } from 'zod';
import { Field } from './field';
import { Literal, LiteralType } from './literal';

export interface CollectionPathSegment { }

const collectionSchema = z.object({
  group: z.boolean(),
  path: z.array(z.unknown()),
});

type CollectionInput = z.infer<typeof collectionSchema>;

export interface Collection extends CollectionInput { }

export class Collection {
  constructor(opts: CollectionInput) {
    Object.assign(this, collectionSchema.parse(opts));
  }
}

function parseSegment(seg: string): CollectionPathSegment {
  const braceMatch = seg.match(/^\{(.+)\}$/);
  if (!braceMatch) {
    return new Literal(seg, LiteralType.String);
  }
  const inside = braceMatch[1];
  const parts = inside.split('.');
  const source = parts.shift() || null;
  return new Field(source, parts);
}

function makeCollection(pathStr: string, forceGroup: boolean): Collection {
  const rawSegments = pathStr.split('/').filter(s => s.length > 0);
  const nodes = rawSegments.map(parseSegment);
  const group = forceGroup || rawSegments.length > 1;
  return new Collection({ group, path: nodes });
}

export function collection(path: string): Collection {
  return makeCollection(path, false);
}

export function collectionGroup(name: string): Collection {
  return makeCollection(name, true);
}
