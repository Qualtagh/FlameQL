import { type } from 'arktype';
import type { CollectionPathSegment } from './collection-path-segment';
import { collectionPathSegmentType } from './collection-path-segment';
import { Field } from './field';
import { Literal, LiteralType } from './literal';

const { collection: collectionType } = type.module({
  collectionPathSegment: collectionPathSegmentType,
  collection: {
    group: 'boolean | undefined',
    path: 'collectionPathSegment[]',
  },
});

type CollectionInput = typeof collectionType.infer;

export interface Collection extends CollectionInput { }

export class Collection {
  constructor(opts: CollectionInput) {
    Object.assign(this, collectionType.assert(opts));
  }
}

function parseSegment(seg: string): CollectionPathSegment {
  const braceMatch = seg.match(/^\{(.+)\}$/);
  if (!braceMatch) {
    return new Literal(seg, LiteralType.String);
  }
  const inside = braceMatch[1];
  const parts = inside.split('.');
  const source = parts.shift()!;
  return new Field(source, parts);
}

function makeCollection(pathStr: string, forceGroup: boolean): Collection {
  const rawSegments = pathStr.split('/').filter(s => s.length > 0);
  const nodes = rawSegments.map(parseSegment);
  const group = forceGroup ? true : rawSegments.length <= 1 ? false : undefined;
  return new Collection({ group, path: nodes });
}

export function collection(path: string): Collection {
  return makeCollection(path, false);
}

export function collectionGroup(name: string): Collection {
  return makeCollection(name, true);
}
