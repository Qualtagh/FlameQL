import { type } from 'arktype';
import { fieldType } from './field';
import { literalType } from './literal';

export const { collectionPathSegment: collectionPathSegmentType } = type.module({
  field: fieldType,
  literal: literalType,
  collectionPathSegment: 'field | literal',
});

export type CollectionPathSegment = typeof collectionPathSegmentType.infer;
