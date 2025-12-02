export type QueryScope = 'COLLECTION' | 'COLLECTION_GROUP';

export type IndexFieldMode = 'ASCENDING' | 'DESCENDING' | 'ARRAY_CONTAINS';

export interface IndexField {
  fieldPath: string;
  mode: IndexFieldMode;
}

export interface Index {
  collectionId: string; // collection name or collection group id
  queryScope: QueryScope;
  fields: IndexField[];
}
