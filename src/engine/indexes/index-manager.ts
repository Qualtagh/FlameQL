import { Index, IndexField, IndexFieldMode, QueryScope } from './index-definitions';

interface FirestoreIndexJson {
  indexes?: Array<{
    collectionGroup: string;
    queryScope: string;
    fields: Array<{
      fieldPath: string;
      order?: string;
      arrayConfig?: string;
    }>;
  }>;
}

export class IndexManager {
  private indexes: Index[] = [];

  addIndex(index: Index) {
    this.indexes.push(index);
  }

  getIndexes(collectionId: string): Index[] {
    return this.indexes.filter(
      (index) => index.collectionId === collectionId
    );
  }

  loadFromFirestoreJson(jsonContent: string) {
    try {
      const parsed: FirestoreIndexJson = JSON.parse(jsonContent);

      if (parsed.indexes) {
        for (const idx of parsed.indexes) {
          const fields: IndexField[] = idx.fields.map((f) => {
            let mode: IndexFieldMode = 'ASCENDING';
            if (f.order === 'DESCENDING') {
              mode = 'DESCENDING';
            } else if (f.arrayConfig === 'CONTAINS') {
              mode = 'ARRAY_CONTAINS';
            }
            return {
              fieldPath: f.fieldPath,
              mode: mode,
            };
          });

          this.addIndex({
            collectionId: idx.collectionGroup,
            queryScope: idx.queryScope as QueryScope,
            fields: fields,
          });
        }
      }
    } catch (e) {
      console.error('Failed to parse firestore.indexes.json', e);
      throw e;
    }
  }

  /**
   * Deduces single-field indexes for the given fields.
   * Firestore automatically creates single-field indexes for each field in a document,
   * unless explicitly exempted.
   * This method assumes all provided fields have single-field indexes (ASC and DESC).
   */
  deduceSingleFieldIndexes(collectionId: string, fields: string[]) {
    for (const field of fields) {
      // Ascending index
      this.addIndex({
        collectionId,
        queryScope: 'COLLECTION',
        fields: [{ fieldPath: field, mode: 'ASCENDING' }],
      });

      // Descending index
      this.addIndex({
        collectionId,
        queryScope: 'COLLECTION',
        fields: [{ fieldPath: field, mode: 'DESCENDING' }],
      });

      // Array-contains index is also automatic for arrays, but we don't know types here.
      // We'll skip array-contains for now or add it if needed.
      // For now, let's stick to ASC/DESC as they are most relevant for sorting.
    }
  }
}
