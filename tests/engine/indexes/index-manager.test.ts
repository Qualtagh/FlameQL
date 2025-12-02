import { Index } from '../../../src/engine/indexes/index-definitions';
import { IndexManager } from '../../../src/engine/indexes/index-manager';

console.log('IndexManager test file loaded');

describe('IndexManager', () => {
  let indexManager: IndexManager;

  beforeEach(() => {
    indexManager = new IndexManager();
  });

  it('should load indexes from firestore.indexes.json format', () => {
    const json = `
    {
      "indexes": [
        {
          "collectionGroup": "posts",
          "queryScope": "COLLECTION",
          "fields": [
            { "fieldPath": "author", "order": "ASCENDING" },
            { "fieldPath": "timestamp", "order": "DESCENDING" }
          ]
        },
        {
          "collectionGroup": "comments",
          "queryScope": "COLLECTION_GROUP",
          "fields": [
            { "fieldPath": "text", "arrayConfig": "CONTAINS" }
          ]
        }
      ]
    }
    `;

    indexManager.loadFromFirestoreJson(json);

    const postsIndexes = indexManager.getIndexes('posts');
    expect(postsIndexes).toHaveLength(1);
    expect(postsIndexes[0].collectionId).toBe('posts');
    expect(postsIndexes[0].queryScope).toBe('COLLECTION');
    expect(postsIndexes[0].fields).toEqual([
      { fieldPath: 'author', mode: 'ASCENDING' },
      { fieldPath: 'timestamp', mode: 'DESCENDING' },
    ]);

    const commentsIndexes = indexManager.getIndexes('comments');
    expect(commentsIndexes).toHaveLength(1);
    expect(commentsIndexes[0].collectionId).toBe('comments');
    expect(commentsIndexes[0].queryScope).toBe('COLLECTION_GROUP');
    expect(commentsIndexes[0].fields).toEqual([
      { fieldPath: 'text', mode: 'ARRAY_CONTAINS' },
    ]);
  });

  it('should deduce single field indexes', () => {
    indexManager.deduceSingleFieldIndexes('users', ['name', 'age']);

    const indexes = indexManager.getIndexes('users');
    // Expect 2 indexes per field (ASC, DESC) -> 4 total
    expect(indexes).toHaveLength(4);

    const nameAsc = indexes.find((i: Index) => i.fields[0].fieldPath === 'name' && i.fields[0].mode === 'ASCENDING');
    const nameDesc = indexes.find((i: Index) => i.fields[0].fieldPath === 'name' && i.fields[0].mode === 'DESCENDING');
    const ageAsc = indexes.find((i: Index) => i.fields[0].fieldPath === 'age' && i.fields[0].mode === 'ASCENDING');
    const ageDesc = indexes.find((i: Index) => i.fields[0].fieldPath === 'age' && i.fields[0].mode === 'DESCENDING');

    expect(nameAsc).toBeDefined();
    expect(nameDesc).toBeDefined();
    expect(ageAsc).toBeDefined();
    expect(ageDesc).toBeDefined();
  });

  it('should return empty array for unknown collection', () => {
    expect(indexManager.getIndexes('unknown')).toEqual([]);
  });
});
