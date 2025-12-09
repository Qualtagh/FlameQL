import { field, literal } from '../../../src/api/api';
import { Field, Literal } from '../../../src/api/expression';
import { Constraint } from '../../../src/engine/ast';
import { IndexManager } from '../../../src/engine/indexes/index-manager';
import { SortOrder } from '../../../src/engine/operators/operator';

const eq = (a: Field, b: Literal): Constraint => ({ field: a, op: '==', value: b });
const gt = (a: Field, b: Literal): Constraint => ({ field: a, op: '>', value: b });

describe('IndexManager', () => {
  let indexManager: IndexManager;

  beforeEach(() => {
    indexManager = new IndexManager();
  });

  describe('loadFromFirestoreJson', () => {
    it('should parse valid JSON correctly', () => {
      const json = JSON.stringify({
        indexes: [
          {
            collectionGroup: 'posts',
            queryScope: 'COLLECTION',
            fields: [
              { fieldPath: 'authorId', order: 'ASCENDING' },
              { fieldPath: 'timestamp', order: 'DESCENDING' },
            ],
          },
        ],
      });

      indexManager.loadFromFirestoreJson(json);
      const indexes = indexManager.getIndexes('posts');

      expect(indexes).toHaveLength(1);
      expect(indexes[0].fields).toHaveLength(2);
      expect(indexes[0].fields[0]).toEqual({ fieldPath: 'authorId', mode: 'ASCENDING' });
      expect(indexes[0].fields[1]).toEqual({ fieldPath: 'timestamp', mode: 'DESCENDING' });
    });

    it('should handle array-contains config', () => {
      const json = JSON.stringify({
        indexes: [
          {
            collectionGroup: 'posts',
            queryScope: 'COLLECTION',
            fields: [
              { fieldPath: 'tags', arrayConfig: 'CONTAINS' },
            ],
          },
        ],
      });

      indexManager.loadFromFirestoreJson(json);
      const indexes = indexManager.getIndexes('posts');

      expect(indexes[0].fields[0]).toEqual({ fieldPath: 'tags', mode: 'ARRAY_CONTAINS' });
    });
  });

  describe('deduceSingleFieldIndexes', () => {
    it('should add ASC and DESC indexes for fields', () => {
      indexManager.deduceSingleFieldIndexes('users', ['name', 'age']);
      const indexes = indexManager.getIndexes('users');

      // 2 fields * 2 modes = 4 indexes
      expect(indexes).toHaveLength(4);

      const nameAsc = indexes.find(i => i.fields[0].fieldPath === 'name' && i.fields[0].mode === 'ASCENDING');
      const nameDesc = indexes.find(i => i.fields[0].fieldPath === 'name' && i.fields[0].mode === 'DESCENDING');

      expect(nameAsc).toBeDefined();
      expect(nameDesc).toBeDefined();
    });
  });

  describe('match', () => {
    beforeEach(() => {
      // Setup some indexes
      indexManager.deduceSingleFieldIndexes('users', ['name', 'age', 'status']);

      // Composite index: status ASC, age DESC
      indexManager.addIndex({
        collectionId: 'users',
        queryScope: 'COLLECTION',
        fields: [
          { fieldPath: 'status', mode: 'ASCENDING' },
          { fieldPath: 'age', mode: 'DESCENDING' },
        ],
      });
    });

    it('should match exact single field equality', () => {
      const constraints = [eq(field('u.name'), literal('Alice'))];
      const match = indexManager.match('users', constraints);

      expect(match.type).toBe('exact');
      expect(match.index).not.toBeNull();
    });

    it('should match composite index', () => {
      const constraints = [
        eq(field('u.status'), literal('active')),
        gt(field('u.age'), literal(20)),
      ];
      // Note: Firestore requires equality fields first, then inequality.
      // Our index is (status ASC, age DESC).
      // status == 'active' matches first field.
      // age > 20 matches second field (inequality).

      const match = indexManager.match('users', constraints);
      expect(match.type).toBe('exact');
      expect(match.index?.fields[0].fieldPath).toBe('status');
      expect(match.index?.fields[1].fieldPath).toBe('age');
    });

    it('should match sort order', () => {
      const constraints = [eq(field('u.status'), literal('active'))];
      const sort: SortOrder = { field: 'age', direction: 'desc' };

      const match = indexManager.match('users', constraints, sort);
      expect(match.type).toBe('exact');
      expect(match.index?.fields[0].fieldPath).toBe('status');
      expect(match.index?.fields[1].fieldPath).toBe('age');
    });

    it('should return partial match if sort is not covered', () => {
      const constraints = [eq(field('u.status'), literal('active'))];
      const sort: SortOrder = { field: 'name', direction: 'asc' };

      // We have index on status, but not (status, name).
      // But we have single index on name.
      // However, we can't use index(status) to sort by name.
      // We can use index(status) to filter, then sort in memory.
      // Or use index(name) to sort, but then filter manually? No, Firestore doesn't support that easily for equality.
      // Actually, if we use index(status), we get results sorted by status (and then docId).
      // So this is a partial match.

      const match = indexManager.match('users', constraints, sort);
      expect(match.type).toBe('partial');
    });

    it('should return none if no index matches constraints', () => {
      const constraints = [eq(field('u.missingField'), literal('value'))];
      const match = indexManager.match('users', constraints);
      expect(match.type).toBe('none');
    });
  });
});
