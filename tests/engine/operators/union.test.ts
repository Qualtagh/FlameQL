import { NodeType, ScanNode, UnionDistinctStrategy, UnionNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { clearDatabase, db } from '../../setup';

describe('Union Operator', () => {
  let executor: Executor;

  beforeEach(async () => {
    await clearDatabase();
    executor = new Executor(db);

    // Seed data
    await db.collection('users').doc('user1').set({ age: 25, city: 'NY', name: 'Alice' });
    await db.collection('users').doc('user2').set({ age: 30, city: 'LA', name: 'Bob' });
    await db.collection('users').doc('user3').set({ age: 25, city: 'LA', name: 'Charlie' });
    await db.collection('users').doc('user4').set({ age: 40, city: 'Chicago', name: 'David' });
  });

  describe('deduplicateByDocPath (optimizer-safe)', () => {
    test('should deduplicate by DOC_PATH', async () => {
      // Two full scans of same collection - same DOC_PATH values
      const scan1: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u',
        constraints: [],
      };

      const scan2: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u',
        constraints: [],
      };

      const unionPlan: UnionNode = {
        type: NodeType.UNION,
        inputs: [scan1, scan2],
        distinct: UnionDistinctStrategy.DocPath,
      };

      const results = await executor.execute(unionPlan);

      // 4 unique users (deduplicated by DOC_PATH)
      expect(results.length).toBe(4);
    });
  });

  describe('distinct (SQL semantics)', () => {
    test('should deduplicate by content hash', async () => {
      // Two full scans - SQL DISTINCT compares all field values
      const scan1: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u1',
        constraints: [],
      };

      const scan2: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u2',
        constraints: [],
      };

      const unionPlan: UnionNode = {
        type: NodeType.UNION,
        inputs: [scan1, scan2],
        distinct: UnionDistinctStrategy.HashMap,
      };

      const results = await executor.execute(unionPlan);

      // 8 results because different aliases make rows structurally different
      // (u1: {...} vs u2: {...})
      expect(results.length).toBe(8);
    });

    test('should deduplicate identical rows', async () => {
      // Same alias means identical structure
      const scan1: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u',
        constraints: [],
      };

      const scan2: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u',
        constraints: [],
      };

      const unionPlan: UnionNode = {
        type: NodeType.UNION,
        inputs: [scan1, scan2],
        distinct: UnionDistinctStrategy.HashMap,
      };

      const results = await executor.execute(unionPlan);

      // 4 unique rows (same alias, same data = same content hash)
      expect(results.length).toBe(4);
    });
  });

  describe('UNION ALL (no deduplication)', () => {
    test('should return all results', async () => {
      const scan1: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u1',
        constraints: [],
      };

      const scan2: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u2',
        constraints: [],
      };

      const unionPlan: UnionNode = {
        type: NodeType.UNION,
        inputs: [scan1, scan2],
        distinct: UnionDistinctStrategy.None,
      };

      const results = await executor.execute(unionPlan);

      // 8 total (4 + 4, no deduplication)
      expect(results.length).toBe(8);
    });
  });
});
