import { eq, field, JoinStrategy } from '../../../src/api/api';
import { JoinNode, NodeType, ScanNode, UnionDistinctStrategy, UnionNode } from '../../../src/engine/ast';
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

      const results = await executor.execute(unionPlan, {});

      // 4 unique users (deduplicated by DOC_PATH)
      expect(results.length).toBe(4);
    });

    test('should not collapse distinct join rows that share a left-side DOC_PATH', async () => {
      // Seed joinable data: two orders for the same user (same user DOC_PATH, different order DOC_PATH)
      await db.collection('orders').doc('o1').set({ userId: 'user1', item: 'x' });
      await db.collection('orders').doc('o2').set({ userId: 'user1', item: 'y' });
      await db.collection('orders').doc('o3').set({ userId: 'user2', item: 'z' });

      const usersScan: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'users',
        alias: 'u',
        constraints: [],
      };

      const ordersScan: ScanNode = {
        type: NodeType.SCAN,
        collectionPath: 'orders',
        alias: 'o',
        constraints: [],
      };

      const join: JoinNode = {
        type: NodeType.JOIN,
        left: usersScan,
        right: ordersScan,
        joinType: JoinStrategy.Hash,
        condition: eq(field('u.#id'), field('o.userId')),
      };

      // Duplicate the join subtree so every join row is produced twice.
      const unionPlan: UnionNode = {
        type: NodeType.UNION,
        inputs: [join, join],
        distinct: UnionDistinctStrategy.DocPath,
      };

      const results = await executor.execute(unionPlan, {});

      // Expected join cardinality:
      // user1 joins o1, o2 (2 rows); user2 joins o3 (1 row) => 3 rows total
      // UNION with doc-path-based dedupe should remove duplicates across inputs, but must keep
      // both rows for user1 (they differ by order DOC_PATH).
      expect(results.length).toBe(3);
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

      const results = await executor.execute(unionPlan, {});

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

      const results = await executor.execute(unionPlan, {});

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

      const results = await executor.execute(unionPlan, {});

      // 8 total (4 + 4, no deduplication)
      expect(results.length).toBe(8);
    });
  });
});
