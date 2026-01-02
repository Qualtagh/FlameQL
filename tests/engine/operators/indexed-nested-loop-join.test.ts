import { and, collection, eq, field, gt, JoinStrategy, literal, or, PredicateMode, projection } from '../../../src/api/api';
import { IndexedNestedLoopJoinNode, NodeType, PreparedScanNode, ProjectNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { IndexManager } from '../../../src/engine/indexes/index-manager';
import { Planner } from '../../../src/engine/planner';
import { executeTest } from '../../helpers/test-utils';
import { clearDatabase, db } from '../../setup';

describe('IndexedNestedLoopJoinOperator', () => {
  let indexManager: IndexManager;

  beforeEach(async () => {
    await clearDatabase();
    indexManager = new IndexManager();
    // Configure indexes required for INLJ.
    // 'orders' collection needs indexes on fields used for joining.
    indexManager.loadFromFirestoreJson(JSON.stringify({
      indexes: [
        { collectionGroup: 'orders', queryScope: 'COLLECTION', fields: [{ fieldPath: 'userId', order: 'ASCENDING' }] },
        { collectionGroup: 'orders', queryScope: 'COLLECTION', fields: [{ fieldPath: 'region', order: 'ASCENDING' }] },
      ],
    }));
  });

  it('batches Firestore lookups using `in` (splits into multiple queries when left has many unique keys)', async () => {
    // Seed users + orders with 31 unique join keys (must be batched due to Firestore `in` limit).
    for (let i = 1; i <= 31; i++) {
      await db.collection('users').doc(String(i)).set({ id: i, name: `u${i}` });
      await db.collection('orders').doc(`o${i}`).set({ userId: i, total: i * 10 });
    }

    const p = projection({
      id: 'inlj-batching',
      from: { u: collection('users'), o: collection('orders') },
      select: { uId: field('u.id'), oTotal: field('o.total') },
      where: eq(field('u.id'), field('o.userId')),
      hints: { join: JoinStrategy.IndexedNestedLoop },
    });

    const planner = new Planner(indexManager);
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as IndexedNestedLoopJoinNode;

    expect(joinNode.type).toBe(NodeType.JOIN);
    expect(joinNode.joinType).toBe(JoinStrategy.IndexedNestedLoop);
    expect(joinNode.right.type).toBe(NodeType.PREPARED_SCAN);
    expect(joinNode.indexJoin).toBeDefined();
    expect(joinNode.indexJoin.mode).toBe('batch');

    const executor = new Executor(db, indexManager);
    const results = await executeTest(executor, plan, {});

    expect(results).toHaveLength(31);
    expect(results).toContainEqual({ uId: 1, oTotal: 10 });
    expect(results).toContainEqual({ uId: 31, oTotal: 310 });
  });

  it('supports a right-side FilterNode (non-indexable OR predicate) by post-filtering lookup results', async () => {
    await db.collection('users').doc('1').set({ id: 1, name: 'Alice', active: true });
    await db.collection('users').doc('2').set({ id: 2, name: 'Bob', active: true });

    await db.collection('orders').doc('o1').set({ userId: 1, status: 'paid', total: 10 });
    await db.collection('orders').doc('o2').set({ userId: 1, status: 'open', total: 20 });
    await db.collection('orders').doc('o3').set({ userId: 2, status: 'refunded', total: 30 });

    const p = projection({
      id: 'inlj-right-filter',
      from: { u: collection('users'), o: collection('orders') },
      where: and([
        eq(field('u.active'), literal(true)),
        eq(field('u.id'), field('o.userId')),
        or([
          eq(field('o.status'), literal('paid')),
          gt(field('o.total'), literal(25)),
        ]),
      ]),
      select: { uId: field('u.id'), oStatus: field('o.status') },
      hints: {
        join: JoinStrategy.IndexedNestedLoop,
        predicateMode: PredicateMode.Respect,
      },
    });

    const planner = new Planner(indexManager);
    const plan = planner.plan(p) as ProjectNode;

    // Verify the plan structure: Join -> Right is PREPARED_SCAN wrapping Filter -> Scan
    const joinNode = (plan.source.type === NodeType.JOIN ? plan.source : (plan.source as any).source) as IndexedNestedLoopJoinNode;
    // console.log('DEBUG: joinNode.right', JSON.stringify(joinNode.right, null, 2));
    expect(joinNode.joinType).toBe(JoinStrategy.IndexedNestedLoop);
    expect(joinNode.right.type).toBe(NodeType.PREPARED_SCAN);

    const prepared = joinNode.right as PreparedScanNode;
    expect(prepared.postFilter).toBeDefined(); // The OR predicate should be here

    const executor = new Executor(db, indexManager);
    const results = await executeTest(executor, plan, {});

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({ uId: 1, oStatus: 'paid' });
    expect(results).toContainEqual({ uId: 2, oStatus: 'refunded' });
  });

  it('is selected automatically (AUTO) when hash/merge are not chosen and a right-side index exists', async () => {
    await db.collection('users').doc('1').set({ id: 1, region: 'US' });
    await db.collection('users').doc('2').set({ id: 2, region: 'EU' });

    await db.collection('orders').doc('o1').set({ userId: 1, region: 'US', total: 10 });
    await db.collection('orders').doc('o2').set({ userId: 1, region: 'EU', total: 20 }); // region mismatch
    await db.collection('orders').doc('o3').set({ userId: 2, region: 'EU', total: 30 });

    // Join predicate is a conjunction (AND), so hash/merge are not selected; indexed nested loop should be.
    const p = projection({
      id: 'inlj-auto',
      from: { u: collection('users'), o: collection('orders') },
      where: and([
        eq(field('u.id'), field('o.userId')),
        eq(field('u.region'), field('o.region')),
      ]),
      select: { uId: field('u.id'), oTotal: field('o.total') },
      hints: { join: JoinStrategy.Auto },
    });

    const planner = new Planner(indexManager);
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as IndexedNestedLoopJoinNode;

    expect(joinNode.joinType).toBe(JoinStrategy.IndexedNestedLoop);
    expect(joinNode.right.type).toBe(NodeType.PREPARED_SCAN);
    expect(joinNode.indexJoin).toBeDefined();

    const executor = new Executor(db, indexManager);
    const results = await executeTest(executor, plan, {});

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({ uId: 1, oTotal: 10 });
    expect(results).toContainEqual({ uId: 2, oTotal: 30 });
  });

  it('supports inequality join operators (>) via per-row indexed lookups', async () => {
    await db.collection('users').doc('u1').set({ id: 1 });
    await db.collection('users').doc('u2').set({ id: 3 });
    await db.collection('users').doc('u3').set({ id: 5 });

    await db.collection('orders').doc('o1').set({ userId: 2 });
    await db.collection('orders').doc('o2').set({ userId: 4 });
    await db.collection('orders').doc('o3').set({ userId: 6 });

    const p = projection({
      id: 'inlj-ineq',
      from: { u: collection('users'), o: collection('orders') },
      select: { uId: field('u.id'), oUserId: field('o.userId') },
      where: gt(field('u.id'), field('o.userId')),
      hints: { join: JoinStrategy.IndexedNestedLoop },
    });

    const planner = new Planner(indexManager);
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as IndexedNestedLoopJoinNode;

    expect(joinNode.joinType).toBe(JoinStrategy.IndexedNestedLoop);
    expect(joinNode.right.type).toBe(NodeType.PREPARED_SCAN);
    expect(joinNode.indexJoin.mode).toBe('perRow');

    const executor = new Executor(db, indexManager);
    const results = await executeTest(executor, plan, {});

    expect(results).toHaveLength(3);
    expect(results).toContainEqual({ uId: 3, oUserId: 2 });
    expect(results).toContainEqual({ uId: 5, oUserId: 2 });
    expect(results).toContainEqual({ uId: 5, oUserId: 4 });
  });
});
