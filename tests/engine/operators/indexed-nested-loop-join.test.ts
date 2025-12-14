import { collection, field, literal, projection } from '../../../src/api/api';
import { JoinStrategy, PredicateMode } from '../../../src/api/hints';
import { JoinNode, NodeType, ProjectNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { IndexManager } from '../../../src/engine/indexes/index-manager';
import { Planner } from '../../../src/engine/planner';
import { clearDatabase, db } from '../../setup';

describe('IndexedNestedLoopJoinOperator', () => {
  beforeEach(async () => {
    await clearDatabase();
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
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    // Force indexed nested loop join on equality predicate.
    joinNode.joinType = JoinStrategy.IndexedNestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: field('u.id'),
      right: field('o.userId'),
      operation: '==',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

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

    // Force planner to keep OR as a FilterNode (no UNION rewrite) so the RIGHT input becomes FILTER->SCAN.
    const p = projection({
      id: 'inlj-right-filter',
      from: { u: collection('users'), o: collection('orders') },
      where: {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: field('u.active'), right: literal(true), operation: '==' },
          {
            type: 'OR',
            conditions: [
              { type: 'COMPARISON', left: field('o.status'), right: literal('paid'), operation: '==' },
              { type: 'COMPARISON', left: field('o.status'), right: literal('refunded'), operation: '==' },
            ],
          },
        ],
      },
      select: { uId: field('u.id'), oStatus: field('o.status') },
      hints: { predicateMode: PredicateMode.Respect },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    // The plan is PROJECT over either JOIN or FILTER->JOIN depending on planner; find the join.
    const joinNode = (plan.source.type === NodeType.JOIN ? plan.source : (plan.source as any).source) as JoinNode;
    joinNode.joinType = JoinStrategy.IndexedNestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: field('u.id'),
      right: field('o.userId'),
      operation: '==',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({ uId: 1, oStatus: 'paid' });
    expect(results).toContainEqual({ uId: 2, oStatus: 'refunded' });
  });

  it('is selected automatically (AUTO) when hash/merge are not chosen and a right-side index exists', async () => {
    // Indexes: right-side lookup field is indexed (required for auto selection).
    const indexManager = new IndexManager();
    indexManager.loadFromFirestoreJson(JSON.stringify({
      indexes: [
        { collectionGroup: 'orders', queryScope: 'COLLECTION', fields: [{ fieldPath: 'userId' }] },
      ],
    }));

    await db.collection('users').doc('1').set({ id: 1, region: 'US' });
    await db.collection('users').doc('2').set({ id: 2, region: 'EU' });

    await db.collection('orders').doc('o1').set({ userId: 1, region: 'US', total: 10 });
    await db.collection('orders').doc('o2').set({ userId: 1, region: 'EU', total: 20 }); // region mismatch
    await db.collection('orders').doc('o3').set({ userId: 2, region: 'EU', total: 30 });

    // Join predicate is a conjunction (AND), so hash/merge are not selected; indexed nested loop should be.
    const p = projection({
      id: 'inlj-auto',
      from: { u: collection('users'), o: collection('orders') },
      where: {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: field('u.id'), right: field('o.userId'), operation: '==' },
          { type: 'COMPARISON', left: field('u.region'), right: field('o.region'), operation: '==' },
        ],
      },
      select: { uId: field('u.id'), oTotal: field('o.total') },
      hints: { join: JoinStrategy.Auto },
    });

    const planner = new Planner(indexManager);
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    expect(joinNode.joinType).toBe(JoinStrategy.IndexedNestedLoop);

    const executor = new Executor(db, indexManager);
    const results = await executor.execute(plan);

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
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.IndexedNestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: field('u.id'),
      right: field('o.userId'),
      operation: '>',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(3);
    expect(results).toContainEqual({ uId: 3, oUserId: 2 });
    expect(results).toContainEqual({ uId: 5, oUserId: 2 });
    expect(results).toContainEqual({ uId: 5, oUserId: 4 });
  });
});
