import { collection } from '../../../src/api/collection';
import { JoinType } from '../../../src/api/hints';
import { projection } from '../../../src/api/projection';
import { JoinNode, ProjectNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { Planner } from '../../../src/engine/planner';
import { clearDatabase, db } from '../../setup';

describe('NestedLoopJoinOperator', () => {
  beforeEach(async () => {
    await clearDatabase();
  });

  it('should join using NestedLoopJoinStrategy (equality condition)', async () => {
    // Seed data
    await db.collection('users').doc('1').set({ id: 1, val: 'a' });
    await db.collection('users').doc('2').set({ id: 2, val: 'b' });

    await db.collection('orders').doc('101').set({ userId: 1, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 2, other: 'y' });
    await db.collection('orders').doc('103').set({ userId: 1, other: 'z' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: 'u.val', oOther: 'o.other' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    // Force NestedLoop Join
    joinNode.joinType = JoinType.NestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 'u.id',
      right: 'o.userId',
      operation: '==',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(3);
    expect(results).toContainEqual({ uVal: 'a', oOther: 'x' });
    expect(results).toContainEqual({ uVal: 'a', oOther: 'z' });
    expect(results).toContainEqual({ uVal: 'b', oOther: 'y' });
  });

  it('should support inequality operations', async () => {
    await db.collection('scores').doc('s1').set({ val: 10 });
    await db.collection('scores').doc('s2').set({ val: 20 });
    await db.collection('scores').doc('s3').set({ val: 30 });

    await db.collection('thresholds').doc('t1').set({ limit: 15 });
    await db.collection('thresholds').doc('t2').set({ limit: 25 });

    const p = projection({
      id: 'test',
      from: { s: collection('scores'), t: collection('thresholds') },
      select: { sVal: 's.val', tLimit: 't.limit' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.NestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 's.val',
      right: 't.limit',
      operation: '>',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    // 20 > 15
    // 30 > 15
    // 30 > 25
    expect(results).toHaveLength(3);
    expect(results).toContainEqual({ sVal: 20, tLimit: 15 });
    expect(results).toContainEqual({ sVal: 30, tLimit: 15 });
    expect(results).toContainEqual({ sVal: 30, tLimit: 25 });
  });

  it('should support array-contains operation', async () => {
    await db.collection('posts').doc('p1').set({ tags: ['a', 'b', 'c'] });
    await db.collection('posts').doc('p2').set({ tags: ['x', 'y'] });

    await db.collection('searches').doc('s1').set({ tag: 'b' });
    await db.collection('searches').doc('s2').set({ tag: 'z' });

    const p = projection({
      id: 'test',
      from: { p: collection('posts'), s: collection('searches') },
      select: { pTags: 'p.tags', sTag: 's.tag' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.NestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 'p.tags',
      right: 's.tag',
      operation: 'array-contains',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(1);
    expect(results).toContainEqual({ pTags: ['a', 'b', 'c'], sTag: 'b' });
  });

  it('should support array-contains-any operation', async () => {
    await db.collection('items').doc('i1').set({ tags: ['a', 'b'] });
    await db.collection('items').doc('i2').set({ tags: ['x', 'y'] });

    await db.collection('filters').doc('f1').set({ options: ['b', 'c'] });
    await db.collection('filters').doc('f2').set({ options: ['z'] });

    const p = projection({
      id: 'test',
      from: { i: collection('items'), f: collection('filters') },
      select: { iTags: 'i.tags', fOptions: 'f.options' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.NestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 'i.tags',
      right: 'f.options',
      operation: 'array-contains-any',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ iTags: ['a', 'b'], fOptions: ['b', 'c'] });
  });

  it('should support in operation', async () => {
    await db.collection('tasks').doc('t1').set({ status: 'active' });
    await db.collection('tasks').doc('t2').set({ status: 'pending' });
    await db.collection('tasks').doc('t3').set({ status: 'archived' });

    await db.collection('rules').doc('r1').set({ allowed: ['active', 'pending'] });

    const p = projection({
      id: 'test',
      from: { t: collection('tasks'), r: collection('rules') },
      select: { tStatus: 't.status', rAllowed: 'r.allowed' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.NestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 't.status',
      right: 'r.allowed',
      operation: 'in',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({ tStatus: 'active', rAllowed: ['active', 'pending'] });
    expect(results).toContainEqual({ tStatus: 'pending', rAllowed: ['active', 'pending'] });
  });

  it('should join on nested field `price.currency`', async () => {
    await db.collection('products').doc('prod1').set({ price: { value: 20, currency: 'USD' } });
    await db.collection('products').doc('prod2').set({ price: { value: 15, currency: 'EUR' } });

    await db.collection('currencyRates').doc('USD').set({ code: 'USD', rate: 1.0 });
    await db.collection('currencyRates').doc('EUR').set({ code: 'EUR', rate: 0.9 });

    const p = projection({
      id: 'test',
      from: { p: collection('products'), r: collection('currencyRates') },
      select: { pPrice: 'p.price', rRate: 'r.rate' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.NestedLoop;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 'p.price.currency',
      right: 'r.code',
      operation: '==',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toStrictEqual([{
      pPrice: { value: 20, currency: 'USD' },
      rRate: 1.0,
    }, {
      pPrice: { value: 15, currency: 'EUR' },
      rRate: 0.9,
    }]);
  });

  it('should join using complex AND/OR conditions', async () => {
    await db.collection('users').doc('1').set({ id: 1, role: 'admin', active: true });
    await db.collection('users').doc('2').set({ id: 2, role: 'user', active: true });
    await db.collection('users').doc('3').set({ id: 3, role: 'user', active: false });

    await db.collection('logs').doc('l1').set({ userId: 1, action: 'login' });
    await db.collection('logs').doc('l2').set({ userId: 2, action: 'logout' });
    await db.collection('logs').doc('l3').set({ userId: 3, action: 'login' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), l: collection('logs') },
      select: { uId: 'u.id', lAction: 'l.action' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.NestedLoop;

    // Join users and logs where:
    // (u.id == l.userId) AND (u.role == l.action)

    // Let's adjust data to make it match
    await db.collection('users').doc('4').set({ id: 4, role: 'login', active: true });
    await db.collection('logs').doc('l4').set({ userId: 4, action: 'login' });

    joinNode.condition = {
      type: 'AND',
      conditions: [
        {
          type: 'COMPARISON',
          left: 'u.id',
          right: 'l.userId',
          operation: '==',
        },
        {
          type: 'COMPARISON',
          left: 'u.role',
          right: 'l.action',
          operation: '==',
        },
      ],
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ uId: 4, lAction: 'login' });
  });
});
