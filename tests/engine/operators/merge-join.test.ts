import { arrayContains, collection, eq, field, gt, gte, lt, lte, projection } from '../../../src/api/api';
import { JoinStrategy } from '../../../src/api/hints';
import { JoinNode, ProjectNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { Planner } from '../../../src/engine/planner';
import { clearDatabase, db } from '../../setup';

describe('MergeJoinOperator', () => {
  beforeEach(async () => {
    await clearDatabase();
  });

  it('should join using MergeJoinStrategy (equality condition)', async () => {
    // Seed data
    await db.collection('users').doc('1').set({ id: 1, val: 'a' });
    await db.collection('users').doc('2').set({ id: 2, val: 'b' });
    await db.collection('users').doc('3').set({ id: 1, val: 'c' }); // Duplicate key in left

    await db.collection('orders').doc('101').set({ userId: 1, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 3, other: 'y' });
    await db.collection('orders').doc('103').set({ userId: 1, other: 'z' }); // Duplicate key in right

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: field('u.val'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    // Force Merge Join and set condition
    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = eq(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(4);
    // u.id=1 (val='a') matches o.userId=1 (other='x', other='z') -> 2 rows
    // u.id=1 (val='c') matches o.userId=1 (other='x', other='z') -> 2 rows
    // Total 4 rows

    expect(results).toContainEqual({ uVal: 'a', oOther: 'x' });
    expect(results).toContainEqual({ uVal: 'a', oOther: 'z' });
    expect(results).toContainEqual({ uVal: 'c', oOther: 'x' });
    expect(results).toContainEqual({ uVal: 'c', oOther: 'z' });
  });

  it('should handle empty left collection', async () => {
    // Seed only right collection
    await db.collection('orders').doc('101').set({ userId: 1, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 2, other: 'y' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: field('u.val'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = eq(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(0);
  });

  it('should handle empty right collection', async () => {
    // Seed only left collection
    await db.collection('users').doc('1').set({ id: 1, val: 'a' });
    await db.collection('users').doc('2').set({ id: 2, val: 'b' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: field('u.val'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = eq(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(0);
  });

  it('should handle no matching values', async () => {
    // Seed data with no overlapping join keys
    await db.collection('users').doc('1').set({ id: 1, val: 'a' });
    await db.collection('users').doc('2').set({ id: 2, val: 'b' });

    await db.collection('orders').doc('101').set({ userId: 5, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 6, other: 'y' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: field('u.val'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = eq(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(0);
  });

  it('should handle single matching pair', async () => {
    // Seed data with exactly one match
    await db.collection('users').doc('1').set({ id: 1, val: 'a' });
    await db.collection('users').doc('2').set({ id: 2, val: 'b' });

    await db.collection('orders').doc('101').set({ userId: 1, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 3, other: 'y' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: field('u.val'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = eq(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ uVal: 'a', oOther: 'x' });
  });

  it('should correctly handle string join keys', async () => {
    // Test with string values
    await db.collection('products').doc('p1').set({ code: 'ABC', name: 'Widget' });
    await db.collection('products').doc('p2').set({ code: 'XYZ', name: 'Gadget' });

    await db.collection('inventory').doc('i1').set({ productCode: 'ABC', qty: 10 });
    await db.collection('inventory').doc('i2').set({ productCode: 'DEF', qty: 5 });

    const p = projection({
      id: 'test',
      from: { p: collection('products'), i: collection('inventory') },
      select: { pName: field('p.name'), iQty: field('i.qty') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = eq(field('p.code'), field('i.productCode'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ pName: 'Widget', iQty: 10 });
  });

  it('should throw error for unsupported operations', async () => {
    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: field('u.val'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = arrayContains(field('u.id'), field('o.userId'));

    const executor = new Executor(db);

    await expect(executor.execute(plan)).rejects.toThrow(
      'MergeJoin strategy requires comparison operation'
    );
  });

  it('should support < (less than) operator', async () => {
    // Seed data
    await db.collection('users').doc('1').set({ id: 1, val: 'a' });
    await db.collection('users').doc('2').set({ id: 3, val: 'b' });
    await db.collection('users').doc('3').set({ id: 5, val: 'c' });

    await db.collection('orders').doc('101').set({ userId: 2, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 4, other: 'y' });
    await db.collection('orders').doc('103').set({ userId: 6, other: 'z' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uId: field('u.id'), uVal: field('u.val'), oUserId: field('o.userId'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = lt(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    // u.id=1 < o.userId in [2,4,6] -> 3 matches
    // u.id=3 < o.userId in [4,6] -> 2 matches
    // u.id=5 < o.userId in [6] -> 1 match
    expect(results).toHaveLength(6);
    expect(results).toContainEqual({ uId: 1, uVal: 'a', oUserId: 2, oOther: 'x' });
    expect(results).toContainEqual({ uId: 1, uVal: 'a', oUserId: 4, oOther: 'y' });
    expect(results).toContainEqual({ uId: 1, uVal: 'a', oUserId: 6, oOther: 'z' });
    expect(results).toContainEqual({ uId: 3, uVal: 'b', oUserId: 4, oOther: 'y' });
    expect(results).toContainEqual({ uId: 3, uVal: 'b', oUserId: 6, oOther: 'z' });
    expect(results).toContainEqual({ uId: 5, uVal: 'c', oUserId: 6, oOther: 'z' });
  });

  it('should support <= (less than or equal) operator', async () => {
    // Seed data
    await db.collection('users').doc('1').set({ id: 2, val: 'a' });
    await db.collection('users').doc('2').set({ id: 4, val: 'b' });

    await db.collection('orders').doc('101').set({ userId: 2, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 3, other: 'y' });
    await db.collection('orders').doc('103').set({ userId: 4, other: 'z' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uId: field('u.id'), uVal: field('u.val'), oUserId: field('o.userId'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = lte(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    // u.id=2 <= o.userId in [2,3,4] -> 3 matches
    // u.id=4 <= o.userId in [4] -> 1 match
    expect(results).toHaveLength(4);
    expect(results).toContainEqual({ uId: 2, uVal: 'a', oUserId: 2, oOther: 'x' });
    expect(results).toContainEqual({ uId: 2, uVal: 'a', oUserId: 3, oOther: 'y' });
    expect(results).toContainEqual({ uId: 2, uVal: 'a', oUserId: 4, oOther: 'z' });
    expect(results).toContainEqual({ uId: 4, uVal: 'b', oUserId: 4, oOther: 'z' });
  });

  it('should support > (greater than) operator', async () => {
    // Seed data
    await db.collection('users').doc('1').set({ id: 3, val: 'a' });
    await db.collection('users').doc('2').set({ id: 5, val: 'b' });
    await db.collection('users').doc('3').set({ id: 7, val: 'c' });

    await db.collection('orders').doc('101').set({ userId: 2, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 4, other: 'y' });
    await db.collection('orders').doc('103').set({ userId: 6, other: 'z' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uId: field('u.id'), uVal: field('u.val'), oUserId: field('o.userId'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = gt(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    // u.id=3 > o.userId in [2] -> 1 match
    // u.id=5 > o.userId in [2,4] -> 2 matches
    // u.id=7 > o.userId in [2,4,6] -> 3 matches
    expect(results).toHaveLength(6);
    expect(results).toContainEqual({ uId: 3, uVal: 'a', oUserId: 2, oOther: 'x' });
    expect(results).toContainEqual({ uId: 5, uVal: 'b', oUserId: 2, oOther: 'x' });
    expect(results).toContainEqual({ uId: 5, uVal: 'b', oUserId: 4, oOther: 'y' });
    expect(results).toContainEqual({ uId: 7, uVal: 'c', oUserId: 2, oOther: 'x' });
    expect(results).toContainEqual({ uId: 7, uVal: 'c', oUserId: 4, oOther: 'y' });
    expect(results).toContainEqual({ uId: 7, uVal: 'c', oUserId: 6, oOther: 'z' });
  });

  it('should support >= (greater than or equal) operator', async () => {
    // Seed data
    await db.collection('users').doc('1').set({ id: 3, val: 'a' });
    await db.collection('users').doc('2').set({ id: 5, val: 'b' });

    await db.collection('orders').doc('101').set({ userId: 1, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 3, other: 'y' });
    await db.collection('orders').doc('103').set({ userId: 5, other: 'z' });

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uId: field('u.id'), uVal: field('u.val'), oUserId: field('o.userId'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = gte(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    // u.id=3 >= o.userId in [1,3] -> 2 matches
    // u.id=5 >= o.userId in [1,3,5] -> 3 matches
    expect(results).toHaveLength(5);
    expect(results).toContainEqual({ uId: 3, uVal: 'a', oUserId: 1, oOther: 'x' });
    expect(results).toContainEqual({ uId: 3, uVal: 'a', oUserId: 3, oOther: 'y' });
    expect(results).toContainEqual({ uId: 5, uVal: 'b', oUserId: 1, oOther: 'x' });
    expect(results).toContainEqual({ uId: 5, uVal: 'b', oUserId: 3, oOther: 'y' });
    expect(results).toContainEqual({ uId: 5, uVal: 'b', oUserId: 5, oOther: 'z' });
  });

  it('should handle duplicates with inequality operators', async () => {
    // Seed data with duplicates
    await db.collection('users').doc('1').set({ id: 3, val: 'a' });
    await db.collection('users').doc('2').set({ id: 3, val: 'b' }); // Duplicate id

    await db.collection('orders').doc('101').set({ userId: 2, other: 'x' });
    await db.collection('orders').doc('102').set({ userId: 2, other: 'y' }); // Duplicate userId

    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uId: field('u.id'), uVal: field('u.val'), oUserId: field('o.userId'), oOther: field('o.other') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinStrategy.Merge;
    joinNode.condition = gt(field('u.id'), field('o.userId'));

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    // Both u.id=3 rows match both o.userId=2 rows (3 > 2)
    // 2 left rows × 2 right rows = 4 results
    expect(results).toHaveLength(4);
    expect(results).toContainEqual({ uId: 3, uVal: 'a', oUserId: 2, oOther: 'x' });
    expect(results).toContainEqual({ uId: 3, uVal: 'a', oUserId: 2, oOther: 'y' });
    expect(results).toContainEqual({ uId: 3, uVal: 'b', oUserId: 2, oOther: 'x' });
    expect(results).toContainEqual({ uId: 3, uVal: 'b', oUserId: 2, oOther: 'y' });
  });
});
