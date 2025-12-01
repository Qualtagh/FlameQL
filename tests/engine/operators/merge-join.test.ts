import { collection } from '../../../src/api/collection';
import { JoinType } from '../../../src/api/hints';
import { projection } from '../../../src/api/projection';
import { JoinNode, ProjectNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { Planner } from '../../../src/engine/planner';
import { clearDatabase, db } from '../../setup';

describe('MergeJoinOperator (Integration)', () => {
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
      select: { uVal: 'u.val', oOther: 'o.other' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    // Force Merge Join and set condition
    joinNode.joinType = JoinType.Merge;
    joinNode.condition = {
      left: 'u.id',
      right: 'o.userId',
      operation: '==',
    };

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
      select: { uVal: 'u.val', oOther: 'o.other' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Merge;
    joinNode.condition = {
      left: 'u.id',
      right: 'o.userId',
      operation: '==',
    };

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
      select: { uVal: 'u.val', oOther: 'o.other' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Merge;
    joinNode.condition = {
      left: 'u.id',
      right: 'o.userId',
      operation: '==',
    };

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
      select: { uVal: 'u.val', oOther: 'o.other' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Merge;
    joinNode.condition = {
      left: 'u.id',
      right: 'o.userId',
      operation: '==',
    };

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
      select: { uVal: 'u.val', oOther: 'o.other' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Merge;
    joinNode.condition = {
      left: 'u.id',
      right: 'o.userId',
      operation: '==',
    };

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
      select: { pName: 'p.name', iQty: 'i.qty' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Merge;
    joinNode.condition = {
      left: 'p.code',
      right: 'i.productCode',
      operation: '==',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ pName: 'Widget', iQty: 10 });
  });

  it('should throw error for non-equality operations', async () => {
    const p = projection({
      id: 'test',
      from: { u: collection('users'), o: collection('orders') },
      select: { uVal: 'u.val', oOther: 'o.other' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Merge;
    joinNode.condition = {
      left: 'u.id',
      right: 'o.userId',
      operation: '>',
    };

    const executor = new Executor(db);

    await expect(executor.execute(plan)).rejects.toThrow(
      'MergeJoin strategy requires equality operation (==)'
    );
  });
});
