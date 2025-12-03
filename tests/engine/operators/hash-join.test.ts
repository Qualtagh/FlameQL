import { collection } from '../../../src/api/collection';
import { JoinType } from '../../../src/api/hints';
import { projection } from '../../../src/api/projection';
import { JoinNode, ProjectNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { Planner } from '../../../src/engine/planner';
import { clearDatabase, db } from '../../setup';

describe('HashJoinOperator', () => {
  beforeEach(async () => {
    await clearDatabase();
  });

  it('should join using HashJoinStrategy (equality condition)', async () => {
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

    // Force Hash Join and set condition
    joinNode.joinType = JoinType.Hash;
    joinNode.condition = {
      type: 'COMPARISON',
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

  it('should support array-contains with hash join', async () => {
    await db.collection('posts').doc('p1').set({ tags: ['a', 'b', 'c'] });
    await db.collection('posts').doc('p2').set({ tags: ['x', 'y'] });
    await db.collection('posts').doc('p3').set({ tags: ['b', 'd'] });

    await db.collection('searches').doc('s1').set({ tag: 'b' });
    await db.collection('searches').doc('s2').set({ tag: 'z' });
    await db.collection('searches').doc('s3').set({ tag: 'x' });

    const p = projection({
      id: 'test',
      from: { p: collection('posts'), s: collection('searches') },
      select: { pTags: 'p.tags', sTag: 's.tag' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Hash;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 'p.tags',
      right: 's.tag',
      operation: 'array-contains',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(3);
    expect(results).toContainEqual({ pTags: ['a', 'b', 'c'], sTag: 'b' });
    expect(results).toContainEqual({ pTags: ['x', 'y'], sTag: 'x' });
    expect(results).toContainEqual({ pTags: ['b', 'd'], sTag: 'b' });
  });

  it('should support array-contains-any with hash join', async () => {
    await db.collection('items').doc('i1').set({ tags: ['a', 'b'] });
    await db.collection('items').doc('i2').set({ tags: ['x', 'y'] });
    await db.collection('items').doc('i3').set({ tags: ['m', 'n'] });

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

    joinNode.joinType = JoinType.Hash;
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

  it('should support in with hash join', async () => {
    await db.collection('things').doc('t1').set({ tag: 'a' });
    await db.collection('things').doc('t2').set({ tag: 'b' });
    await db.collection('things').doc('t3').set({ tag: 'x' });

    await db.collection('filters2').doc('f1').set({ options: ['b', 'c'] });
    await db.collection('filters2').doc('f2').set({ options: ['z'] });

    const p = projection({
      id: 'test',
      from: { t: collection('things'), f: collection('filters2') },
      select: { tTag: 't.tag', fOptions: 'f.options' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;
    const joinNode = plan.source as JoinNode;

    joinNode.joinType = JoinType.Hash;
    joinNode.condition = {
      type: 'COMPARISON',
      left: 't.tag',
      right: 'f.options',
      operation: 'in',
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ tTag: 'b', fOptions: ['b', 'c'] });
  });
});
