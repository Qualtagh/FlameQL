import { collection, field, param, projection } from '../../../src/api/api';
import { Field } from '../../../src/api/expression';
import { NodeType, ProjectNode, ScanNode } from '../../../src/engine/ast';
import { Executor } from '../../../src/engine/executor';
import { Planner } from '../../../src/engine/planner';
import { clearDatabase, db } from '../../setup';

describe('generic orderBy', () => {
  beforeEach(async () => {
    await clearDatabase();
    await db.collection('items').doc('1').set({ price: 10, name: 'A' });
    await db.collection('items').doc('2').set({ price: 20, name: 'B' });
    await db.collection('items').doc('3').set({ price: 5, name: 'C' });
  });

  test('should sort locally by function expression', async () => {
    const p = projection({
      id: 'test_func_sort',
      from: { t: collection('items') },
      select: { price: field('t.price') },
      orderBy: [
        {
          field: {
            kind: 'FunctionExpression',
            fn: ([price, direction]: any[]) => price * direction,
            input: [field('t.price'), param('direction')],
          } as any,
          direction: 'asc',
        },
      ],
    });

    const planner = new Planner();
    const plan = planner.plan(p);
    const executor = new Executor(db);

    // Test direction = 1 (ASC: 5, 10, 20)
    const res1 = await executor.execute(plan, { direction: 1 });
    expect(res1.map(r => r.price)).toEqual([5, 10, 20]);

    // Test direction = -1 (DESC: 20, 10, 5)
    // Note: We need to recreate executor or just rerun execute?
    // Plan is reusable.
    const res2 = await executor.execute(plan, { direction: -1 });
    expect(res2.map(r => r.price)).toEqual([20, 10, 5]);
  });

  test('should push down simple field sort + local sort should work (or double sort)', async () => {
    // This test ensures that if we use a simple field, it still works.
    // Ideally it pushes down to Firestore (ScanNode has orderBy).

    // We can check if it works end-to-end first.

    const p = projection({
      id: 'test_field_sort',
      from: { t: collection('items') },
      select: { price: field('t.price') },
      orderBy: [{ field: field('t.price'), direction: 'desc' }],
    });

    const planner = new Planner();
    const plan = planner.plan(p);
    expect(plan.type).toBe(NodeType.PROJECT);
    const project = plan as ProjectNode;
    expect(project.source.type).toBe(NodeType.SCAN);
    const scan = project.source as ScanNode;
    expect((scan.orderBy?.[0].field as Field).path.join('.')).toBe('price');
    expect(scan.orderBy?.[0].direction).toBe('desc');
    const executor = new Executor(db);

    const res = await executor.execute(plan, {});
    expect(res.map(r => r.price)).toEqual([20, 10, 5]);
  });
});
