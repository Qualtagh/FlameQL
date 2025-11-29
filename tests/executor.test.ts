import { collection } from '../src/api/collection';
import { projection } from '../src/api/projection';
import { JoinNode, ProjectNode } from '../src/engine/ast';
import { Executor } from '../src/engine/executor';
import { Planner } from '../src/engine/planner';
import { clearDatabase, db } from './setup';

describe('Executor', () => {
  beforeEach(async () => {
    await clearDatabase();
    // Seed data
    await db.collection('jobs').doc('job1').set({ title: 'Software Engineer' });
    await db.collection('jobs').doc('job2').set({ title: 'Product Manager' });
  });

  test('should execute a simple scan', async () => {
    const p = projection({
      id: 'test',
      from: { j: collection('jobs') },
      select: { id: 'j.id', title: 'j.title' },
    });

    const planner = new Planner();
    const plan = planner.plan(p);
    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toStrictEqual([
      { id: 'job1', title: 'Software Engineer' },
      { id: 'job2', title: 'Product Manager' },
    ]);
  });

  test('should execute an aggregate', async () => {
    // Manually construct plan for aggregate since Planner doesn't fully support it yet
    const plan: any = {
      type: 'AGGREGATE',
      source: {
        type: 'SCAN',
        collectionPath: 'jobs',
        alias: 'j',
        constraints: [],
      },
      groupBy: [],
      aggregates: {},
    };

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results.length).toBe(1);
    expect(results[0].count).toBe(2);
  });

  test('should execute a join', async () => {
    // Seed shifts
    await db.collection('shifts').doc('shift1').set({ jobId: 'job1', date: '2023-01-01' });
    await db.collection('shifts').doc('shift2').set({ jobId: 'job1', date: '2023-01-02' });
    await db.collection('shifts').doc('shift3').set({ jobId: 'job2', date: '2023-01-03' });

    const p = projection({
      id: 'test',
      from: { j: collection('jobs'), s: collection('shifts') },
      select: { jobId: 'j.id', jobTitle: 'j.title', shiftDate: 's.date' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    // Manually set join condition for now since Planner doesn't extract it yet
    (plan.source as JoinNode).on = (left: any, right: any) => left['j'].id === right['s'].jobId;

    const executor = new Executor(db);
    const results = await executor.execute(plan);

    expect(results).toStrictEqual([
      { 'jobId': 'job1', 'jobTitle': 'Software Engineer', 'shiftDate': '2023-01-01' },
      { 'jobId': 'job1', 'jobTitle': 'Software Engineer', 'shiftDate': '2023-01-02' },
      { 'jobId': 'job2', 'jobTitle': 'Product Manager', 'shiftDate': '2023-01-03' },
    ]);
  });
});
