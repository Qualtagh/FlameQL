import { collection, eq, field, JoinStrategy, projection } from '../../src/api/api';
import { JoinNode, NodeType, ProjectNode, ScanNode } from '../../src/engine/ast';
import { Planner } from '../../src/engine/planner';

describe('Planner', () => {
  test('should plan a simple scan', () => {
    const p = projection({
      id: 'test',
      from: { j: collection('jobs') },
      select: { id: field('j.#id') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.type).toBe(NodeType.PROJECT);
    expect(plan.source.type).toBe(NodeType.SCAN);
    expect((plan.source as ScanNode).collectionPath).toBe('jobs');
  });

  test('should plan a join with default nested loop', () => {
    const p = projection({
      id: 'test',
      from: { j: collection('jobs'), s: collection('shifts') },
      select: { id: field('j.#id') },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.type).toBe(NodeType.PROJECT);
    expect(plan.source.type).toBe(NodeType.JOIN);
    expect((plan.source as JoinNode).joinType).toBe(JoinStrategy.NestedLoop);
  });

  test('should respect join hints', () => {
    const p = projection({
      id: 'test',
      from: { j: collection('jobs'), s: collection('shifts') },
      select: { id: field('j.#id') },
      where: eq(field('j.#id'), field('s.jobId')),
      hints: { join: JoinStrategy.Hash },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect((plan.source as JoinNode).joinType).toBe(JoinStrategy.Hash);
  });

  test('applies sort and limit', () => {
    const p = projection({
      id: 'sorted',
      from: { j: collection('jobs') },
      select: { id: field('j.#id') },
      orderBy: ['j.title'],
      limit: 5,
      offset: 2,
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.type).toBe(NodeType.PROJECT);
    const limitNode = plan.source as any;
    expect(limitNode.type).toBe(NodeType.LIMIT);
    expect(limitNode.limit).toBe(5);
    expect(limitNode.offset).toBe(2);
    expect(limitNode.source.type).toBe(NodeType.SORT);
  });
});
