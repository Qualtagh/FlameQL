import { collection, JoinType, projection } from '../../src/api/api';
import { JoinNode, NodeType, ProjectNode, ScanNode } from '../../src/engine/ast';
import { Planner } from '../../src/engine/planner';

describe('Planner', () => {
  test('should plan a simple scan', () => {
    const p = projection({
      id: 'test',
      from: { j: collection('jobs') },
      select: { id: 'j.#id' },
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
      select: { id: 'j.#id' },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.type).toBe(NodeType.PROJECT);
    expect(plan.source.type).toBe(NodeType.JOIN);
    expect((plan.source as JoinNode).joinType).toBe(JoinType.NestedLoop);
  });

  test('should respect join hints', () => {
    const p = projection({
      id: 'test',
      from: { j: collection('jobs'), s: collection('shifts') },
      select: { id: 'j.#id' },
      hints: { joinType: JoinType.Hash },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect((plan.source as JoinNode).joinType).toBe(JoinType.Hash);
  });
});
