import { collection, field, literal, projection } from '../../src/api/api';
import { JoinStrategy, PredicateMode, PredicateOrMode } from '../../src/api/hints';
import { FilterNode, JoinNode, NodeType, ProjectNode, ScanNode, UnionDistinctStrategy, UnionNode } from '../../src/engine/ast';
import { Planner } from '../../src/engine/planner';

describe('Planner disjunction handling', () => {
  test('PredicateOrMode.Union: single-source OR becomes UNION of conjunction plans', () => {
    const p = projection({
      id: 'single-source-or',
      from: { u: collection('users') },
      where: {
        type: 'OR',
        conditions: [
          { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
          { type: 'COMPARISON', left: field('u.age'), operation: '>', right: literal(30) },
        ],
      },
      select: { id: field('u.#id') },
      hints: { predicateOrMode: PredicateOrMode.Union },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.source.type).toBe(NodeType.UNION);
    const union = plan.source as UnionNode;
    expect(union.distinct).toBe(UnionDistinctStrategy.DocPath);
    expect(union.inputs).toHaveLength(2);

    for (const input of union.inputs) {
      expect(input.type).toBe(NodeType.SCAN);
      const scan = input as ScanNode;
      expect(scan.collectionPath).toBe('users');
      expect(scan.constraints).toHaveLength(1);
      expect(scan.constraints[0].field.path.join('.')).toBe('age');
    }
  });

  test('PredicateOrMode.SingleScan: single-source OR becomes FILTER over a single scan', () => {
    const p = projection({
      id: 'single-source-or-single-scan',
      from: { u: collection('users') },
      where: {
        type: 'OR',
        conditions: [
          { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
          { type: 'COMPARISON', left: field('u.age'), operation: '>', right: literal(30) },
        ],
      },
      select: { id: field('u.#id') },
      hints: { predicateOrMode: PredicateOrMode.SingleScan },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.source.type).toBe(NodeType.FILTER);
    const filter = plan.source as FilterNode;
    expect(filter.source.type).toBe(NodeType.SCAN);
  });

  test('PredicateOrMode.Union: multi-source DNF becomes UNION of conjunction plans (duplicate joins allowed)', () => {
    const p = projection({
      id: 'multi-source-or',
      from: { u: collection('users'), o: collection('orders') },
      where: {
        type: 'OR',
        conditions: [
          {
            type: 'AND',
            conditions: [
              { type: 'COMPARISON', left: field('u.#id'), operation: '==', right: field('o.userId') },
              { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
              { type: 'COMPARISON', left: field('o.total'), operation: '==', right: literal(100) },
            ],
          },
          {
            type: 'AND',
            conditions: [
              { type: 'COMPARISON', left: field('u.#id'), operation: '==', right: field('o.userId') },
              { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(30) },
              { type: 'COMPARISON', left: field('o.total'), operation: '==', right: literal(200) },
            ],
          },
        ],
      },
      select: { uId: field('u.#id'), oTotal: field('o.total') },
      hints: { join: JoinStrategy.Auto, predicateOrMode: PredicateOrMode.Union },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.source.type).toBe(NodeType.UNION);
    const union = plan.source as UnionNode;
    expect(union.distinct).toBe(UnionDistinctStrategy.DocPath);
    expect(union.inputs).toHaveLength(2);

    for (const input of union.inputs) {
      expect(input.type).toBe(NodeType.JOIN);
      const join = input as JoinNode;
      // Join predicate should remain hash-joinable
      expect(join.joinType).toBe(JoinStrategy.Hash);

      expect(join.left.type).toBe(NodeType.SCAN);
      expect(join.right.type).toBe(NodeType.SCAN);

      const leftScan = join.left as ScanNode;
      const rightScan = join.right as ScanNode;

      // Ensure scan-level constraints were pushed down for both sources (age and total)
      expect(leftScan.constraints.length + rightScan.constraints.length).toBeGreaterThanOrEqual(2);
      const constraintFields = [
        ...leftScan.constraints.map(c => c.field.path.join('.')),
        ...rightScan.constraints.map(c => c.field.path.join('.')),
      ];
      expect(constraintFields).toContain('age');
      expect(constraintFields).toContain('total');
    }
  });

  test('PredicateOrMode.SingleScan: factors out global common join predicate and adds residual OR filter', () => {
    const p = projection({
      id: 'common-factor-single-scan',
      from: { u: collection('users'), o: collection('orders') },
      where: {
        type: 'OR',
        conditions: [
          {
            type: 'AND',
            conditions: [
              { type: 'COMPARISON', left: field('u.#id'), operation: '==', right: field('o.userId') },
              { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
            ],
          },
          {
            type: 'AND',
            conditions: [
              { type: 'COMPARISON', left: field('u.#id'), operation: '==', right: field('o.userId') },
              { type: 'COMPARISON', left: field('u.age'), operation: '>', right: literal(30) },
            ],
          },
        ],
      },
      select: { uId: field('u.#id') },
      hints: { predicateOrMode: PredicateOrMode.SingleScan },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.source.type).toBe(NodeType.FILTER);
    const filter = plan.source as FilterNode;
    expect(filter.source.type).toBe(NodeType.JOIN);
    expect((filter.source as JoinNode).joinType).toBe(JoinStrategy.Hash);
  });

  test('PredicateMode.Respect: does not rewrite OR; join falls back to NestedLoop (OR join condition)', () => {
    const p = projection({
      id: 'respect-mode',
      from: { u: collection('users'), o: collection('orders') },
      where: {
        type: 'OR',
        conditions: [
          {
            type: 'AND',
            conditions: [
              { type: 'COMPARISON', left: field('u.#id'), operation: '==', right: field('o.userId') },
              { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
            ],
          },
          {
            type: 'AND',
            conditions: [
              { type: 'COMPARISON', left: field('u.#id'), operation: '==', right: field('o.userId') },
              { type: 'COMPARISON', left: field('u.age'), operation: '>', right: literal(30) },
            ],
          },
        ],
      },
      select: { uId: field('u.#id') },
      hints: { predicateMode: PredicateMode.Respect },
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.source.type).toBe(NodeType.JOIN);
    expect((plan.source as JoinNode).joinType).toBe(JoinStrategy.NestedLoop);
  });
});
