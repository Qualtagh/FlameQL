import { apply, collection, eq, field, literal, projection } from '../../src/api/api';
import { FilterNode, NodeType, ProjectNode, ScanNode } from '../../src/engine/ast';
import { evaluate } from '../../src/engine/evaluator';
import { Planner } from '../../src/engine/planner';

describe('apply expression', () => {
  it('folds function expressions over literals during planning', () => {
    const p = projection({
      id: 'apply-fold-literal',
      from: { u: collection('users') },
      select: {
        val: apply(literal('HELLO'), (s: string) => s.toLowerCase()),
      },
    });

    const planner = new Planner();
    const plan = planner.plan(p);

    expect(plan.type).toBe(NodeType.PROJECT);
    const project = plan as ProjectNode;
    const valExpr = project.fields.val;
    expect(valExpr.kind).toBe('Literal');
    expect((valExpr as any).value).toBe('hello');
  });

  it('evaluates function expressions at runtime for non-literals', () => {
    const expr = apply(field('u.name'), (s: string) => s.toLowerCase());
    const row = { u: { name: 'MixedCase' } };
    const result = evaluate(expr, row);
    expect(result).toBe('mixedcase');
  });

  it('avoids index constraints when function expressions appear in predicates', () => {
    const p = projection({
      id: 'apply-nonindexable',
      from: { u: collection('users') },
      where: eq(apply(field('u.name'), (s: string) => s.toLowerCase()), literal('alice')),
      select: { name: field('u.name') },
    });

    const planner = new Planner();
    const plan = planner.plan(p);
    expect(plan.type).toBe(NodeType.PROJECT);

    const project = plan as ProjectNode;
    const filter = project.source as FilterNode;
    expect(filter.type).toBe(NodeType.FILTER);

    const scan = filter.source as ScanNode;
    expect(scan.type).toBe(NodeType.SCAN);
    expect(scan.constraints).toHaveLength(0);
  });
});
