import { apply, collection, eq, field, literal, projection } from '../../src/api/api';
import { Literal } from '../../src/api/expression';
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
    expect((valExpr as Literal).value).toBe('hello');
  });

  it('folds nested literal array inputs during planning', () => {
    const p = projection({
      id: 'apply-fold-nested-literals',
      from: { u: collection('users') },
      select: {
        path: apply(
          [[literal('Users'), literal('Orders')], literal('/')],
          ([segments, delimiter]) => (segments as any[]).join(delimiter as string)
        ),
      },
    });

    const planner = new Planner();
    const plan = planner.plan(p);

    expect(plan.type).toBe(NodeType.PROJECT);
    const project = plan as ProjectNode;
    const pathExpr = project.fields.path;
    expect(pathExpr.kind).toBe('Literal');
    expect((pathExpr as Literal).value).toBe('Users/Orders');
  });

  it('evaluates function expressions at runtime for non-literals', () => {
    const expr = apply(field('u.name'), (s: string) => s.toLowerCase());
    const row = { u: { name: 'MixedCase' } };
    const result = evaluate(expr, row);
    expect(result).toBe('mixedcase');
  });

  it('deeply evaluates nested array inputs at runtime', () => {
    const expr = apply(
      [[literal('/users/'), field('u.userId')], [literal('/orders/'), field('u.orderId')]],
      ([userParts, orderParts]) => (userParts as any[]).join('') + (orderParts as any[]).join('')
    );
    const row = { u: { userId: 'abc', orderId: 'def' } };
    const result = evaluate(expr, row);
    expect(result).toBe('/users/abc/orders/def');
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
