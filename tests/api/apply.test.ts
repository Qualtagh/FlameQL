import { add, apply, collection, concatenate, divide, eq, field, literal, lowercase, multiply, projection, subtract, uppercase } from '../../src/api/api';
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
    const result = evaluate(expr, row, {});
    expect(result).toBe('mixedcase');
  });

  it('deeply evaluates nested array inputs at runtime', () => {
    const expr = apply(
      [[literal('/users/'), field('u.userId')], [literal('/orders/'), field('u.orderId')]],
      ([userParts, orderParts]) => (userParts as any[]).join('') + (orderParts as any[]).join('')
    );
    const row = { u: { userId: 'abc', orderId: 'def' } };
    const result = evaluate(expr, row, {});
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

describe('convenience functions', () => {
  describe('string functions', () => {
    it('concatenates strings', () => {
      const expr = concatenate([literal('hello'), literal(' '), literal('world')]);
      const result = evaluate(expr, {}, {});
      expect(result).toBe('hello world');
    });

    it('converts to lowercase', () => {
      const expr = lowercase(literal('HELLO WORLD'));
      const result = evaluate(expr, {}, {});
      expect(result).toBe('hello world');
    });

    it('converts to uppercase', () => {
      const expr = uppercase(literal('hello world'));
      const result = evaluate(expr, {}, {});
      expect(result).toBe('HELLO WORLD');
    });

    it('folds string functions over literals during planning', () => {
      const p = projection({
        id: 'string-functions-fold',
        from: { u: collection('users') },
        select: {
          concat: concatenate([literal('prefix_'), field('u.id')]),
          lower: lowercase(literal('MIXED')),
          upper: uppercase(literal('mixed')),
        },
      });

      const planner = new Planner();
      const plan = planner.plan(p);

      expect(plan.type).toBe(NodeType.PROJECT);
      const project = plan as ProjectNode;
      expect(project.fields.concat.kind).toBe('FunctionExpression'); // Can't fold with field
      expect(project.fields.lower.kind).toBe('Literal');
      expect((project.fields.lower as Literal).value).toBe('mixed');
      expect(project.fields.upper.kind).toBe('Literal');
      expect((project.fields.upper as Literal).value).toBe('MIXED');
    });
  });

  describe('math functions', () => {
    it('adds numbers', () => {
      const expr = add(literal(5), literal(3));
      const result = evaluate(expr, {}, {});
      expect(result).toBe(8);
    });

    it('subtracts numbers', () => {
      const expr = subtract(literal(10), literal(4));
      const result = evaluate(expr, {}, {});
      expect(result).toBe(6);
    });

    it('multiplies numbers', () => {
      const expr = multiply(literal(6), literal(7));
      const result = evaluate(expr, {}, {});
      expect(result).toBe(42);
    });

    it('divides numbers', () => {
      const expr = divide(literal(15), literal(3));
      const result = evaluate(expr, {}, {});
      expect(result).toBe(5);
    });

    it('folds math functions over literals during planning', () => {
      const p = projection({
        id: 'math-functions-fold',
        from: { u: collection('users') },
        select: {
          sum: add(literal(2), literal(3)),
          diff: subtract(literal(8), literal(3)),
          product: multiply(literal(4), literal(5)),
          quotient: divide(literal(20), literal(4)),
        },
      });

      const planner = new Planner();
      const plan = planner.plan(p);

      expect(plan.type).toBe(NodeType.PROJECT);
      const project = plan as ProjectNode;
      expect(project.fields.sum.kind).toBe('Literal');
      expect((project.fields.sum as Literal).value).toBe(5);
      expect(project.fields.diff.kind).toBe('Literal');
      expect((project.fields.diff as Literal).value).toBe(5);
      expect(project.fields.product.kind).toBe('Literal');
      expect((project.fields.product as Literal).value).toBe(20);
      expect(project.fields.quotient.kind).toBe('Literal');
      expect((project.fields.quotient as Literal).value).toBe(5);
    });

    it('evaluates math functions with fields at runtime', () => {
      const expr = add(field('u.score'), literal(10));
      const row = { u: { score: 15 } };
      const result = evaluate(expr, row, {});
      expect(result).toBe(25);
    });
  });
});
