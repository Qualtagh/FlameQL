import { and, apply, collection, constant, eq, field, literal, lowercase, ne, or, param, projection } from '../../src/api/api';
import { FunctionExpression, Param } from '../../src/api/expression';
import { ProjectNode, ScanNode } from '../../src/engine/ast';
import { Executor } from '../../src/engine/executor';
import { Planner } from '../../src/engine/planner';
import { simplifyPredicate } from '../../src/engine/utils/predicate-utils';
import { clearDatabase, db } from '../setup';

describe('Parameterized Plan Execution', () => {
  beforeEach(async () => {
    await clearDatabase();
  });

  it('executes the same plan with different parameters', async () => {
    // Seed data
    await db.collection('users').doc('u1').set({ id: 'u1', name: 'Alice' });
    await db.collection('users').doc('u2').set({ id: 'u2', name: 'Bob' });
    await db.collection('users').doc('u3').set({ id: 'u3', name: 'Charlie' });

    const p = projection({
      id: 'param-reuse',
      from: { u: collection('users') },
      select: { userName: field('u.name') },
      where: eq(field('u.id'), param('userId')),
    });

    // Build plan once - params remain unresolved
    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    // Verify predicate pushdown: should have constraints on ScanNode, NOT a FilterNode
    const scanNode = plan.source as ScanNode;
    // Verify Param is in constraints (pushed down to Firestore)
    expect(scanNode.constraints).toHaveLength(1);
    expect(scanNode.constraints[0].field.path).toEqual(['id']);
    const value = scanNode.constraints[0].value as Param;
    expect(value.kind).toBe('Param');
    expect(value.name).toBe('userId');

    // Execute with different params
    const executor = new Executor(db);
    const result1 = await executor.execute(plan, { userId: 'u1' });
    const result2 = await executor.execute(plan, { userId: 'u2' });

    expect(result1).toEqual([{ userName: 'Alice' }]);
    expect(result2).toEqual([{ userName: 'Bob' }]);
  });

  it('throws error when parameter is missing during execution', async () => {
    await db.collection('users').doc('u1').set({ id: 'u1', name: 'Alice' });

    const p = projection({
      id: 'param-missing',
      from: { u: collection('users') },
      select: { userName: field('u.name') },
      where: eq(field('u.id'), param('userId')),
    });

    const planner = new Planner();
    const plan = planner.plan(p);  // Should NOT throw
    const executor = new Executor(db);

    // Missing parameter should throw at execution time
    await expect(executor.execute(plan, {})).rejects.toThrow('Parameter "userId" was not provided.');
  });

  describe('Param predicate simplification', () => {
    it('simplifies == P && != P to false (contradiction)', () => {
      const contradiction = and([
        eq(field('u.id'), param('p')),
        ne(field('u.id'), param('p')),
      ]);
      expect(simplifyPredicate(contradiction)).toStrictEqual(constant(false));
    });

    it('simplifies == P || != P to true (tautology)', () => {
      const tautology = or([
        eq(field('u.id'), param('p')),
        ne(field('u.id'), param('p')),
      ]);
      expect(simplifyPredicate(tautology)).toStrictEqual(constant(true));
    });

    it('does not simplify comparisons between Param and Literal', () => {
      // Cannot simplify: field == P && field != 3 (P value unknown)
      const mixedAnd = and([
        eq(field('u.id'), param('p')),
        ne(field('u.id'), literal(3)),
      ]);
      const result = simplifyPredicate(mixedAnd);
      // Should remain as AND, not simplified to false
      expect(result.type).toBe('AND');
    });

    it('treats same Params as equal in expressions', () => {
      // Both predicates compare to the same param - should be treated as duplicate
      const duplicateParams = and([
        eq(field('u.id'), param('p')),
        eq(field('u.id'), param('p')),
      ]);
      const result = simplifyPredicate(duplicateParams);
      // Should simplify to single predicate
      expect(result).toStrictEqual(eq(field('u.id'), param('p')));
    });

    it('treats different Params as different expressions', () => {
      const differentParams = and([
        eq(field('u.id'), param('p1')),
        eq(field('u.id'), param('p2')),
      ]);
      const result = simplifyPredicate(differentParams);
      // Should remain as AND since p1 and p2 are different
      expect(result.type).toBe('AND');
    });
  });

  it('resolves function expressions that depend on parameters', async () => {
    // Seed data
    await db.collection('items').doc('i1').set({ name: 'alpha', prefix: 'A' });
    await db.collection('items').doc('i2').set({ name: 'beta', prefix: 'B' });
    await db.collection('items').doc('i3').set({ name: 'gamma', prefix: 'C' });

    const p = projection({
      id: 'fn-over-param',
      from: { i: collection('items') },
      select: {
        name: field('i.name'),
        // Function expression: concatenate a param with a field
        combined: apply([param('prefix'), field('i.name')], ([prefix, name]) => `${prefix}-${name}`),
      },
      // Filter using a function expression over param
      where: eq(
        apply([param('matchPrefix'), field('i.prefix')], ([matchPrefix, prefix]) => matchPrefix === prefix),
        literal(true)
      ),
    });

    const planner = new Planner();
    const plan = planner.plan(p);
    const executor = new Executor(db);

    // Execute with different parameters
    const result1 = await executor.execute(plan, { prefix: 'X', matchPrefix: 'A' });
    expect(result1).toEqual([{ name: 'alpha', combined: 'X-alpha' }]);

    const result2 = await executor.execute(plan, { prefix: 'Y', matchPrefix: 'B' });
    expect(result2).toEqual([{ name: 'beta', combined: 'Y-beta' }]);
  });

  it('pushes down function expressions over parameters to Scan constraints', async () => {
    await db.collection('items').doc('i1').set({ prefix: 'a' });
    await db.collection('items').doc('i2').set({ prefix: 'b' });

    const p = projection({
      id: 'fn-pushdown',
      from: { i: collection('items') },
      select: { prefix: field('i.prefix') },
      // eq(field, function(param)) should be pushed down
      where: eq(field('i.prefix'), lowercase(param('prefixParam'))),
    });

    const planner = new Planner();
    const plan = planner.plan(p) as ProjectNode;

    // Verify pushdown
    const scanNode = plan.source as ScanNode;
    expect(scanNode.type).toBe('SCAN');
    expect(scanNode.constraints).toHaveLength(1);
    expect(scanNode.constraints[0].field.path).toEqual(['prefix']);
    const val = scanNode.constraints[0].value as FunctionExpression;
    expect(val.kind).toBe('FunctionExpression');

    const executor = new Executor(db);
    const results = await executor.execute(plan, { prefixParam: 'A' });

    expect(results).toEqual([{ prefix: 'a' }]);
  });
});
