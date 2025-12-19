import { collection, compare, field, like, literal, projection } from '../../src/api/api';
import { FilterNode, NodeType, ProjectNode } from '../../src/engine/ast';
import { evaluatePredicate } from '../../src/engine/evaluator';
import { Planner } from '../../src/engine/planner';

describe('like and custom predicates', () => {
  describe('compare()', () => {
    it('creates custom predicate with tag and metadata', () => {
      const pred = compare([literal('test')], (v) => v === 'test', {
        tag: 'test-tag',
        key: 'value',
      });

      expect(pred.type).toBe('CUSTOM');
      expect(pred.metadata).toEqual({ tag: 'test-tag', key: 'value' });
    });

    it('evaluates custom predicates at runtime', () => {
      const pred = compare([field('u.name'), literal('Alice')], ([name, expected]) => name === expected);
      const row = { u: { name: 'Alice' } };

      expect(evaluatePredicate(pred, row)).toBe(true);

      const row2 = { u: { name: 'Bob' } };
      expect(evaluatePredicate(pred, row2)).toBe(false);
    });
  });

  describe('like()', () => {
    it('matches exact strings', () => {
      const pred = like(field('u.name'), literal('Alice'));
      const row = { u: { name: 'Alice' } };
      expect(evaluatePredicate(pred, row)).toBe(true);

      const row2 = { u: { name: 'Bob' } };
      expect(evaluatePredicate(pred, row2)).toBe(false);
    });

    it('matches prefix patterns with %', () => {
      const pred = like(field('u.name'), literal('Ali%'));
      expect(evaluatePredicate(pred, { u: { name: 'Alice' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'Alison' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'Bob' } })).toBe(false);
    });

    it('matches suffix patterns', () => {
      const pred = like(field('u.name'), literal('%ice'));
      expect(evaluatePredicate(pred, { u: { name: 'Alice' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'ice' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'Bob' } })).toBe(false);
    });

    it('matches contains patterns', () => {
      const pred = like(field('u.name'), literal('%li%'));
      expect(evaluatePredicate(pred, { u: { name: 'Alice' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'Olivia' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'Bob' } })).toBe(false);
    });

    it('matches single character wildcards with _', () => {
      const pred = like(field('u.name'), literal('A_i%'));
      expect(evaluatePredicate(pred, { u: { name: 'Alice' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'A1ice' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'A12ice' } })).toBe(false);
    });

    it('handles empty patterns', () => {
      const pred = like(field('u.name'), literal(''));
      expect(evaluatePredicate(pred, { u: { name: '' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'Alice' } })).toBe(false);
    });

    it('escapes special regex characters', () => {
      const pred = like(field('u.name'), literal('Alice.+'));
      expect(evaluatePredicate(pred, { u: { name: 'Alice.+' } })).toBe(true);
      expect(evaluatePredicate(pred, { u: { name: 'AliceX+' } })).toBe(false);
    });

    it('routes LIKE predicates to residual filters', () => {
      const p = projection({
        id: 'like-residual',
        from: { u: collection('users') },
        where: like(field('u.name'), literal('%Alice')), // Pattern without prefix won't be optimized
        select: { name: field('u.name') },
      });

      const planner = new Planner();
      const plan = planner.plan(p);

      expect(plan.type).toBe(NodeType.PROJECT);
      const project = plan as ProjectNode;
      expect(project.source.type).toBe(NodeType.FILTER);
      const filter = project.source as FilterNode;
      expect(filter.predicate.type).toBe('CUSTOM');
      expect((filter.predicate as any).metadata.name).toBe('like');
    });
  });

  describe('LIKE prefix optimization', () => {
    it('optimizes prefix patterns to range queries', () => {
      const p = projection({
        id: 'like-optimization',
        from: { u: collection('users') },
        where: like(field('u.name'), literal('Alice%')),
        select: { name: field('u.name') },
      });

      const planner = new Planner();
      const plan = planner.plan(p);

      expect(plan.type).toBe(NodeType.PROJECT);
      const project = plan as ProjectNode;
      expect(project.source.type).toBe(NodeType.FILTER);
      const filter = project.source as FilterNode;

      // Should be AND of bounds + optimized LIKE
      expect(filter.predicate.type).toBe('AND');
      expect((filter.predicate as any).conditions).toHaveLength(3);

      const [gtePred, ltPred, likePred] = (filter.predicate as any).conditions;
      expect(gtePred.type).toBe('COMPARISON');
      expect(gtePred.operation).toBe('>=');
      expect(ltPred.type).toBe('COMPARISON');
      expect(ltPred.operation).toBe('<');
      expect(likePred.type).toBe('CUSTOM');
      expect(likePred.metadata?.optimized).toBe(true);
    });

    it('does not optimize patterns without prefix', () => {
      const p = projection({
        id: 'like-no-optimization',
        from: { u: collection('users') },
        where: like(field('u.name'), literal('%Alice')),
        select: { name: field('u.name') },
      });

      const planner = new Planner();
      const plan = planner.plan(p);

      expect(plan.type).toBe(NodeType.PROJECT);
      const project = plan as ProjectNode;
      expect(project.source.type).toBe(NodeType.FILTER);
      const filter = project.source as FilterNode;

      // Should remain as single LIKE predicate
      expect(filter.predicate.type).toBe('CUSTOM');
      expect((filter.predicate as any).metadata.name).toBe('like');
      expect((filter.predicate as any).metadata.optimized).toBeUndefined();
    });

    it('handles unicode characters in bounds calculation', () => {
      const pred = like(field('u.name'), literal('Zürich%'));
      const row = { u: { name: 'Zürich Hauptbahnhof' } };
      expect(evaluatePredicate(pred, row)).toBe(true);
    });

    it('prevents infinite loops by marking optimized predicates', () => {
      // Test that optimization doesn't run twice on the same predicate
      const optimizedPred = like(field('u.name'), literal('Alice%'));
      (optimizedPred as any).metadata = { optimized: true };

      const p = projection({
        id: 'like-no-double-optimization',
        from: { u: collection('users') },
        where: optimizedPred,
        select: { name: field('u.name') },
      });

      const planner = new Planner();
      const plan = planner.plan(p);

      expect(plan.type).toBe(NodeType.PROJECT);
      const project = plan as ProjectNode;
      expect(project.source.type).toBe(NodeType.FILTER);
      const filter = project.source as FilterNode;

      // Should remain as single optimized LIKE predicate
      expect(filter.predicate.type).toBe('CUSTOM');
      expect((filter.predicate as any).metadata?.optimized).toBe(true);
    });
  });
});
