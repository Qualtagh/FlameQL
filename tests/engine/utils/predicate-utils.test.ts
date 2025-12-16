import { arrayContains, arrayContainsAny, field, inList, literal, notInList, param } from '../../../src/api/api';
import { Predicate } from '../../../src/api/expression';
import { simplifyPredicate, toDNF } from '../../../src/engine/utils/predicate-utils';

const cmp = (left: any, right: any, operation: any = '==') => ({
  type: 'COMPARISON',
  left: field(`t.${left}`),
  right: literal(right),
  operation,
} as const);

describe('Predicate Utilities', () => {
  describe('simplifyPredicate', () => {
    it('normalizes comparison to keep Field on the left', () => {
      const predicate: Predicate = {
        type: 'COMPARISON',
        operation: '==',
        left: literal(5),
        right: field('u.id'),
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'COMPARISON',
        operation: '==',
        left: field('u.id'),
        right: literal(5),
      });
    });

    it('folds literal-to-literal comparisons (including lists) into CONSTANT', () => {
      expect(simplifyPredicate({ type: 'COMPARISON', operation: '==', left: literal(1), right: literal(1) }))
        .toStrictEqual({ type: 'CONSTANT', value: true });
      expect(simplifyPredicate({ type: 'COMPARISON', operation: '!=', left: literal(1), right: literal(2) }))
        .toStrictEqual({ type: 'CONSTANT', value: true });
      expect(simplifyPredicate(inList(literal(2), [literal(1), literal(2)])))
        .toStrictEqual({ type: 'CONSTANT', value: true });
      expect(simplifyPredicate(inList(literal(3), [literal(1), literal(2)])))
        .toStrictEqual({ type: 'CONSTANT', value: false });
    });

    it('splits inList with a Field inside the list into OR of EQ', () => {
      const predicate = inList(field('a.id'), [literal(1), param('p'), field('b.id')]);

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'OR',
        conditions: [
          { type: 'COMPARISON', operation: 'in', left: field('a.id'), right: [literal(1), param('p')] },
          { type: 'COMPARISON', operation: '==', left: field('a.id'), right: field('b.id') },
        ],
      });
    });

    it('splits notInList with a Field inside the list into AND of NE', () => {
      const predicate = notInList(field('a.id'), [literal(1), field('b.id')]);

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', operation: '!=', left: field('a.id'), right: literal(1) },
          { type: 'COMPARISON', operation: '!=', left: field('a.id'), right: field('b.id') },
        ],
      });
    });

    it('throws when arrayContainsAny has a Field inside the list', () => {
      const predicate = arrayContainsAny(field('a.tags'), [literal('x'), field('b.tag')]);
      expect(() => simplifyPredicate(predicate)).toThrow(/array-contains-any/);
    });

    it('simplifies single-element lists to scalar comparisons', () => {
      expect(simplifyPredicate(inList(field('a.id'), [literal(1)]))).toStrictEqual({
        type: 'COMPARISON',
        operation: '==',
        left: field('a.id'),
        right: literal(1),
      });

      expect(simplifyPredicate(notInList(field('a.id'), [literal(2)]))).toStrictEqual({
        type: 'COMPARISON',
        operation: '!=',
        left: field('a.id'),
        right: literal(2),
      });

      expect(simplifyPredicate(arrayContainsAny(field('a.tags'), [literal('warm')]))).toStrictEqual({
        type: 'COMPARISON',
        operation: 'array-contains',
        left: field('a.tags'),
        right: literal('warm'),
      });
    });

    it('merges OR of EQs on the same field into inList', () => {
      const predicate: Predicate = {
        type: 'OR',
        conditions: [
          { type: 'COMPARISON', operation: '==', left: field('u.id'), right: literal(1) },
          { type: 'COMPARISON', operation: '==', left: field('u.id'), right: literal(2) },
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'COMPARISON',
        operation: 'in',
        left: field('u.id'),
        right: [literal(1), literal(2)],
      });
    });

    it('merges OR of array-contains into array-contains-any', () => {
      const predicate: Predicate = {
        type: 'OR',
        conditions: [
          arrayContains(field('a.tags'), literal('red')),
          arrayContains(field('a.tags'), literal('blue')),
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(arrayContainsAny(field('a.tags'), [literal('red'), literal('blue')]));
    });

    it('combines shared conjunction with membership into a single inList', () => {
      const base: Predicate = { type: 'COMPARISON', operation: '==', left: field('x.flag'), right: literal(true) };
      const predicate: Predicate = {
        type: 'OR',
        conditions: [
          { type: 'AND', conditions: [base, inList(field('u.id'), [literal(1), literal(2)])] },
          { type: 'AND', conditions: [base, { type: 'COMPARISON', operation: '==', left: field('u.id'), right: literal(3) }] },
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'AND',
        conditions: [
          base,
          { type: 'COMPARISON', operation: 'in', left: field('u.id'), right: [literal(1), literal(2), literal(3)] },
        ],
      });
    });

    it('merges OR of equality with inequalities into inclusive bounds', () => {
      const ltOrEq: Predicate = {
        type: 'OR',
        conditions: [
          { type: 'COMPARISON', operation: '<', left: field('u.score'), right: literal(5) },
          { type: 'COMPARISON', operation: '==', left: field('u.score'), right: literal(5) },
        ],
      };
      expect(simplifyPredicate(ltOrEq)).toStrictEqual({
        type: 'COMPARISON',
        operation: '<=',
        left: field('u.score'),
        right: literal(5),
      });

      const gtOrEq: Predicate = {
        type: 'OR',
        conditions: [
          { type: 'COMPARISON', operation: '>', left: field('u.level'), right: literal(10) },
          { type: 'COMPARISON', operation: '==', left: field('u.level'), right: literal(10) },
        ],
      };
      expect(simplifyPredicate(gtOrEq)).toStrictEqual({
        type: 'COMPARISON',
        operation: '>=',
        left: field('u.level'),
        right: literal(10),
      });
    });

    it('detects contradictory equality against inequalities', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', operation: '<', left: field('u.age'), right: literal(5) },
          { type: 'COMPARISON', operation: '==', left: field('u.age'), right: literal(5) },
        ],
      };
      expect(simplifyPredicate(predicate)).toStrictEqual({ type: 'CONSTANT', value: false });

      const disjoint: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', operation: '<', left: field('u.age'), right: literal(5) },
          { type: 'COMPARISON', operation: '>', left: field('u.age'), right: literal(7) },
        ],
      };
      expect(simplifyPredicate(disjoint)).toStrictEqual({ type: 'CONSTANT', value: false });
    });

    it('prunes inList by scalar bounds', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          inList(field('u.id'), [literal(1), literal(2), literal(5), literal(7)]),
          { type: 'COMPARISON', operation: '>', left: field('u.id'), right: literal(4) },
          { type: 'COMPARISON', operation: '<=', left: field('u.id'), right: literal(7) },
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'COMPARISON',
        operation: 'in',
        left: field('u.id'),
        right: [literal(5), literal(7)],
      });
    });

    it('applies scalar inequality rules (< A || > A => != A) and detects contradictions for AND', () => {
      const orPredicate: Predicate = {
        type: 'OR',
        conditions: [
          { type: 'COMPARISON', operation: '<', left: field('u.score'), right: literal(5) },
          { type: 'COMPARISON', operation: '>', left: field('u.score'), right: literal(5) },
        ],
      };
      expect(simplifyPredicate(orPredicate)).toStrictEqual({
        type: 'COMPARISON',
        operation: '!=',
        left: field('u.score'),
        right: literal(5),
      });

      const andPredicate: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', operation: '<', left: field('u.score'), right: literal(5) },
          { type: 'COMPARISON', operation: '>', left: field('u.score'), right: literal(5) },
        ],
      };
      expect(simplifyPredicate(andPredicate)).toStrictEqual({ type: 'CONSTANT', value: false });

      const mixed: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', operation: '<', left: field('u.score'), right: literal(5) },
          { type: 'COMPARISON', operation: '==', left: field('u.score'), right: literal(2) },
        ],
      };
      expect(simplifyPredicate(mixed)).toStrictEqual({
        type: 'COMPARISON',
        operation: '==',
        left: field('u.score'),
        right: literal(2),
      });
    });

    it('splits oversized inList and notInList to respect Firestore limits', () => {
      const many = Array.from({ length: 31 }, (_, idx) => literal(idx));
      const inPredicate = simplifyPredicate(inList(field('u.id'), many));
      // console.dir(inPredicate, { depth: null });
      expect(inPredicate.type).toBe('OR');
      if (inPredicate.type !== 'OR') throw new Error('Expected OR after splitting');
      expect(inPredicate.conditions).toHaveLength(2);
      const firstChunk = inPredicate.conditions[0];
      const secondChunk = inPredicate.conditions[1];
      if (firstChunk.type !== 'COMPARISON') throw new Error('Expected comparison chunk');
      if (Array.isArray(firstChunk.right)) {
        expect(firstChunk.right).toHaveLength(30);
      }
      if (secondChunk.type !== 'COMPARISON') throw new Error('Expected comparison chunk');
      if (Array.isArray(secondChunk.right)) {
        expect(secondChunk.right).toHaveLength(1);
      } else {
        expect(secondChunk.operation).toBe('==');
      }

      const notInMany = Array.from({ length: 35 }, (_, idx) => literal(idx));
      const notInPredicate = simplifyPredicate(notInList(field('u.id'), notInMany));
      expect(notInPredicate.type).toBe('AND');
      if (notInPredicate.type !== 'AND') throw new Error('Expected AND after splitting');
      expect(notInPredicate.conditions.length).toBeGreaterThan(1);
      const combinedLengths = notInPredicate.conditions.reduce((acc, cond) => {
        if (cond.type !== 'COMPARISON') return acc;
        return acc + (Array.isArray(cond.right) ? cond.right.length : 1);
      }, 0);
      expect(combinedLengths).toBe(notInMany.length);
    });

    it('should unwrap single element AND', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [cmp('a', 'b')],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(cmp('a', 'b'));
    });

    it('should flatten nested AND', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          {
            type: 'AND',
            conditions: [
              cmp('a', 'b'),
              cmp('c', 'd'),
            ],
          },
          cmp('e', 'f'),
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'AND',
        conditions: [
          cmp('a', 'b'),
          cmp('c', 'd'),
          cmp('e', 'f'),
        ],
      });
    });

    it('should remove TRUE from AND', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          cmp('a', 'b'),
          { type: 'CONSTANT', value: true },
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(cmp('a', 'b'));
    });

    it('should reduce AND with FALSE to FALSE', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          cmp('a', 'b'),
          { type: 'CONSTANT', value: false },
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({ type: 'CONSTANT', value: false });
    });

    it('should apply De Morgan: !(A AND B) => !A OR !B', () => {
      const predicate: Predicate = {
        type: 'NOT',
        operand: {
          type: 'AND',
          conditions: [
            cmp('a', 'b'),
            cmp('c', 'd'),
          ],
        },
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'OR',
        conditions: [
          { type: 'NOT', operand: cmp('a', 'b') },
          { type: 'NOT', operand: cmp('c', 'd') },
        ],
      });
    });

    it('should apply double negation: !!A => A', () => {
      const predicate: Predicate = {
        type: 'NOT',
        operand: {
          type: 'NOT',
          operand: {
            ...cmp('a', 'b'),
          },
        },
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(cmp('a', 'b'));
    });
  });

  describe('toDNF', () => {
    it('should distribute (A AND (B OR C)) to (A AND B) OR (A AND C)', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          cmp('a', 'b'),
          {
            type: 'OR',
            conditions: [
              cmp('c', 'd'),
              cmp('e', 'f'),
            ],
          },
        ],
      };

      const result = toDNF(predicate);
      expect(result).toStrictEqual({
        type: 'OR',
        conditions: [
          {
            type: 'AND', conditions: [
              cmp('a', 'b'),
              cmp('c', 'd'),
            ],
          },
          {
            type: 'AND', conditions: [
              cmp('a', 'b'),
              cmp('e', 'f'),
            ],
          },
        ],
      });
    });

    it('multi-level AND/OR/NOTs should be simplified to OR of ANDs (max 3 levels)', () => {
      const A = cmp('A', 'val') as Predicate;
      const B = cmp('B', 'val') as Predicate;
      const C = cmp('C', 'val') as Predicate;
      const D = cmp('D', 'val') as Predicate;

      // deeply nested mix of AND/OR/NOT
      const predicate: Predicate = {
        type: 'OR',
        conditions: [
          {
            type: 'AND',
            conditions: [
              A,
              {
                type: 'OR',
                conditions: [
                  {
                    type: 'AND',
                    conditions: [B, { type: 'NOT', operand: C }],
                  },
                  {
                    type: 'OR',
                    conditions: [
                      { type: 'NOT', operand: { type: 'NOT', operand: D } as Predicate },
                      C,
                    ],
                  },
                ],
              },
            ],
          },
          { type: 'NOT', operand: { type: 'OR', conditions: [A, B] } as Predicate },
        ],
      };

      const result = toDNF(predicate);
      expect(result).toStrictEqual({
        type: 'OR',
        conditions: [
          { type: 'AND', conditions: [A, B, { type: 'NOT', operand: C }] },
          { type: 'AND', conditions: [A, D] },
          { type: 'AND', conditions: [A, C] },
          { type: 'AND', conditions: [{ type: 'NOT', operand: A }, { type: 'NOT', operand: B }] },
        ],
      });
    });

    it('should simplify (A || B) && (A || !B || C) && !C to A', () => {
      const A = cmp('A', 'val') as Predicate;
      const B = cmp('B', 'val') as Predicate;
      const C = cmp('C', 'val') as Predicate;

      const expr: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'OR', conditions: [A, B] },
          {
            type: 'OR',
            conditions: [
              A,
              { type: 'NOT', operand: B },
              C,
            ],
          },
          { type: 'NOT', operand: C },
        ],
      };

      const result = simplifyPredicate(toDNF(expr));

      expect(result).toStrictEqual({ type: 'AND', conditions: [{ type: 'NOT', operand: C }, A] });
    });
  });
});
