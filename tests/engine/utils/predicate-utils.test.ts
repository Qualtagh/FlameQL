import { and, arrayContains, arrayContainsAny, constant, eq, field, gt, gte, inList, literal, lt, lte, ne, not, notInList, or, param } from '../../../src/api/api';
import { Predicate } from '../../../src/api/expression';
import { simplifyPredicate, toDNF } from '../../../src/engine/utils/predicate-utils';

const cmp = (left: any, right: any) => eq(field(`t.${left}`), literal(right));

describe('Predicate Utilities', () => {
  describe('simplifyPredicate', () => {
    it('normalizes comparison to keep Field on the left', () => {
      const predicate: Predicate = eq(literal(5), field('u.id'));
      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(eq(field('u.id'), literal(5)));
    });

    it('folds literal-to-literal comparisons (including lists) into CONSTANT', () => {
      expect(simplifyPredicate(eq(literal(1), literal(1)))).toStrictEqual(constant(true));
      expect(simplifyPredicate(ne(literal(1), literal(2)))).toStrictEqual(constant(true));
      expect(simplifyPredicate(inList(literal(2), [literal(1), literal(2)]))).toStrictEqual(constant(true));
      expect(simplifyPredicate(inList(literal(3), [literal(1), literal(2)]))).toStrictEqual(constant(false));
    });

    it('splits inList with a Field inside the list into OR of EQ', () => {
      const predicate = inList(field('a.id'), [literal(1), param('p'), field('b.id')]);
      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(or([
        inList(field('a.id'), [literal(1), param('p')]),
        eq(field('a.id'), field('b.id')),
      ]));
    });

    it('splits notInList with a Field inside the list into AND of NE', () => {
      const predicate = notInList(field('a.id'), [literal(1), field('b.id')]);
      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(and([
        ne(field('a.id'), literal(1)),
        ne(field('a.id'), field('b.id')),
      ]));
    });

    it('throws when arrayContainsAny has a Field inside the list', () => {
      const predicate = arrayContainsAny(field('a.tags'), [literal('x'), field('b.tag')]);
      expect(() => simplifyPredicate(predicate)).toThrow(/array-contains-any/);
    });

    it('simplifies single-element lists to scalar comparisons', () => {
      expect(simplifyPredicate(inList(field('a.id'), [literal(1)]))).toStrictEqual(eq(field('a.id'), literal(1)));
      expect(simplifyPredicate(notInList(field('a.id'), [literal(2)]))).toStrictEqual(ne(field('a.id'), literal(2)));
      expect(simplifyPredicate(arrayContainsAny(field('a.tags'), [literal('warm')]))).toStrictEqual(arrayContains(field('a.tags'), literal('warm')));
    });

    it('merges OR of EQs on the same field into inList', () => {
      const predicate: Predicate = or([eq(field('u.id'), literal(1)), eq(field('u.id'), literal(2))]);
      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(inList(field('u.id'), [literal(1), literal(2)]));
    });

    it('merges OR of array-contains into array-contains-any', () => {
      const predicate: Predicate = or([arrayContains(field('a.tags'), literal('red')), arrayContains(field('a.tags'), literal('blue'))]);
      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(arrayContainsAny(field('a.tags'), [literal('red'), literal('blue')]));
    });

    it('combines shared conjunction with membership into a single inList', () => {
      const base: Predicate = eq(field('x.flag'), literal(true));
      const predicate: Predicate = or([
        and([base, inList(field('u.id'), [literal(1), literal(2)])]),
        and([base, eq(field('u.id'), literal(3))]),
      ]);
      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(and([
        base,
        inList(field('u.id'), [literal(1), literal(2), literal(3)]),
      ]));
    });

    it('merges OR of equality with inequalities into inclusive bounds', () => {
      const ltOrEq: Predicate = or([
        lt(field('u.score'), literal(5)),
        eq(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(ltOrEq)).toStrictEqual(lte(field('u.score'), literal(5)));

      const gtOrEq: Predicate = or([
        gt(field('u.level'), literal(10)),
        eq(field('u.level'), literal(10)),
      ]);
      expect(simplifyPredicate(gtOrEq)).toStrictEqual(gte(field('u.level'), literal(10)));
    });

    it('handles != with bounds on AND/OR', () => {
      const neqAndGte: Predicate = and([
        ne(field('u.score'), literal(5)),
        gte(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqAndGte)).toStrictEqual(gt(field('u.score'), literal(5)));

      const neqAndGt: Predicate = and([
        ne(field('u.score'), literal(5)),
        gt(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqAndGt)).toStrictEqual(gt(field('u.score'), literal(5)));

      const neqOrGte: Predicate = or([
        ne(field('u.score'), literal(5)),
        gte(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqOrGte)).toStrictEqual(constant(true));

      const neqOrGt: Predicate = or([
        ne(field('u.score'), literal(5)),
        gt(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqOrGt)).toStrictEqual(ne(field('u.score'), literal(5)));

      const neqAndLte: Predicate = and([
        ne(field('u.score'), literal(5)),
        lte(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqAndLte)).toStrictEqual(lt(field('u.score'), literal(5)));

      const neqAndLt: Predicate = and([
        ne(field('u.score'), literal(5)),
        lt(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqAndLt)).toStrictEqual(lt(field('u.score'), literal(5)));

      const neqOrLte: Predicate = or([
        ne(field('u.score'), literal(5)),
        lte(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqOrLte)).toStrictEqual(constant(true));

      const neqOrLt: Predicate = or([
        ne(field('u.score'), literal(5)),
        lt(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(neqOrLt)).toStrictEqual(ne(field('u.score'), literal(5)));
    });

    it('handles not-in with eq/inList across AND/OR', () => {
      const notInAndEq: Predicate = and([
        notInList(field('u.id'), [literal(1), literal(2)]),
        eq(field('u.id'), literal(1)),
      ]);
      expect(simplifyPredicate(notInAndEq)).toStrictEqual(constant(false));

      const notInAndEqDisjoint: Predicate = and([
        notInList(field('u.id'), [literal(1), literal(2)]),
        eq(field('u.id'), literal(3)),
      ]);
      expect(simplifyPredicate(notInAndEqDisjoint)).toStrictEqual(eq(field('u.id'), literal(3)));

      const notInAndIn: Predicate = and([
        notInList(field('u.id'), [literal(1), literal(2)]),
        inList(field('u.id'), [literal(1), literal(3)]),
      ]);
      expect(simplifyPredicate(notInAndIn)).toStrictEqual(eq(field('u.id'), literal(3)));

      const notInOrEq: Predicate = or([
        notInList(field('u.id'), [literal(1)]),
        eq(field('u.id'), literal(1)),
      ]);
      expect(simplifyPredicate(notInOrEq)).toStrictEqual(constant(true));

      const notInOrEqDisjoint: Predicate = or([
        notInList(field('u.id'), [literal(1)]),
        eq(field('u.id'), literal(2)),
      ]);
      expect(simplifyPredicate(notInOrEqDisjoint)).toStrictEqual(ne(field('u.id'), literal(1)));

      const notInOrIn: Predicate = or([
        notInList(field('u.id'), [literal(1), literal(2)]),
        inList(field('u.id'), [literal(1), literal(3)]),
      ]);
      expect(simplifyPredicate(notInOrIn)).toStrictEqual(ne(field('u.id'), literal(2)));
    });

    it('detects contradictory equality against inequalities', () => {
      const predicate: Predicate = and([
        lt(field('u.age'), literal(5)),
        eq(field('u.age'), literal(5)),
      ]);
      expect(simplifyPredicate(predicate)).toStrictEqual(constant(false));

      const disjoint: Predicate = and([
        lt(field('u.age'), literal(5)),
        gt(field('u.age'), literal(7)),
      ]);
      expect(simplifyPredicate(disjoint)).toStrictEqual(constant(false));
    });

    it('prunes inList by scalar bounds', () => {
      const predicate: Predicate = and([
        inList(field('u.id'), [literal(1), literal(2), literal(5), literal(7)]),
        gt(field('u.id'), literal(4)),
        lte(field('u.id'), literal(7)),
      ]);

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(inList(field('u.id'), [literal(5), literal(7)]));
    });

    it('applies scalar inequality rules (< A || > A => != A) and detects contradictions for AND', () => {
      const orPredicate: Predicate = or([
        lt(field('u.score'), literal(5)),
        gt(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(orPredicate)).toStrictEqual(ne(field('u.score'), literal(5)));

      const andPredicate: Predicate = and([
        lt(field('u.score'), literal(5)),
        gt(field('u.score'), literal(5)),
      ]);
      expect(simplifyPredicate(andPredicate)).toStrictEqual(constant(false));

      const mixed: Predicate = and([
        lt(field('u.score'), literal(5)),
        eq(field('u.score'), literal(2)),
      ]);
      expect(simplifyPredicate(mixed)).toStrictEqual(eq(field('u.score'), literal(2)));
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
      const predicate: Predicate = and([cmp('a', 'b')]);

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(cmp('a', 'b'));
    });

    it('should flatten nested AND', () => {
      const predicate: Predicate = and([
        and([
          cmp('a', 'b'),
          cmp('c', 'd'),
        ]),
        cmp('e', 'f'),
      ]);

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(and([
        cmp('a', 'b'),
        cmp('c', 'd'),
        cmp('e', 'f'),
      ]));
    });

    it('should remove TRUE from AND', () => {
      const predicate: Predicate = and([
        cmp('a', 'b'),
        constant(true),
      ]);

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(cmp('a', 'b'));
    });

    it('should reduce AND with FALSE to FALSE', () => {
      const predicate: Predicate = and([
        cmp('a', 'b'),
        constant(false),
      ]);

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(constant(false));
    });

    it('should apply De Morgan: !(A AND B) => !A OR !B', () => {
      const predicate: Predicate = not(and([
        cmp('a', 'b'),
        cmp('c', 'd'),
      ]));

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(or([
        not(cmp('a', 'b')),
        not(cmp('c', 'd')),
      ]));
    });

    it('should apply double negation: !!A => A', () => {
      const predicate: Predicate = not(not(cmp('a', 'b')));

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual(cmp('a', 'b'));
    });
  });

  describe('toDNF', () => {
    it('should distribute (A AND (B OR C)) to (A AND B) OR (A AND C)', () => {
      const predicate: Predicate = and([
        cmp('a', 'b'),
        or([
          cmp('c', 'd'),
          cmp('e', 'f'),
        ]),
      ]);

      const result = toDNF(predicate);
      expect(result).toStrictEqual(or([
        and([cmp('a', 'b'), cmp('c', 'd')]),
        and([cmp('a', 'b'), cmp('e', 'f')]),
      ]));
    });

    it('multi-level AND/OR/NOTs should be simplified to OR of ANDs (max 3 levels)', () => {
      const A = cmp('A', 'val') as Predicate;
      const B = cmp('B', 'val') as Predicate;
      const C = cmp('C', 'val') as Predicate;
      const D = cmp('D', 'val') as Predicate;

      // deeply nested mix of AND/OR/NOT
      const predicate: Predicate = or([
        and([
          A,
          or([
            and([B, not(C)]),
            or([
              not(not(D)),
              C,
            ]),
          ]),
        ]),
        not(or([A, B])),
      ]);

      const result = toDNF(predicate);
      expect(result).toStrictEqual(or([
        and([A, B, not(C)]),
        and([A, D]),
        and([A, C]),
        and([not(A), not(B)]),
      ]));
    });

    it('should simplify (A || B) && (A || !B || C) && !C to A', () => {
      const A = cmp('A', 'val') as Predicate;
      const B = cmp('B', 'val') as Predicate;
      const C = cmp('C', 'val') as Predicate;

      const expr: Predicate = and([
        or([A, B]),
        or([A, not(B), C]),
        not(C),
      ]);

      const result = simplifyPredicate(toDNF(expr));

      expect(result).toStrictEqual(and([not(C), A]));
    });
  });
});
