import { literal } from '../../../src/api/api';
import { Predicate } from '../../../src/api/expression';
import { simplifyPredicate, toDNF } from '../../../src/engine/utils/predicate-utils';

const cmp = (left: any, right: any, operation: any = '==') => ({
  type: 'COMPARISON',
  left: literal(left),
  right: literal(right),
  operation,
} as const);

describe('Predicate Utilities', () => {
  describe('simplifyPredicate', () => {
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
