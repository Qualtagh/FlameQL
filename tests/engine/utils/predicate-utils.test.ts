import { Predicate } from '../../../src/engine/ast';
import { simplifyPredicate, toDNF } from '../../../src/engine/utils/predicate-utils';

describe('Predicate Utilities', () => {
  describe('simplifyPredicate', () => {
    it('should unwrap single element AND', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [{
          type: 'COMPARISON',
          left: 'a',
          right: 'b',
          operation: '==',
        }],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'COMPARISON',
        left: 'a',
        right: 'b',
        operation: '==',
      });
    });

    it('should flatten nested AND', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          {
            type: 'AND',
            conditions: [
              { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
              { type: 'COMPARISON', left: 'c', right: 'd', operation: '==' },
            ],
          },
          { type: 'COMPARISON', left: 'e', right: 'f', operation: '==' },
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
          { type: 'COMPARISON', left: 'c', right: 'd', operation: '==' },
          { type: 'COMPARISON', left: 'e', right: 'f', operation: '==' },
        ],
      });
    });

    it('should remove TRUE from AND', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
          { type: 'CONSTANT', value: true },
        ],
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'COMPARISON',
        left: 'a',
        right: 'b',
        operation: '==',
      });
    });

    it('should reduce AND with FALSE to FALSE', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
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
            { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
            { type: 'COMPARISON', left: 'c', right: 'd', operation: '==' },
          ],
        },
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'OR',
        conditions: [
          { type: 'NOT', operand: { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' } },
          { type: 'NOT', operand: { type: 'COMPARISON', left: 'c', right: 'd', operation: '==' } },
        ],
      });
    });

    it('should apply double negation: !!A => A', () => {
      const predicate: Predicate = {
        type: 'NOT',
        operand: {
          type: 'NOT',
          operand: {
            type: 'COMPARISON',
            left: 'a',
            right: 'b',
            operation: '==',
          },
        },
      };

      const result = simplifyPredicate(predicate);
      expect(result).toStrictEqual({
        type: 'COMPARISON',
        left: 'a',
        right: 'b',
        operation: '==',
      });
    });
  });

  describe('toDNF', () => {
    it('should distribute (A AND (B OR C)) to (A AND B) OR (A AND C)', () => {
      const predicate: Predicate = {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
          {
            type: 'OR',
            conditions: [
              { type: 'COMPARISON', left: 'c', right: 'd', operation: '==' },
              { type: 'COMPARISON', left: 'e', right: 'f', operation: '==' },
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
              { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
              { type: 'COMPARISON', left: 'c', right: 'd', operation: '==' },
            ],
          },
          {
            type: 'AND', conditions: [
              { type: 'COMPARISON', left: 'a', right: 'b', operation: '==' },
              { type: 'COMPARISON', left: 'e', right: 'f', operation: '==' },
            ],
          },
        ],
      });
    });

    it('multi-level AND/OR/NOTs should be simplified to OR of ANDs (max 3 levels)', () => {
      const A = { type: 'COMPARISON', left: 'A', right: 'val', operation: '==' } as Predicate;
      const B = { type: 'COMPARISON', left: 'B', right: 'val', operation: '==' } as Predicate;
      const C = { type: 'COMPARISON', left: 'C', right: 'val', operation: '==' } as Predicate;
      const D = { type: 'COMPARISON', left: 'D', right: 'val', operation: '==' } as Predicate;

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
      const A = { type: 'COMPARISON', left: 'A', right: 'val', operation: '==' } as Predicate;
      const B = { type: 'COMPARISON', left: 'B', right: 'val', operation: '==' } as Predicate;
      const C = { type: 'COMPARISON', left: 'C', right: 'val', operation: '==' } as Predicate;

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
