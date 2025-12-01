import { WhereFilterOp } from '@google-cloud/firestore';

/**
 * Converts a WhereFilterOp into a binary comparison callback function.
 * This is used for in-memory filtering and nested loop joins.
 *
 * @param operation - The Firestore where filter operation
 * @returns A function that compares two values according to the operation
 */
export function createOperationComparator(
  operation: WhereFilterOp
): (a: any, b: any) => boolean {
  switch (operation) {
    case '==':
      return (a, b) => a == b;

    case '!=':
      return (a, b) => a != b;

    case '<':
      return (a, b) => a < b;

    case '<=':
      return (a, b) => a <= b;

    case '>':
      return (a, b) => a > b;

    case '>=':
      return (a, b) => a >= b;

    case 'in':
      return (a, b) => {
        if (!Array.isArray(b)) {
          throw new Error(`'in' operation requires right operand to be an array`);
        }
        return b.includes(a);
      };

    case 'not-in':
      return (a, b) => {
        if (!Array.isArray(b)) {
          throw new Error(`'not-in' operation requires right operand to be an array`);
        }
        return !b.includes(a);
      };

    case 'array-contains':
      return (a, b) => {
        if (!Array.isArray(a)) {
          return false;
        }
        return a.includes(b);
      };

    case 'array-contains-any':
      return (a, b) => {
        if (!Array.isArray(a) || !Array.isArray(b)) {
          return false;
        }
        const setA = new Set(a);
        for (const item of b) {
          if (setA.has(item)) {
            return true;
          }
        }
        return false;
      };

    default:
      throw new Error(`Unsupported operation: ${operation}`);
  }
}

/**
 * Determines if an operation can be optimized using a hash join.
 * Hash joins require operations that can be efficiently indexed.
 */
export function isHashJoinCompatible(operation: WhereFilterOp): boolean {
  switch (operation) {
    case '==':
    case 'in':
    case 'array-contains':
    case 'array-contains-any':
      return true;
    default:
      return false;
  }
}
