import { Expression, Field, Predicate } from '../api/expression';
import { DOC_COLLECTION, DOC_ID, DOC_PARENT, DOC_PATH } from './symbols';
import { createOperationComparator } from './utils/operation-comparator';

/**
 * Maps special field names (starting with #) to their corresponding Symbol keys.
 */
const METADATA_FIELD_MAP: Record<string, symbol> = {
  id: DOC_ID,
  path: DOC_PATH,
  collection: DOC_COLLECTION,
  parent: DOC_PARENT,
};

export function getValueFromField(row: any, ref: Field): any {
  return getValue(row[ref.source], ref.path);
}

export function evaluate(expr: Expression, row: any): any {
  switch (expr.kind) {
    case 'Field':
      return getValueFromField(row, expr);
    case 'Literal':
      return expr.value;
    case 'Param':
      throw new Error('Param evaluation is not supported at runtime.');
    default:
      expr satisfies never;
  }
}

export function evaluatePredicate(predicate: Predicate, row: any): boolean {
  switch (predicate.type) {
    case 'COMPARISON': {
      const left = evaluate(predicate.left, row);
      const right = Array.isArray(predicate.right)
        ? predicate.right.map(expr => evaluate(expr, row))
        : evaluate(predicate.right, row);
      const comparator = createOperationComparator(predicate.operation);
      return comparator(left, right);
    }
    case 'AND':
      return predicate.conditions.every(cond => evaluatePredicate(cond, row));
    case 'OR':
      return predicate.conditions.some(cond => evaluatePredicate(cond, row));
    case 'NOT':
      return !evaluatePredicate(predicate.operand, row);
    case 'CONSTANT':
      return predicate.value;
    default:
      predicate satisfies never;
      throw new Error('Unexpected predicate type');
  }
}

function getValue(row: any, path: string[]): any {
  let value = row;
  for (let i = 0; i < path.length; i++) {
    const part = path[i];
    if (value === undefined || value === null) return null;

    if (Array.isArray(value)) {
      const remainingPath = path.slice(i);
      const results: any[] = [];
      for (const item of value) {
        const res = getValue(item, remainingPath);
        if (res !== undefined && res !== null) {
          if (Array.isArray(res)) {
            results.push(...res);
          } else {
            results.push(res);
          }
        }
      }
      return results.length > 0 ? results : null;
    }

    if (part.startsWith('#')) {
      const metadataKey = part.substring(1);
      const symbol = METADATA_FIELD_MAP[metadataKey];
      if (symbol) {
        value = value[symbol];
      } else {
        throw new Error(`Unknown metadata field: ${part}`);
      }
    } else {
      value = value[part];
    }
  }
  return value;
}
