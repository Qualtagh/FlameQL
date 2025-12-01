import { Field, Literal } from '../api/expression';
import { DOC_COLLECTION, DOC_ID, DOC_PARENT, DOC_PATH } from './symbols';

/**
 * Maps special field names (starting with #) to their corresponding Symbol keys.
 */
const METADATA_FIELD_MAP: Record<string, symbol> = {
  id: DOC_ID,
  path: DOC_PATH,
  collection: DOC_COLLECTION,
  parent: DOC_PARENT,
};

function getValue(row: any, path: string[]): any {
  let value = row;
  for (const part of path) {
    if (value === undefined || value === null) return null;

    // Check if this is a metadata field reference (e.g., '#id')
    if (part.startsWith('#')) {
      const metadataKey = part.substring(1); // Remove the '#' prefix
      const symbol = METADATA_FIELD_MAP[metadataKey];
      if (symbol) {
        value = value[symbol];
      } else {
        throw new Error(`Unknown metadata field: ${part}`);
      }
    } else {
      // Regular field access
      value = value[part];
    }
  }
  return value;
}

export function getValueFromPath(obj: any, path: string): any {
  return getValue(obj, path.split('.'));
}

export function evaluate(expr: any, row: any): any {
  if (expr instanceof Field) {
    // expr.source is the alias (e.g. 'j')
    // expr.path is the field path (e.g. ['title'] or ['#id'])
    const sourceData = row[expr.source!];
    if (!sourceData) return null;

    return getValue(sourceData, expr.path);
  } else if (expr instanceof Literal) {
    return expr.value;
  }

  return null;
}
