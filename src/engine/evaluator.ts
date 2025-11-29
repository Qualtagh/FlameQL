import { Field, Literal } from '../api/expression';

export function evaluate(expr: any, row: any): any {
  if (expr instanceof Field) {
    // expr.source is the alias (e.g. 'j')
    // expr.path is the field path (e.g. ['title'])
    const sourceData = row[expr.source!];
    if (!sourceData) return null;

    let value = sourceData;
    for (const part of expr.path) {
      if (value === undefined || value === null) return null;
      value = value[part];
    }
    return value;
  } else if (expr instanceof Literal) {
    return expr.value;
  }

  return null;
}
