import { WhereFilterOp } from '@google-cloud/firestore';
import { Expression, ExpressionInput, Field, Literal, Param, Predicate } from '../../api/expression';

export function generateExpressionSelector(expr: Expression): string {
  const valueCode = generateExpressionCode(expr);
  return `(row, params) => ${valueCode}`;
}

export function generateExpressionCode(expr: Expression): string {
  if (expr === undefined || expr === null) return 'null';
  switch (expr.kind) {
    case 'Field':
      return generateFieldCode(expr as Field);
    case 'Literal':
      return generateLiteralCode(expr as Literal);
    case 'Param':
      return generateParamCode(expr as Param);
    case 'FunctionExpression': {
      const inputCode = emitExpressionInput(expr.input as ExpressionInput);
      const fnSource = expr.fn.toString();
      return `(${fnSource})(${inputCode})`;
    }
    default:
      expr satisfies never;
      throw new Error(`Unexpected expression type: ${expr}`);
  }
}

function emitExpressionInput(input: ExpressionInput): string {
  if (Array.isArray(input)) {
    const parts: string[] = [];
    for (const item of input as ExpressionInput[]) {
      parts.push(emitExpressionInput(item));
    }
    return `[${parts.join(', ')}]`;
  }
  return generateExpressionCode(input);
}

function generateFieldCode(field: Field): string {
  const source = field.source;
  if (!source) throw new Error('Field must have a source alias');

  // Use runtime helper `getValue` to support array traversal and metadata fields
  const pathParts = field.path.map(part => `'${part.replace(/'/g, "\\'")}'`).join(', ');
  return `getValue(row.${source}, [${pathParts}])`;
}

function generateLiteralCode(literal: Literal): string {
  const val = literal.value;
  if (typeof val === 'string') return `'${val.replace(/'/g, "\\'")}'`;
  if (typeof val === 'number' || typeof val === 'boolean') return String(val);
  if (val === null) return 'null';
  val satisfies never;
  throw new Error(`Unexpected literal value: ${val}`);
}

function generateParamCode(param: Param): string {
  return `params['${param.name}']`;
}

export function generatePredicateCode(predicate: Predicate): string {
  switch (predicate.type) {
    case 'CONSTANT':
      return String(predicate.value);

    case 'NOT':
      return `!(${generatePredicateCode(predicate.operand)})`;

    case 'AND':
      if (predicate.conditions.length === 0) return 'true';
      return `(${predicate.conditions.map(generatePredicateCode).join(' && ')})`;

    case 'OR':
      if (predicate.conditions.length === 0) return 'false';
      return `(${predicate.conditions.map(generatePredicateCode).join(' || ')})`;

    case 'COMPARISON':
      const left = generateExpressionCode(predicate.left);
      const right = Array.isArray(predicate.right)
        ? `[${predicate.right.map(generateExpressionCode).join(', ')}]`
        : generateExpressionCode(predicate.right);

      return generateComparisonCode(left, predicate.operation, right);

    case 'CUSTOM':
      const fnSource = predicate.fn.toString();
      const inputCode = emitExpressionInput(predicate.input as ExpressionInput);
      return `(${fnSource})(${inputCode})`;

    default:
      throw new Error(`Unknown predicate type: ${(predicate as any).type}`);
  }
}

function generateComparisonCode(left: string, op: WhereFilterOp, right: string): string {
  switch (op) {
    case '==': return `${left} === ${right}`;
    case '!=': return `${left} !== ${right}`;
    case '>': return `${left} > ${right}`;
    case '>=': return `${left} >= ${right}`;
    case '<': return `${left} < ${right}`;
    case '<=': return `${left} <= ${right}`;
    case 'in': return `${right}.includes(${left})`; // array.includes(value)
    case 'not-in': return `!${right}.includes(${left})`;
    case 'array-contains': return `${left}.includes(${right})`;
    case 'array-contains-any':
      // left is array, right is array. check intersection.
      // TODO: turn into Set first
      return `${left}.some(v => ${right}.includes(v))`;
    default:
      op satisfies never;
      throw new Error(`Unknown comparison operator: ${op}`);
  }
}
