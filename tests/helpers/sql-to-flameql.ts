import parse from 'sqlite-parser';
import { collection, field, literal, projection } from '../../src/api/api';
import { type Expression, type Predicate, Projection } from '../../src/api/expression';

export interface SelectMapping {
  outKey: string;
  index: number;
  expr: Expression;
}

export interface TranslationResult {
  projection: Projection;
  select: SelectMapping[];
  ordered: boolean;
}

interface SourceDef {
  alias: string;
  table: string;
}

interface Context {
  sources: SourceDef[];
  schema: Record<string, string[]>;
}

export function translateSqlToFlame(sql: string, schema: Record<string, string[]>): TranslationResult {
  const parsed = parse(sql);
  if (parsed.type !== 'statement' || parsed.variant !== 'list' || !parsed.statement?.length) {
    throw new Error('Expected a SQL statement list');
  }

  const first = parsed.statement[0];
  if (first.variant !== 'select') {
    throw new Error(`Only SELECT is supported, got ${first.variant}`);
  }

  const { sources, joinPredicates } = flattenSources(first.from);
  const ctx: Context = { sources, schema };

  const select = buildSelect(first.result ?? [], ctx);
  const wherePredicates = (first.where ?? []).map((w: any) => toPredicate(w, ctx));
  const combinedPredicates = [...joinPredicates, ...wherePredicates];
  const where = combinedPredicates.length === 0
    ? undefined
    : combinePredicates(combinedPredicates);

  const orderBy = (first.order ?? []).map((o: any) => toOrder(o, ctx));
  const limit = first.limit?.start ? Number(first.limit.start.value) : undefined;
  const offset = first.limit?.offset ? Number(first.limit.offset.value) : undefined;

  const projectionInput: any = {
    id: `sql-${Buffer.from(sql).toString('base64url')}`,
    from: Object.fromEntries(sources.map(src => [src.alias, collection(src.table)])),
    select: Object.fromEntries(select.map(s => [s.outKey, s.expr])),
  };

  if (where) projectionInput.where = where;
  if (orderBy.length) projectionInput.orderBy = orderBy;
  if (limit !== undefined) projectionInput.limit = limit;
  if (offset !== undefined) projectionInput.offset = offset;

  const projectionObj = projection(projectionInput);

  return {
    projection: projectionObj,
    select,
    ordered: orderBy.length > 0,
  };
}

export function normalizeSqlRow(row: any[], select: SelectMapping[]): Record<string, any> {
  const out: Record<string, any> = {};
  for (const mapping of select) {
    out[mapping.outKey] = row[mapping.index];
  }
  return out;
}

export function normalizeFlameRow(row: Record<string, any>, select: SelectMapping[]): Record<string, any> {
  const out: Record<string, any> = {};
  for (const mapping of select) {
    out[mapping.outKey] = row[mapping.outKey];
  }
  return out;
}

function buildSelect(nodes: any[], ctx: Context): SelectMapping[] {
  const mappings: SelectMapping[] = [];
  let index = 0;

  for (const node of nodes) {
    if (node.type === 'identifier' && node.variant === 'column') {
      const { alias, column } = resolveColumn(node.name, ctx);
      const outKey = node.alias ?? `${alias}.${column}`;
      mappings.push({
        outKey,
        index,
        expr: field(`${alias}.${column}`),
      });
      index++;
      continue;
    }

    if (node.type === 'identifier' && node.variant === 'star') {
      const targets = node.name === '*' ? ctx.sources : ctx.sources.filter(s => s.alias === node.name.replace('.*', ''));
      if (!targets.length) {
        throw new Error(`Unknown alias in * expansion: ${node.name}`);
      }
      for (const src of targets) {
        const columns = ctx.schema[src.table] ?? [];
        for (const col of columns) {
          mappings.push({
            outKey: `${src.alias}.${col}`,
            index,
            expr: field(`${src.alias}.${col}`),
          });
          index++;
        }
      }
      continue;
    }

    if (node.type === 'literal') {
      const value = toLiteralValue(node);
      const outKey = node.alias ?? `literal_${mappings.length}`;
      const expr = literal(value);
      mappings.push({ outKey, index, expr });
      index++;
      continue;
    }

    throw new Error(`Unsupported select node: ${JSON.stringify(node)}`);
  }

  return mappings;
}

function flattenSources(fromNode: any): { sources: SourceDef[]; joinPredicates: Predicate[] } {
  if (!fromNode) throw new Error('FROM clause is required');

  if (fromNode.type === 'identifier' && fromNode.variant === 'table') {
    const alias = fromNode.alias ?? fromNode.name;
    return { sources: [{ alias, table: fromNode.name }], joinPredicates: [] };
  }

  if (fromNode.type === 'map' && fromNode.variant === 'join') {
    const base = flattenSources(fromNode.source);
    let sources = [...base.sources];
    let joinPredicates = [...base.joinPredicates];

    for (const j of fromNode.map ?? []) {
      if (j.source?.type === 'identifier' && j.source.variant === 'table') {
        const alias = j.source.alias ?? j.source.name;
        sources.push({ alias, table: j.source.name });
      }
      if (j.constraint?.on) {
        const ctx: Context = { sources, schema: {} };
        joinPredicates.push(toPredicate(j.constraint.on, ctx));
      }
    }
    return { sources, joinPredicates };
  }

  throw new Error(`Unsupported FROM node: ${JSON.stringify(fromNode)}`);
}

function toOrder(node: any, ctx: Context) {
  if (node.type === 'identifier' && node.variant === 'column') {
    const { alias, column } = resolveColumn(node.name, ctx);
    return { field: `${alias}.${column}`, direction: 'asc' as const };
  }
  if (node.type === 'expression' && node.variant === 'order') {
    const expr = node.expression;
    if (expr.type === 'identifier' && expr.variant === 'column') {
      const { alias, column } = resolveColumn(expr.name, ctx);
      return {
        field: `${alias}.${column}`,
        direction: node.direction === 'desc' ? ('desc' as const) : ('asc' as const),
      };
    }
  }
  throw new Error(`Unsupported ORDER BY clause: ${JSON.stringify(node)}`);
}

function toPredicate(node: any, ctx: Context): Predicate {
  if (node.type === 'expression' && node.variant === 'operation') {
    if (node.format === 'unary' && node.operator?.toLowerCase() === 'not') {
      return { type: 'NOT', operand: toPredicate(node.expression, ctx) };
    }

    if (node.format === 'binary') {
      const op = String(node.operation).toLowerCase();
      switch (op) {
        case 'and':
          return {
            type: 'AND',
            conditions: [toPredicate(node.left, ctx), toPredicate(node.right, ctx)],
          };
        case 'or':
          return {
            type: 'OR',
            conditions: [toPredicate(node.left, ctx), toPredicate(node.right, ctx)],
          };
        case '=':
          return comparison(node.left, node.right, '==', ctx);
        case '!=':
        case '<>':
          return comparison(node.left, node.right, '!=', ctx);
        case '>':
        case '>=':
        case '<':
        case '<=':
          return comparison(node.left, node.right, op as any, ctx);
        case 'in':
          return inList(node.left, node.right, ctx, false);
        case 'not in':
          return inList(node.left, node.right, ctx, true);
        case 'is':
          return comparison(node.left, node.right, '==', ctx);
        case 'is not':
          return comparison(node.left, node.right, '!=', ctx);
        default:
          throw new Error(`Unsupported binary operation: ${op}`);
      }
    }
  }

  if (node.type === 'literal') {
    const value = toLiteralValue(node);
    return { type: 'CONSTANT', value: Boolean(value) };
  }

  if (node.type === 'identifier' && node.variant === 'column') {
    const { alias, column } = resolveColumn(node.name, ctx);
    const expr = field(`${alias}.${column}`);
    return { type: 'COMPARISON', left: expr, right: literal(true), operation: '==' };
  }

  throw new Error(`Unsupported predicate node: ${JSON.stringify(node)}`);
}

function inList(
  left: any,
  right: any,
  ctx: Context,
  negate: boolean
): Predicate {
  if (!right || right.type !== 'expression' || right.variant !== 'list') {
    throw new Error('IN requires a list');
  }
  const predicates = (right.expression ?? []).map((item: any) =>
    comparison(left, item, negate ? '!=' : '==', ctx)
  );
  return combinePredicates(predicates, negate ? 'AND' : 'OR');
}

function comparison(
  leftNode: any,
  rightNode: any,
  op: any,
  ctx: Context
): Predicate {
  const left = toExpression(leftNode, ctx);
  const right = toExpression(rightNode, ctx);
  return { type: 'COMPARISON', left, right, operation: op };
}

function toExpression(node: any, ctx: Context): Expression {
  if (node.type === 'identifier' && node.variant === 'column') {
    const { alias, column } = resolveColumn(node.name, ctx);
    return field(`${alias}.${column}`);
  }

  if (node.type === 'literal') {
    return literal(toLiteralValue(node));
  }

  throw new Error(`Unsupported expression node: ${JSON.stringify(node)}`);
}

function resolveColumn(name: string, ctx: Context): { alias: string; column: string } {
  if (name.includes('.')) {
    const [alias, ...rest] = name.split('.');
    return { alias, column: rest.join('.') };
  }

  if (ctx.sources.length === 1) {
    return { alias: ctx.sources[0].alias, column: name };
  }

  const matching = ctx.sources.filter(src => ctx.schema[src.table]?.includes(name));
  if (matching.length === 1) {
    return { alias: matching[0].alias, column: name };
  }

  throw new Error(`Ambiguous column reference: ${name}`);
}

function combinePredicates(predicates: Predicate[], type: 'AND' | 'OR' = 'AND'): Predicate {
  if (predicates.length === 1) return predicates[0];
  return { type, conditions: predicates };
}

function toLiteralValue(node: any): any {
  switch (node.variant) {
    case 'decimal':
      return Number(node.value);
    case 'text':
      return node.value;
    case 'null':
      return null;
    default:
      return node.value;
  }
}
