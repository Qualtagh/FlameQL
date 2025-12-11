import { Collection, Expression, Field, Literal, LiteralType, OrderBySpec, Param, Predicate, Projection } from '../api/expression';
import { JoinStrategy } from '../api/hints';
import {
  Constraint,
  ExecutionNode,
  FilterNode,
  JoinNode,
  LimitNode,
  NodeType,
  ProjectNode,
  ScanNode,
  SortNode,
  UnionDistinctStrategy,
  UnionNode,
} from './ast';
import { IndexManager } from './indexes/index-manager';
import { Optimizer } from './optimizer';
import { PredicateSplitter } from './predicate-splitter';
import { isHashJoinCompatible, isMergeJoinCompatible } from './utils/operation-comparator';
import { simplifyPredicate } from './utils/predicate-utils';

export class Planner {
  private predicateSplitter: PredicateSplitter;
  private optimizer: Optimizer;
  private params?: Record<string, any>;

  constructor(indexManager: IndexManager = new IndexManager()) {
    this.predicateSplitter = new PredicateSplitter();
    this.optimizer = new Optimizer(indexManager);
  }

  plan(projection: Projection, parameters?: Record<string, any>): ExecutionNode {
    const sources = projection.from || {};
    const aliases = Object.keys(sources);
    this.params = parameters;

    if (aliases.length === 0) {
      throw new Error('No sources defined in projection');
    }

    const orderSpecs = this.parseOrderBy(projection.orderBy, new Set(aliases));
    const wherePredicate = projection.where;
    const normalizedWhere = wherePredicate ? this.normalizePredicate(wherePredicate, new Set(aliases)) : undefined;
    const split = wherePredicate
      ? this.predicateSplitter.split(normalizedWhere!, aliases)
      : { sourcePredicates: {}, joinPredicates: [], residualPredicates: [] };

    const scanPlan = this.buildScanNodes(aliases, sources, split.sourcePredicates, orderSpecs);

    const joinOrder = this.rankJoinOrder(aliases, scanPlan.costs);
    const rootJoin = this.buildJoins(
      joinOrder,
      aliases,
      scanPlan.nodes,
      split.joinPredicates,
      projection.hints?.join ?? JoinStrategy.Auto
    );

    const withResiduals = this.applyResidualFilters(rootJoin, split.residualPredicates);
    const withSortAndLimit = this.applySortAndLimit(withResiduals, orderSpecs, projection.limit, projection.offset);
    const planned = this.applyProjection(withSortAndLimit, projection.select as Record<string, any> | undefined, aliases);
    this.params = undefined;
    return planned;
  }

  private buildScanNodes(
    aliases: string[],
    sources: Record<string, any>,
    sourcePredicates: Record<string, Predicate>,
    orderBy: OrderBySpec[]
  ): { nodes: Record<string, ExecutionNode>; costs: Record<string, number> } {
    const scans: Record<string, ExecutionNode> = {};
    const costs: Record<string, number> = {};

    for (const alias of aliases) {
      const source = sources[alias] as Collection;
      const { path, collectionGroup } = this.resolveCollectionPath(source);
      const predicate = sourcePredicates[alias];

      if (predicate) {
        const optimization = this.optimizer.optimize(
          predicate,
          path,
          orderBy.filter(o => o.field.source === alias)
        );

        costs[alias] = optimization.score;
        if (optimization.strategy === 'UNION_SCAN') {
          const inputs = optimization.scans.map(scan => {
            const base = this.createScanNode(alias, path, collectionGroup, scan.constraints.constraints);
            return scan.constraints.nonIndexable > 0
              ? this.wrapFilter(base, scan.predicate)
              : base;
          });
          scans[alias] = {
            type: NodeType.UNION,
            inputs,
            distinct: UnionDistinctStrategy.DocPath,
          } as UnionNode;
        } else {
          const plan = optimization.scans[0];
          const base = this.createScanNode(alias, path, collectionGroup, plan.constraints.constraints);
          scans[alias] = plan.constraints.nonIndexable > 0
            ? this.wrapFilter(base, plan.predicate)
            : base;
        }
      } else {
        costs[alias] = Infinity;
        scans[alias] = this.createScanNode(alias, path, collectionGroup, []);
      }
    }

    return { nodes: scans, costs };
  }

  private rankJoinOrder(aliases: string[], costs: Record<string, number>): string[] {
    return [...aliases].sort((a, b) => {
      const costA = costs[a] ?? Infinity;
      const costB = costs[b] ?? Infinity;
      if (costA === costB) return aliases.indexOf(a) - aliases.indexOf(b);
      return costA - costB;
    });
  }

  private buildJoins(
    orderedAliases: string[],
    allAliases: string[],
    scanNodes: Record<string, ExecutionNode>,
    joinPredicates: Predicate[],
    hint: JoinStrategy
  ): ExecutionNode {
    let root: ExecutionNode = scanNodes[orderedAliases[0]];
    const joined = new Set<string>([orderedAliases[0]]);
    let remaining = [...joinPredicates];

    for (let i = 1; i < orderedAliases.length; i++) {
      const alias = orderedAliases[i];
      const rightNode = scanNodes[alias];

      const relevant: Predicate[] = [];
      const nextRemaining: Predicate[] = [];

      for (const predicate of remaining) {
        const involved = this.predicateSplitter.getInvolvedSources(predicate, allAliases);
        const hasCurrent = involved.has(alias);
        const hasJoined = Array.from(involved).some(a => joined.has(a));
        const hasFuture = Array.from(involved).some(a => !joined.has(a) && a !== alias);

        if (hasCurrent && hasJoined && !hasFuture) {
          relevant.push(predicate);
        } else {
          nextRemaining.push(predicate);
        }
      }

      remaining = nextRemaining;

      let joinCondition: Predicate;
      if (relevant.length === 0) {
        joinCondition = { type: 'CONSTANT', value: true };
      } else if (relevant.length === 1) {
        joinCondition = relevant[0];
      } else {
        joinCondition = { type: 'AND', conditions: relevant };
      }

      const joinNode: JoinNode = {
        type: NodeType.JOIN,
        left: root,
        right: rightNode,
        joinType: this.resolveJoinStrategy(joinCondition, hint),
        condition: simplifyPredicate(joinCondition),
        crossProduct: relevant.length === 0,
      };

      root = joinNode;
      joined.add(alias);
    }

    if (remaining.length > 0) {
      return this.applyResidualFilters(root, remaining);
    }

    return root;
  }

  private resolveJoinStrategy(condition: Predicate, hint: JoinStrategy): JoinStrategy {
    if (hint && hint !== JoinStrategy.Auto) {
      this.assertJoinHintCompatibility(hint, condition);
      return hint;
    }

    if (isHashJoinCompatible(condition)) {
      return JoinStrategy.Hash;
    }

    if (isMergeJoinCompatible(condition)) {
      return JoinStrategy.Merge;
    }

    return JoinStrategy.NestedLoop;
  }

  private applyResidualFilters(root: ExecutionNode, residualPredicates: Predicate[]): ExecutionNode {
    if (!residualPredicates.length) {
      return root;
    }

    const predicate: Predicate = residualPredicates.length === 1
      ? residualPredicates[0]
      : { type: 'AND', conditions: residualPredicates };

    return {
      type: NodeType.FILTER,
      source: root,
      predicate: simplifyPredicate(predicate),
    } as FilterNode;
  }

  private applyProjection(
    node: ExecutionNode,
    select: Record<string, any> | undefined,
    aliases: string[]
  ): ExecutionNode {
    if (!select) return node;

    const fields: Record<string, any> = {};
    const knownAliases = new Set(aliases);

    for (const [key, value] of Object.entries(select)) {
      fields[key] = this.toExpression(value, knownAliases);
    }

    const projectNode: ProjectNode = {
      type: NodeType.PROJECT,
      source: node,
      fields,
    };

    return projectNode;
  }

  private applySortAndLimit(
    node: ExecutionNode,
    orderSpecs: OrderBySpec[],
    limit: number | undefined,
    offset: number | undefined
  ): ExecutionNode {
    let current = node;

    if (orderSpecs.length > 0) {
      current = {
        type: NodeType.SORT,
        source: current,
        orderBy: orderSpecs,
      } as SortNode;
    }

    if (limit !== undefined || offset !== undefined) {
      current = {
        type: NodeType.LIMIT,
        source: current,
        limit: limit ?? Infinity,
        offset,
      } as LimitNode;
    }

    return current;
  }

  private parseOrderBy(orderBy: any, aliases: Set<string>): OrderBySpec[] {
    const specs: OrderBySpec[] = [];
    for (const entry of orderBy ?? []) {
      if (typeof entry === 'string') {
        specs.push({
          field: this.parseField(entry, aliases),
          direction: 'asc',
        });
        continue;
      }

      if (entry && typeof entry === 'object' && typeof entry.field === 'string') {
        specs.push({
          field: this.parseField(entry.field, aliases),
          direction: entry.direction === 'desc' ? 'desc' : 'asc',
        });
        continue;
      }

      throw new Error('Invalid orderBy specification.');
    }

    return specs;
  }

  private resolveCollectionPath(source: Collection): { path: string; collectionGroup?: boolean } {
    if (!source.path || source.path.length === 0) {
      return { path: 'unknown' };
    }

    const literalSegments: string[] = [];
    for (const seg of source.path) {
      if (seg instanceof Literal) {
        literalSegments.push(String(seg.value));
      }
    }

    if (literalSegments.length === 0) {
      return { path: 'unknown', collectionGroup: source.group };
    }

    return { path: literalSegments.join('/'), collectionGroup: source.group };
  }

  private createScanNode(
    alias: string,
    path: string,
    collectionGroup: boolean | undefined,
    constraints: Constraint[]
  ): ScanNode {
    return {
      type: NodeType.SCAN,
      collectionPath: path,
      collectionGroup,
      alias,
      constraints,
    };
  }

  private wrapFilter(source: ExecutionNode, predicate: Predicate): ExecutionNode {
    return {
      type: NodeType.FILTER,
      source,
      predicate: simplifyPredicate(predicate),
    } as FilterNode;
  }

  private toExpression(value: any, aliases: Set<string>): Expression {
    if (value instanceof Param || value && typeof value === 'object' && value.kind === 'Param') {
      return this.resolveParam((value as Param).name);
    }

    if (typeof value === 'string') {
      return this.parseField(value, aliases);
    }

    if (value instanceof Field || value && typeof value === 'object' && value.kind === 'Field') {
      const field = value as Field;
      this.assertAliasKnown(field, aliases);
      return field;
    }

    if (value instanceof Literal || value && typeof value === 'object' && value.kind === 'Literal') {
      return this.toLiteralValue(value as Literal);
    }

    if (typeof value === 'number' || typeof value === 'boolean' || value === null) {
      return this.toLiteralValue(value);
    }

    throw new Error('Unsupported expression in select clause. Use alias-qualified fields or Literal values.');
  }

  private toLiteralValue(value: Literal | number | boolean | null): Literal {
    if (value instanceof Literal) {
      return value;
    }

    if (value === null) {
      return new Literal(null, LiteralType.Null);
    }

    if (typeof value === 'number') {
      return new Literal(value, LiteralType.Number);
    }

    if (typeof value === 'boolean') {
      return new Literal(value, LiteralType.Boolean);
    }

    return new Literal(value as any, LiteralType.String);
  }

  private parseField(path: string, aliases: Set<string>): Field {
    const segments = path.split('.');
    if (segments.length < 2) {
      throw new Error(`Field reference "${path}" must include alias prefix.`);
    }

    const [alias, ...rest] = segments;
    const ref = new Field(alias, rest);
    this.assertAliasKnown(ref, aliases);
    return ref;
  }

  private assertAliasKnown(ref: Field, aliases: Set<string>) {
    if (!ref.source) {
      throw new Error('Field reference must include an alias.');
    }
    if (!aliases.has(ref.source)) {
      throw new Error(`Unknown alias "${ref.source}" in field reference.`);
    }
  }

  private normalizePredicate(predicate: Predicate, aliases: Set<string>): Predicate {
    switch (predicate.type) {
      case 'COMPARISON':
        return {
          type: 'COMPARISON',
          operation: predicate.operation,
          left: this.normalizeExpression(predicate.left, aliases),
          right: this.normalizeExpression(predicate.right, aliases),
        };
      case 'AND':
      case 'OR':
        return {
          type: predicate.type,
          conditions: predicate.conditions.map(p => this.normalizePredicate(p, aliases)),
        };
      case 'NOT':
        return { type: 'NOT', operand: this.normalizePredicate(predicate.operand, aliases) };
      case 'CONSTANT':
        return predicate;
      default:
        return predicate;
    }
  }

  private normalizeExpression(expr: any, aliases: Set<string>): Expression {
    if (expr instanceof Param || expr && typeof expr === 'object' && expr.kind === 'Param') {
      return this.resolveParam((expr as Param).name);
    }
    if (typeof expr === 'string') {
      return this.parseField(expr, aliases);
    }
    if (expr instanceof Field || expr && typeof expr === 'object' && expr.kind === 'Field') {
      this.assertAliasKnown(expr as Field, aliases);
      return expr as Field;
    }
    if (expr instanceof Literal || expr && typeof expr === 'object' && expr.kind === 'Literal') {
      return this.toLiteralValue(expr as Literal);
    }
    if (typeof expr === 'number' || typeof expr === 'boolean' || expr === null) {
      return this.toLiteralValue(expr);
    }

    throw new Error('Unsupported expression in predicate. Use alias-qualified fields or parameters.');
  }

  private resolveParam(name: string): Literal {
    if (!this.params) {
      throw new Error(`Missing parameters; value required for "${name}".`);
    }
    if (!(name in this.params)) {
      throw new Error(`Parameter "${name}" was not provided.`);
    }
    const value = this.params[name];
    if (value === null) return new Literal(null, LiteralType.Null);
    if (typeof value === 'string') return new Literal(value, LiteralType.String);
    if (typeof value === 'number') return new Literal(value, LiteralType.Number);
    if (typeof value === 'boolean') return new Literal(value, LiteralType.Boolean);
    throw new Error(`Unsupported parameter type for "${name}".`);
  }

  private assertJoinHintCompatibility(hint: JoinStrategy, condition: Predicate) {
    switch (hint) {
      case JoinStrategy.Hash:
        if (!isHashJoinCompatible(condition)) {
          throw new Error('Hash join hint is incompatible with the provided join predicate.');
        }
        return;
      case JoinStrategy.Merge:
        if (!isMergeJoinCompatible(condition)) {
          throw new Error('Merge join hint is incompatible with the provided join predicate.');
        }
        return;
      case JoinStrategy.IndexedNestedLoop:
        if (condition.type !== 'COMPARISON' || !isHashJoinCompatible(condition)) {
          throw new Error('Indexed nested-loop join requires equality-style predicates.');
        }
        return;
      case JoinStrategy.NestedLoop:
      case JoinStrategy.Auto:
        return;
    }
  }
}
