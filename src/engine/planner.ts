import { literal } from '../api/api';
import { Collection, Expression, Field, Literal, OrderBySpec, Param, Predicate, Projection } from '../api/expression';
import { JoinStrategy, PredicateMode, PredicateOrMode } from '../api/hints';
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
  traverseExecutionNode,
  UnionDistinctStrategy,
  UnionNode,
} from './ast';
import { IndexManager } from './indexes/index-manager';
import { SortOrder } from './operators/operator';
import { PredicateSplitter } from './predicate-splitter';
import { pickIndexedNestedLoopLookupPlan } from './utils/indexed-nested-loop-utils';
import { invertComparisonOp, isHashJoinCompatible, isMergeJoinCompatible } from './utils/operation-comparator';
import { simplifyPredicate, toDNF } from './utils/predicate-utils';

export class Planner {
  private indexManager: IndexManager;
  private predicateSplitter: PredicateSplitter;
  private params?: Record<string, any>;

  constructor(indexManager: IndexManager = new IndexManager()) {
    this.indexManager = indexManager;
    this.predicateSplitter = new PredicateSplitter();
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
    const joinHint = projection.hints?.join ?? JoinStrategy.Auto;
    const predicateMode = projection.hints?.predicateMode ?? PredicateMode.Auto;
    const predicateOrMode = projection.hints?.predicateOrMode ?? PredicateOrMode.Auto;

    const base = normalizedWhere
      ? this.planWherePredicate(aliases, sources, orderSpecs, normalizedWhere, joinHint, predicateMode, predicateOrMode)
      : this.planBranch(aliases, sources, orderSpecs, undefined, joinHint);

    const withSortAndLimit = this.applySortAndLimit(base, orderSpecs, projection.limit, projection.offset);
    const planned = this.applyProjection(withSortAndLimit, projection.select as Record<string, any> | undefined, aliases);
    this.params = undefined;
    return planned;
  }

  private planWherePredicate(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    predicate: Predicate,
    joinHint: JoinStrategy,
    predicateMode: PredicateMode,
    predicateOrMode: PredicateOrMode
  ): ExecutionNode {
    // Respect mode: keep predicate structure (no OR rewrites).
    if (predicateMode === PredicateMode.Respect) {
      return this.planBranch(aliases, sources, orderSpecs, predicate, joinHint);
    }

    const dnf = toDNF(predicate);
    const disjuncts = dnf.type === 'OR' ? dnf.conditions : [dnf];
    if (disjuncts.length === 1) {
      return this.planBranch(aliases, sources, orderSpecs, disjuncts[0], joinHint);
    }

    const { common, remainders } = this.extractGlobalCommonFactor(disjuncts);
    const unionPlan = this.planUnionOfDisjuncts(aliases, sources, orderSpecs, disjuncts, joinHint);
    const commonPlan = this.planCommonFactorWithResidualOr(aliases, sources, orderSpecs, common, remainders, joinHint);

    switch (predicateOrMode) {
      case PredicateOrMode.Union:
        return unionPlan;
      case PredicateOrMode.SingleScan:
        return commonPlan;
      case PredicateOrMode.Auto:
        return this.chooseBestOrPlan(aliases, sources, orderSpecs, disjuncts, common, remainders, unionPlan, commonPlan);
      default:
        predicateOrMode satisfies never;
        throw new Error(`Unexpected predicateOrMode: ${predicateOrMode}`);
    }
  }

  private planBranch(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    wherePredicate: Predicate | undefined,
    joinHint: JoinStrategy
  ): ExecutionNode {
    const split = wherePredicate
      ? this.predicateSplitter.split(wherePredicate, aliases)
      : { sourcePredicates: {}, joinPredicates: [], residualPredicates: [] };

    const scanPlan = this.buildScanNodes(aliases, sources, split.sourcePredicates, orderSpecs);
    const joinOrder = this.rankJoinOrder(aliases, scanPlan.costs);

    const rootJoin = this.buildJoins(
      joinOrder,
      aliases,
      scanPlan.nodes,
      split.joinPredicates,
      joinHint
    );

    return this.applyResidualFilters(rootJoin, split.residualPredicates);
  }

  private extractGlobalCommonFactor(disjuncts: Predicate[]): { common: Predicate | undefined; remainders: Predicate[] } {
    const normalized = disjuncts.map(d => simplifyPredicate(d));
    const conjunctLists = normalized.map(d => this.getConjuncts(d));

    const first = conjunctLists[0];
    const firstMap = new Map<string, Predicate>();
    for (const c of first) {
      firstMap.set(this.predicateKey(c), c);
    }

    const commonKeys = new Set<string>(firstMap.keys());
    for (let i = 1; i < conjunctLists.length; i++) {
      const keys = new Set(conjunctLists[i].map(c => this.predicateKey(c)));
      for (const k of Array.from(commonKeys)) {
        if (!keys.has(k)) commonKeys.delete(k);
      }
    }

    const commonConjuncts = Array.from(commonKeys).map(k => firstMap.get(k)!).map(simplifyPredicate);
    const common = this.andOf(commonConjuncts);

    const remainders = conjunctLists.map(list => {
      const remaining = list
        .filter(c => !commonKeys.has(this.predicateKey(c)))
        .map(simplifyPredicate);
      return this.andOf(remaining) ?? ({ type: 'CONSTANT', value: true } as Predicate);
    }).map(simplifyPredicate);

    return { common, remainders };
  }

  private planUnionOfDisjuncts(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    disjuncts: Predicate[],
    joinHint: JoinStrategy
  ): ExecutionNode {
    const inputs = disjuncts.map(d => this.planBranch(aliases, sources, orderSpecs, d, joinHint));
    return {
      type: NodeType.UNION,
      inputs,
      distinct: UnionDistinctStrategy.DocPath,
    } as UnionNode;
  }

  private planCommonFactorWithResidualOr(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    common: Predicate | undefined,
    remainders: Predicate[],
    joinHint: JoinStrategy
  ): ExecutionNode {
    const basePredicate = common && !(common.type === 'CONSTANT' && common.value === true) ? common : undefined;
    const base = this.planBranch(aliases, sources, orderSpecs, basePredicate, joinHint);

    const residualOr = simplifyPredicate(this.orOf(remainders));
    if (residualOr.type === 'CONSTANT' && residualOr.value === true) {
      return base;
    }

    return {
      type: NodeType.FILTER,
      source: base,
      predicate: residualOr,
    } as FilterNode;
  }

  private chooseBestOrPlan(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    disjuncts: Predicate[],
    common: Predicate | undefined,
    remainders: Predicate[],
    unionPlan: ExecutionNode,
    commonPlan: ExecutionNode
  ): ExecutionNode {
    const unionCost = this.estimateUnionCost(aliases, sources, orderSpecs, disjuncts);
    const commonCost = this.estimateCommonCost(aliases, sources, orderSpecs, common, remainders);
    return unionCost < commonCost ? unionPlan : commonPlan;
  }

  private estimateUnionCost(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    disjuncts: Predicate[]
  ): number {
    let total = 0;
    for (const d of disjuncts) {
      total += this.estimateConjunctivePlanCost(aliases, sources, orderSpecs, d);
    }
    const branches = disjuncts.length;
    const hasJoin = aliases.length > 1;
    // Penalize UNION dedupe and (for joins) duplicated join work.
    total += (branches - 1) * (hasJoin ? 500 : 50);
    return total;
  }

  private estimateCommonCost(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    common: Predicate | undefined,
    remainders: Predicate[]
  ): number {
    const basePredicate = common && !(common.type === 'CONSTANT' && common.value === true) ? common : undefined;
    let total = this.estimateConjunctivePlanCost(aliases, sources, orderSpecs, basePredicate);
    // Residual OR filter is evaluated in-memory; approximate its cost by number of branches.
    total += remainders.length * 10;
    return total;
  }

  private estimateConjunctivePlanCost(
    aliases: string[],
    sources: Record<string, any>,
    orderSpecs: OrderBySpec[],
    wherePredicate: Predicate | undefined
  ): number {
    const split = wherePredicate
      ? this.predicateSplitter.split(wherePredicate, aliases)
      : { sourcePredicates: {}, joinPredicates: [], residualPredicates: [] };

    let total = 0;
    for (const alias of aliases) {
      const source = sources[alias] as Collection;
      const { path, collectionGroup } = this.resolveCollectionPath(source);
      const pred = split.sourcePredicates[alias];
      const { score } = this.planSingleScan(alias, path, collectionGroup, pred, orderSpecs.filter(o => o.field.source === alias));
      total += score;
    }
    return total;
  }

  private getConjuncts(predicate: Predicate): Predicate[] {
    const simplified = simplifyPredicate(predicate);
    if (simplified.type === 'AND') return simplified.conditions;
    if (simplified.type === 'CONSTANT' && simplified.value === true) return [];
    return [simplified];
  }

  private andOf(conditions: Predicate[]): Predicate | undefined {
    const filtered = conditions.filter(p => !(p.type === 'CONSTANT' && p.value === true));
    if (filtered.length === 0) return undefined;
    if (filtered.length === 1) return filtered[0];
    return { type: 'AND', conditions: filtered } as Predicate;
  }

  private orOf(conditions: Predicate[]): Predicate {
    const filtered = conditions.map(simplifyPredicate);
    if (filtered.length === 0) return { type: 'CONSTANT', value: false } as Predicate;
    if (filtered.length === 1) return filtered[0];
    return { type: 'OR', conditions: filtered } as Predicate;
  }

  private predicateKey(p: Predicate): string {
    const pred = simplifyPredicate(p);
    switch (pred.type) {
      case 'CONSTANT':
        return `C:${pred.value}`;
      case 'COMPARISON':
        return `CMP:${pred.operation}:${this.exprKey(pred.left)}:${this.exprKey(pred.right)}`;
      case 'NOT':
        return `NOT:${this.predicateKey(pred.operand)}`;
      case 'AND': {
        const parts = pred.conditions.map(c => this.predicateKey(c)).sort();
        return `AND:${parts.join('|')}`;
      }
      case 'OR': {
        const parts = pred.conditions.map(c => this.predicateKey(c)).sort();
        return `OR:${parts.join('|')}`;
      }
      default:
        pred satisfies never;
        return 'UNKNOWN';
    }
  }

  private exprKey(e: any): string {
    if (Array.isArray(e)) {
      const parts = e.map(item => this.exprKey(item));
      return `A:${parts.join('|')}`;
    }

    if (!e || typeof e !== 'object') return `X:${String(e)}`;
    switch (e.kind) {
      case 'Field':
        return `F:${e.source}.${e.path.join('.')}`;
      case 'Literal':
        return `L:${e.type}:${JSON.stringify(e.value)}`;
      case 'Param':
        return `P:${e.name}`;
      default:
        return `X:${JSON.stringify(e)}`;
    }
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

      const { node, score } = this.planSingleScan(
        alias,
        path,
        collectionGroup,
        predicate,
        orderBy.filter(o => o.field.source === alias)
      );
      scans[alias] = node;
      costs[alias] = predicate ? score : Infinity;
    }

    return { nodes: scans, costs };
  }

  private planSingleScan(
    alias: string,
    path: string,
    collectionGroup: boolean | undefined,
    predicate: Predicate | undefined,
    orderBy: OrderBySpec[]
  ): { node: ExecutionNode; score: number } {
    if (!predicate) {
      return { node: this.createScanNode(alias, path, collectionGroup, []), score: 1000 };
    }

    const constraints: Constraint[] = [];
    const nonIndexable = this.extractConstraints(predicate, constraints);
    const removedMembership = this.enforceMembershipConstraintLimit(constraints);
    const totalNonIndexable = nonIndexable + removedMembership;
    this.validateFirestoreGuardrails(constraints, orderBy);
    const score = this.scoreConstraints(path, constraints, orderBy) + totalNonIndexable * 100;

    const base = this.createScanNode(alias, path, collectionGroup, constraints);
    const node = totalNonIndexable > 0 ? this.wrapFilter(base, predicate) : base;
    return { node, score };
  }

  private scoreConstraints(collectionPath: string, constraints: Constraint[], orderBy?: OrderBySpec[]): number {
    const sortOrder = this.toSortOrder(orderBy);
    const match = this.indexManager.match(collectionPath, constraints, sortOrder);

    if (match.type === 'exact') {
      return 1;
    }
    if (match.type === 'partial') {
      return Math.max(1, 10 - match.matchedFields) + 5;
    }
    return 1000;
  }

  private toSortOrder(orderBy?: OrderBySpec[]): SortOrder | undefined {
    if (!orderBy || orderBy.length === 0) return undefined;
    const primary = orderBy[0];
    return {
      field: primary.field.path.join('.'),
      direction: primary.direction,
    };
  }

  private extractConstraints(predicate: Predicate, constraints: Constraint[]): number {
    let nonIndexable = 0;

    if (predicate.type === 'AND') {
      for (const c of predicate.conditions) {
        nonIndexable += this.extractConstraints(c, constraints);
      }
      return nonIndexable;
    }

    if (predicate.type === 'COMPARISON') {
      const field = this.asField(predicate.left);
      const literal = !Array.isArray(predicate.right) && predicate.right.kind === 'Literal'
        ? predicate.right
        : null;
      const literalList = Array.isArray(predicate.right) && predicate.right.every(item => item.kind === 'Literal')
        ? predicate.right
        : null;

      if (field && (literal || literalList)) {
        constraints.push({
          field,
          op: predicate.operation,
          value: literal ?? literalList!,
        });
      } else {
        nonIndexable += 1;
      }
      return nonIndexable;
    }

    if (predicate.type === 'OR' || predicate.type === 'NOT') {
      return 1;
    }

    return 0;
  }

  /**
   * Firestore allows at most one of IN / NOT_IN / ARRAY_CONTAINS_ANY per query.
   * Prefer keeping IN (most selective), then ARRAY_CONTAINS_ANY, then NOT_IN.
   * Removed constraints are still applied via post-fetch filtering.
   *
   * @returns number of constraints removed from pushdown.
   */
  private enforceMembershipConstraintLimit(constraints: Constraint[]): number {
    const membershipOps: Constraint['op'][] = ['in', 'not-in', 'array-contains-any'];
    const membership = constraints.filter(c => membershipOps.includes(c.op));
    if (membership.length <= 1) return 0;

    const priority = (op: Constraint['op']) => {
      switch (op) {
        case 'in':
          return 2;
        case 'array-contains-any':
          return 1;
        case 'not-in':
          return 0;
        default:
          return -1;
      }
    };

    let best = membership[0];
    for (const c of membership) {
      if (priority(c.op) > priority(best.op)) {
        best = c;
      }
    }

    const kept = new Set<Constraint>([best]);
    const filtered: Constraint[] = [];
    for (const c of constraints) {
      if (membershipOps.includes(c.op)) {
        if (kept.has(c)) {
          filtered.push(c);
        }
      } else {
        filtered.push(c);
      }
    }

    const removed = constraints.length - filtered.length;
    constraints.length = 0;
    constraints.push(...filtered);
    return removed;
  }

  private asField(expr: any): Field | null {
    if (expr && typeof expr === 'object' && expr.kind === 'Field' && expr.source) {
      return expr as Field;
    }
    return null;
  }

  private validateFirestoreGuardrails(constraints: Constraint[], orderBy?: OrderBySpec[]) {
    const inequalityOps = new Set<Constraint['op']>(['<', '<=', '>', '>=', '!=', 'not-in']);
    const inequalityFields = new Set<string>();

    for (const c of constraints) {
      if (inequalityOps.has(c.op)) {
        inequalityFields.add(c.field.path.join('.'));
      }
    }

    if (inequalityFields.size > 1) {
      throw new Error(`Firestore allows at most one inequality field per query (found: ${Array.from(inequalityFields).join(', ')}).`);
    }

    if (inequalityFields.size === 1 && orderBy && orderBy.length > 0) {
      const inequalityField = Array.from(inequalityFields)[0];
      const orderField = orderBy[0].field.path.join('.');
      if (orderField !== inequalityField) {
        throw new Error('When using an inequality filter, the first orderBy field must match the inequality field.');
      }
    }
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

      // If we have a simple binary predicate, normalize orientation so the LEFT operand
      // comes from the left subtree and the RIGHT operand from the right subtree.
      // This is required for merge join correctness (it does not re-orient dynamically).
      const leftAliases = this.collectNodeAliases(root);
      const rightAliases = this.collectNodeAliases(rightNode);
      const orientedCondition = this.orientJoinPredicateForSides(joinCondition, leftAliases, rightAliases);

      const joinNode: JoinNode = {
        type: NodeType.JOIN,
        left: root,
        right: rightNode,
        joinType: this.resolveJoinStrategy(orientedCondition, hint, root, rightNode),
        condition: simplifyPredicate(orientedCondition),
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

  private resolveJoinStrategy(condition: Predicate, hint: JoinStrategy, left: ExecutionNode, right: ExecutionNode): JoinStrategy {
    if (hint && hint !== JoinStrategy.Auto) {
      this.assertJoinHintCompatibility(hint, condition);
      return hint;
    }

    if (isHashJoinCompatible(condition)) {
      return JoinStrategy.Hash;
    }

    if (isMergeJoinCompatible(condition) && this.canUseMergeJoinWithoutSorting(condition, left, right)) {
      return JoinStrategy.Merge;
    }

    if (this.canUseIndexedNestedLoop(condition, left, right)) {
      return JoinStrategy.IndexedNestedLoop;
    }

    return JoinStrategy.NestedLoop;
  }

  /**
   * Indexed nested-loop join is chosen automatically as a fallback when:
   * - Hash and merge joins were not selected, AND
   * - We can perform an indexed lookup on the RIGHT side using a Firestore-supported membership query
   *   (`in` / `array-contains-any`) on a single join field backed by a known index.
   *
   * Note: the operator still evaluates the full join predicate in-memory, so this is safe even if the join
   * condition is a conjunction of multiple predicates; we only need ONE usable lookup predicate to reduce
   * the right-side search space.
   */
  private canUseIndexedNestedLoop(condition: Predicate, _left: ExecutionNode, right: ExecutionNode): boolean {
    const scan = this.findUnderlyingScan(right);
    if (!scan) return false;

    // Choose any feasible lookup plan that is backed by a known index on the RIGHT scan.
    return !!pickIndexedNestedLoopLookupPlan(condition, scan, this.indexManager, { requireIndex: true });
  }

  private collectComparisonPredicates(predicate: Predicate): Array<{ left: any; right: any; operation: any }> {
    switch (predicate.type) {
      case 'COMPARISON':
        return [predicate];
      case 'AND':
        return predicate.conditions.flatMap(p => this.collectComparisonPredicates(p));
      default:
        return [];
    }
  }

  /**
   * Merge-join is only chosen automatically when its required ordering is already satisfied
   * (so it won't introduce extra sorting work).
   *
   * We currently treat ordering as satisfied when:
   * - The left input is already sorted by the left join key (ASC), OR it is a single scan that can be ordered
   *   by that key using a known Firestore index (exact match).
   * - Same for the right input.
   */
  private canUseMergeJoinWithoutSorting(condition: Predicate, left: ExecutionNode, right: ExecutionNode): boolean {
    if (condition.type !== 'COMPARISON') return false;

    const leftField = this.asField(condition.left);
    const rightField = this.asField(condition.right);
    if (!leftField || !rightField) return false;

    const plannedLeft = this.planEnsureSortedBy(left, leftField, 'asc');
    if (!plannedLeft.ok) return false;
    const plannedRight = this.planEnsureSortedBy(right, rightField, 'asc');
    if (!plannedRight.ok) return false;

    // Commit scan orderBy updates only if BOTH sides can satisfy the requirement.
    plannedLeft.apply?.();
    plannedRight.apply?.();
    return true;
  }

  private planEnsureSortedBy(
    node: ExecutionNode,
    field: Field,
    direction: SortOrder['direction']
  ): { ok: boolean; apply?: () => void } {
    const current = this.getPlannedSortOrder(node);
    if (this.sortOrderMatches(current, field, direction)) {
      return { ok: true };
    }

    const scan = this.findUnderlyingScan(node);
    if (!scan) return { ok: false };

    // If the scan already has an orderBy and it doesn't match, don't silently override it.
    if (scan.orderBy && scan.orderBy.length > 0) {
      const primary = scan.orderBy[0];
      const currentKey = `${scan.alias}.${primary.field.path.join('.')}`;
      const expectedKey = `${field.source}.${field.path.join('.')}`;
      if (currentKey === expectedKey && primary.direction === direction) {
        return { ok: true };
      }
      return { ok: false };
    }

    // IndexManager operates on unqualified field paths (Firestore-level).
    const match = this.indexManager.match(
      scan.collectionPath,
      scan.constraints,
      { field: field.path.join('.'), direction }
    );
    if (match.type !== 'exact') return { ok: false };

    return {
      ok: true,
      apply: () => {
        scan.orderBy = [{
          field: new Field(scan.alias, field.path),
          direction,
        }];
      },
    };
  }

  private sortOrderMatches(order: SortOrder | undefined, field: Field, direction: SortOrder['direction']): boolean {
    if (!order) return false;
    const expectedKey = `${field.source}.${field.path.join('.')}`;
    return order.field === expectedKey && order.direction === direction;
  }

  private findUnderlyingScan(node: ExecutionNode): ScanNode | null {
    switch (node.type) {
      case NodeType.SCAN:
        return node as ScanNode;
      case NodeType.FILTER:
        return this.findUnderlyingScan((node as FilterNode).source);
      case NodeType.PROJECT:
        return this.findUnderlyingScan((node as ProjectNode).source);
      case NodeType.LIMIT:
        return this.findUnderlyingScan((node as LimitNode).source);
      default:
        return null;
    }
  }

  private collectNodeAliases(node: ExecutionNode): Set<string> {
    const out = new Set<string>();
    traverseExecutionNode(node, n => {
      if (n.type === NodeType.SCAN) {
        out.add((n as ScanNode).alias);
      }
    });
    return out;
  }

  private getPlannedSortOrder(node: ExecutionNode): SortOrder | undefined {
    switch (node.type) {
      case NodeType.SCAN: {
        const scan = node as ScanNode;
        const primary = scan.orderBy?.[0];
        if (!primary) return undefined;
        return {
          field: `${scan.alias}.${primary.field.path.join('.')}`,
          direction: primary.direction,
        };
      }
      case NodeType.FILTER:
        return this.getPlannedSortOrder((node as FilterNode).source);
      case NodeType.PROJECT:
        return this.getPlannedSortOrder((node as ProjectNode).source);
      case NodeType.LIMIT:
        return this.getPlannedSortOrder((node as LimitNode).source);
      case NodeType.SORT: {
        const sort = node as SortNode;
        const primary = sort.orderBy[0];
        if (!primary) return undefined;
        return {
          field: `${primary.field.source}.${primary.field.path.join('.')}`,
          direction: primary.direction,
        };
      }
      case NodeType.JOIN: {
        const join = node as JoinNode;

        // Merge join outputs rows sorted by the join key (ASC), regardless of input ordering.
        if (join.joinType === JoinStrategy.Merge) {
          if (join.condition.type !== 'COMPARISON') return undefined;
          const leftField = this.asField(join.condition.left);
          if (!leftField) return undefined;
          return { field: `${leftField.source}.${leftField.path.join('.')}`, direction: 'asc' };
        }

        // Hash and nested-loop joins preserve the order of the LEFT input stream.
        return this.getPlannedSortOrder(join.left);
      }
      default:
        return undefined;
    }
  }

  private orientJoinPredicateForSides(predicate: Predicate, leftAliases: Set<string>, rightAliases: Set<string>): Predicate {
    if (predicate.type !== 'COMPARISON') return predicate;
    if (Array.isArray(predicate.left) || Array.isArray(predicate.right)) return predicate;

    const leftField = this.asField(predicate.left);
    const rightField = this.asField(predicate.right);
    if (!leftField || !rightField) return predicate;

    const leftInLeft = !!leftField.source && leftAliases.has(leftField.source);
    const leftInRight = !!leftField.source && rightAliases.has(leftField.source);
    const rightInLeft = !!rightField.source && leftAliases.has(rightField.source);
    const rightInRight = !!rightField.source && rightAliases.has(rightField.source);

    // Already oriented.
    if (leftInLeft && rightInRight) return predicate;

    // Swapped operands: invert comparator when flipping.
    if (leftInRight && rightInLeft) {
      const inverted = invertComparisonOp(predicate.operation);
      if (!inverted) return predicate;
      return {
        type: 'COMPARISON',
        left: predicate.right,
        right: predicate.left,
        operation: inverted,
      };
    }

    return predicate;
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
      fields[key] = this.normalizeExpression(value, knownAliases);
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
          right: Array.isArray(predicate.right)
            ? predicate.right.map(expr => this.normalizeExpression(expr, aliases))
            : this.normalizeExpression(predicate.right, aliases),
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
        predicate satisfies never;
        throw new Error(`Unexpected predicate type: ${predicate}`);
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
    return literal(value);
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

  private toLiteralValue(value: Literal | number | boolean | null): Literal {
    return value instanceof Literal ? value : literal(value);
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
        if (!this.hasIndexedNestedLoopPredicate(condition)) {
          throw new Error('Indexed nested-loop join requires a conjunctive join predicate containing Field-vs-Field comparisons.');
        }
        return;
      case JoinStrategy.NestedLoop:
      case JoinStrategy.Auto:
        return;
      default:
        hint satisfies never;
        throw new Error(`Unexpected join strategy: ${hint}`);
    }
  }

  private hasIndexedNestedLoopPredicate(condition: Predicate): boolean {
    const comparisons = this.collectComparisonPredicates(condition);
    return comparisons.some(c => {
      return !!this.asField(c.left) && !!this.asField(c.right);
    });
  }
}
