import { CompositePredicate, Expression, Field, Predicate } from '../api/expression';
import { simplifyPredicate } from './utils/predicate-utils';

export interface SplitPredicates {
  sourcePredicates: Record<string, Predicate>;
  joinPredicates: Predicate[];
  residualPredicates: Predicate[];
}

export class PredicateSplitter {
  split(predicate: Predicate, sources: string[]): SplitPredicates {
    const normalized = simplifyPredicate(predicate);
    const conjuncts = this.getConjuncts(normalized);

    const result: SplitPredicates = {
      sourcePredicates: {},
      joinPredicates: [],
      residualPredicates: [],
    };

    for (const conjunct of conjuncts) {
      const involvedSources = Array.from(this.getInvolvedSources(conjunct, sources));

      if (involvedSources.length === 0) {
        if (conjunct.type !== 'CONSTANT' || conjunct.value !== true) {
          result.residualPredicates.push(conjunct);
        }
        continue;
      }

      if (involvedSources.length === 1) {
        const source = involvedSources[0];
        const existing = result.sourcePredicates[source];
        if (existing && existing.type === 'AND') {
          (result.sourcePredicates[source] as CompositePredicate).conditions.push(conjunct);
        } else if (existing) {
          result.sourcePredicates[source] = { type: 'AND', conditions: [existing, conjunct] };
        } else {
          result.sourcePredicates[source] = conjunct;
        }
        continue;
      }

      if (involvedSources.length === 2) {
        result.joinPredicates.push(conjunct);
        continue;
      }

      result.residualPredicates.push(conjunct);
    }

    return result;
  }

  private getConjuncts(predicate: Predicate): Predicate[] {
    if (predicate.type === 'AND') {
      return predicate.conditions;
    }
    return [predicate];
  }

  public getInvolvedSources(predicate: Predicate, sources: string[]): Set<string> {
    const involved = new Set<string>();
    this.collectSources(predicate, sources, involved);
    return involved;
  }

  private collectSources(predicate: Predicate, sources: string[], involved: Set<string>) {
    if (predicate.type === 'COMPARISON') {
      this.collectFromExpression(predicate.left, sources, involved);
      this.collectFromExpression(predicate.right, sources, involved);
      return;
    }

    if (predicate.type === 'AND' || predicate.type === 'OR') {
      predicate.conditions.forEach(cond => this.collectSources(cond, sources, involved));
      return;
    }

    if (predicate.type === 'NOT') {
      this.collectSources(predicate.operand, sources, involved);
    }
  }

  private collectFromExpression(expr: Expression | Expression[], sources: string[], involved: Set<string>) {
    if (Array.isArray(expr)) {
      expr.forEach(item => this.collectFromExpression(item, sources, involved));
      return;
    }

    if (expr.kind === 'Field') {
      this.assertKnownAlias(expr, sources);
      if (expr.source) {
        involved.add(expr.source);
      }
      return;
    }
  }

  private assertKnownAlias(ref: Field, sources: string[]) {
    if (!ref.source || !sources.includes(ref.source)) {
      throw new Error(`Unknown alias "${ref.source}" referenced in predicate.`);
    }
  }
}
