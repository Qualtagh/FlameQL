import { Predicate } from './ast';

export interface SplitPredicates {
  sourcePredicates: Record<string, Predicate>;
  joinPredicates: Predicate[];
  residualPredicates: Predicate[];
}

export class PredicateSplitter {
  split(predicate: Predicate, sources: string[]): SplitPredicates {
    const conjuncts = this.getConjuncts(predicate);

    const result: SplitPredicates = {
      sourcePredicates: {},
      joinPredicates: [],
      residualPredicates: [],
    };

    for (const conjunct of conjuncts) {
      const involvedSources = this.getInvolvedSources(conjunct, sources);

      if (involvedSources.length === 0) {
        // Constant or unknown. Treat as residual if not TRUE.
        if (conjunct.type !== 'CONSTANT' || conjunct.value !== true) {
          result.residualPredicates.push(conjunct);
        }
      } else if (involvedSources.length === 1) {
        const source = involvedSources[0];
        const existing = result.sourcePredicates[source];
        if (existing) {
          result.sourcePredicates[source] = { type: 'AND', conditions: [existing, conjunct] };
        } else {
          result.sourcePredicates[source] = conjunct;
        }
      } else {
        // Involves multiple sources -> Join condition or Residual
        result.joinPredicates.push(conjunct);
      }
    }

    return result;
  }

  private getConjuncts(predicate: Predicate): Predicate[] {
    if (predicate.type === 'AND') {
      return predicate.conditions;
    }
    return [predicate];
  }

  public getInvolvedSources(predicate: Predicate, sources: string[]): string[] {
    const involved = new Set<string>();
    this.collectSources(predicate, sources, involved);
    return Array.from(involved);
  }

  private collectSources(predicate: Predicate, sources: string[], involved: Set<string>) {
    if (predicate.type === 'COMPARISON') {
      this.checkField(predicate.left, sources, involved);
      // Check right side if it's a field?
      // Currently AST assumes right is literal, but for joins it might be field.
      // If right is string and looks like field path?
      // The current AST definition says right is 'any'.
      // But typically for joins we might have a special structure or just string.
      // If it's a string, we check if it matches an alias.
      if (typeof predicate.right === 'string') {
        this.checkField(predicate.right, sources, involved);
      }
    } else if (predicate.type === 'AND' || predicate.type === 'OR') {
      for (const condition of predicate.conditions) {
        this.collectSources(condition, sources, involved);
      }
    } else if (predicate.type === 'NOT') {
      this.collectSources(predicate.operand, sources, involved);
    }
  }

  private checkField(fieldPath: string, sources: string[], involved: Set<string>) {
    const parts = fieldPath.split('.');
    const potentialAlias = parts[0];
    if (sources.includes(potentialAlias)) {
      involved.add(potentialAlias);
    }
  }
}
