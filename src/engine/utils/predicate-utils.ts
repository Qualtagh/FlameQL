import { ComparisonPredicate, CompositePredicate, ConstantPredicate, Expression, Field, Literal, NotPredicate, Param, Predicate } from '../../api/expression';

/**
 * Simplifies a predicate by applying logical rules:
 * - Single element lists → replace with element
 * - Nested AND/OR → flatten to single list
 * - De Morgan's laws for NOT
 * - Eliminate always TRUE/FALSE conditions
 * - Reduce to disjunctive normal form (DNF)
 */
export function simplifyPredicate(predicate: Predicate): Predicate {
  // First pass: recursive simplification
  const simplified = simplifyOnce(predicate);

  // If nothing changed, we're done
  if (predicatesEqual(simplified, predicate)) {
    return simplified;
  }

  // Otherwise, recursively simplify again
  return simplifyPredicate(simplified);
}

function simplifyOnce(predicate: Predicate): Predicate {
  switch (predicate.type) {
    case 'CONSTANT':
    case 'COMPARISON':
      return predicate;

    case 'NOT':
      return simplifyNot(predicate);

    case 'AND':
    case 'OR':
      return simplifyComposite(predicate);
  }
}

function simplifyNot(predicate: NotPredicate): Predicate {
  const operand = simplifyOnce(predicate.operand);

  // !!A => A
  if (operand.type === 'NOT') {
    return operand.operand;
  }

  // !TRUE => FALSE, !FALSE => TRUE
  if (operand.type === 'CONSTANT') {
    return { type: 'CONSTANT', value: !operand.value };
  }

  // De Morgan's laws: !(A AND B) => !A OR !B
  if (operand.type === 'AND') {
    return {
      type: 'OR',
      conditions: operand.conditions.map(c => simplifyOnce({ type: 'NOT', operand: c })),
    };
  }

  // De Morgan's laws: !(A OR B) => !A AND !B
  if (operand.type === 'OR') {
    return {
      type: 'AND',
      conditions: operand.conditions.map(c => simplifyOnce({ type: 'NOT', operand: c })),
    };
  }

  return { type: 'NOT', operand };
}

function simplifyComposite(predicate: CompositePredicate): Predicate {
  const type = predicate.type;
  const identityValue = type === 'AND'; // TRUE for AND, FALSE for OR
  const absorbingValue = !identityValue; // FALSE for AND, TRUE for OR

  // Recursively simplify all conditions
  let conditions = predicate.conditions.map(simplifyOnce);

  // Flatten nested same-type operations: (A AND B) AND C => A AND B AND C
  conditions = conditions.flatMap(c => {
    if (c.type === type) {
      return c.conditions;
    }
    return [c];
  });

  // Remove identity values: A AND TRUE => A, A OR FALSE => A
  conditions = conditions.filter(c => {
    if (c.type === 'CONSTANT') {
      return c.value !== identityValue;
    }
    return true;
  });

  // Remove duplicates: A AND A => A, A OR A => A
  const unique: Predicate[] = [];
  for (const cond of conditions) {
    if (!unique.some(u => predicatesEqual(u, cond))) {
      unique.push(cond);
    }
  }
  conditions = unique;

  // Check for absorbing element: A AND FALSE => FALSE, A OR TRUE => TRUE
  if (conditions.some(c => c.type === 'CONSTANT' && c.value === absorbingValue)) {
    return { type: 'CONSTANT', value: absorbingValue };
  }

  // Check for contradictions and tautologies
  if (type === 'AND') {
    // A && !A => FALSE
    for (let i = 0; i < conditions.length; i++) {
      for (let j = i + 1; j < conditions.length; j++) {
        if (isNegationOf(conditions[i], conditions[j])) {
          return { type: 'CONSTANT', value: false };
        }
      }
    }
  } else if (type === 'OR') {
    // A || !A => TRUE
    for (let i = 0; i < conditions.length; i++) {
      for (let j = i + 1; j < conditions.length; j++) {
        if (isNegationOf(conditions[i], conditions[j])) {
          return { type: 'CONSTANT', value: true };
        }
      }
    }
  }

  // Absorption for OR: remove conjunctions that are supersets of others
  // E.g., (A && !C) || (A && B && !C) => (A && !C)
  if (type === 'OR') {
    const filtered: Predicate[] = [];
    for (const cond of conditions) {
      // Check if this condition is absorbed by any condition already in filtered
      const isAbsorbed = filtered.some(f => absorbsCondition(f, cond));
      if (!isAbsorbed) {
        // Remove any conditions from filtered that are absorbed by this one
        const notAbsorbedByThis = filtered.filter(f => !absorbsCondition(cond, f));
        filtered.length = 0;
        filtered.push(...notAbsorbedByThis, cond);
      }
    }
    conditions = filtered;
  }

  // Single element => unwrap
  if (conditions.length === 0) {
    return { type: 'CONSTANT', value: identityValue };
  }

  if (conditions.length === 1) {
    return conditions[0];
  }

  return { type, conditions };
}

/**
 * Check if predicate a is the negation of predicate b (or vice versa)
 */
function isNegationOf(a: Predicate, b: Predicate): boolean {
  if (a.type === 'NOT') {
    return predicatesEqual(a.operand, b);
  }
  if (b.type === 'NOT') {
    return predicatesEqual(b.operand, a);
  }
  return false;
}

/**
 * Check if predicate a absorbs predicate b in a disjunction.
 * This happens when a is a subset of b (in AND terms).
 * E.g., (A && !C) absorbs (A && B && !C) because if (A && !C) is true,
 * then (A && B && !C) might also be true, making it redundant.
 */
function absorbsCondition(a: Predicate, b: Predicate): boolean {
  // Extract conjuncts (AND terms) from each predicate
  const aConjuncts = getConjuncts(a);
  const bConjuncts = getConjuncts(b);

  // a absorbs b if all of a's conjuncts are in b's conjuncts
  return aConjuncts.every(ac =>
    bConjuncts.some(bc => predicatesEqual(ac, bc))
  );
}

/**
 * Get the list of conjuncts (AND terms) from a predicate
 */
function getConjuncts(predicate: Predicate): Predicate[] {
  if (predicate.type === 'AND') {
    return predicate.conditions;
  }
  return [predicate];
}

/**
 * Converts a predicate to Disjunctive Normal Form (OR of ANDs).
 * DNF has maximum depth of 2: top level is OR, each element is either AND or atomic.
 */
export function toDNF(predicate: Predicate): Predicate {
  const simplified = simplifyPredicate(predicate);
  return toDNFInternal(simplified);
}

function toDNFInternal(predicate: Predicate): Predicate {
  if (predicate.type === 'CONSTANT' || predicate.type === 'COMPARISON') {
    return predicate;
  }

  if (predicate.type === 'NOT') {
    // Push NOT down to comparisons
    return simplifyPredicate(predicate);
  }

  if (predicate.type === 'OR') {
    // OR is already at the top level, just ensure children are in CNF-like form
    const conditions = predicate.conditions.map(toDNFInternal);

    // Flatten nested ORs
    const flattened = conditions.flatMap(c => {
      if (c.type === 'OR') {
        return c.conditions;
      }
      return [c];
    });

    return simplifyComposite({ type: 'OR', conditions: flattened });
  }

  // AND case - need to distribute over OR if present
  if (predicate.type === 'AND') {
    const conditions = predicate.conditions.map(toDNFInternal);

    // Find first OR among conditions
    const orIndex = conditions.findIndex(c => c.type === 'OR');

    if (orIndex === -1) {
      // No OR found, already in DNF
      return simplifyComposite({ type: 'AND', conditions });
    }

    // Distribute: (A AND (B OR C)) => (A AND B) OR (A AND C)
    const orCondition = conditions[orIndex] as CompositePredicate;
    const otherConditions = conditions.filter((_, i) => i !== orIndex);

    const distributed = orCondition.conditions.map(orChild => {
      return toDNFInternal({
        type: 'AND',
        conditions: [...otherConditions, orChild],
      });
    });

    return toDNFInternal({
      type: 'OR',
      conditions: distributed,
    });
  }

  return predicate;
}

function predicatesEqual(a: Predicate, b: Predicate): boolean {
  if (a.type !== b.type) return false;

  switch (a.type) {
    case 'CONSTANT':
      return (b as ConstantPredicate).value === a.value;
    case 'COMPARISON': {
      const bComp = b as ComparisonPredicate;
      return expressionsEqual(a.left, bComp.left) &&
        expressionsEqual(a.right, bComp.right) &&
        a.operation === bComp.operation;
    }
    case 'NOT':
      return predicatesEqual(a.operand, (b as NotPredicate).operand);
    case 'AND':
    case 'OR': {
      const bComposite = b as CompositePredicate;
      if (a.conditions.length !== bComposite.conditions.length) return false;
      return a.conditions.every((c, i) => predicatesEqual(c, bComposite.conditions[i]));
    }
  }
}

function expressionsEqual(a: Expression, b: Expression): boolean {
  if (a === b) return true;
  if (a.kind !== b.kind) return false;

  switch (a.kind) {
    case 'Field': {
      const fieldA = a;
      const fieldB = b as Field;
      return fieldA.source === fieldB.source && arrayEquals(fieldA.path, fieldB.path);
    }
    case 'Literal': {
      const litA = a;
      const litB = b as Literal;
      return litA.type === litB.type && litA.value === litB.value;
    }
    case 'Param': {
      const paramA = a;
      const paramB = b as Param;
      return paramA.name === paramB.name;
    }
  }
}

function arrayEquals(left: unknown[], right: unknown[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, idx) => value === right[idx]);
}
