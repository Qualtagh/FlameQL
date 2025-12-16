import { WhereFilterOp } from '@google-cloud/firestore';
import { and, arrayContains, arrayContainsAny, constant, eq, gt, gte, inList, lt, lte, ne, not, notInList, or } from '../../api/api';
import { ComparisonPredicate, CompositePredicate, ConstantPredicate, Expression, Field, Literal, NotPredicate, Param, Predicate } from '../../api/expression';
import { createOperationComparator, invertComparisonOp } from './operation-comparator';

const IN_LIST_MAX = 30;
const NOT_IN_LIST_MAX = 30;
const ARRAY_CONTAINS_ANY_MAX = 10;

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
      return predicate;
    case 'COMPARISON':
      return simplifyComparison(predicate);
    case 'NOT':
      return simplifyNot(predicate);
    case 'AND':
    case 'OR':
      return simplifyComposite(predicate);
    default:
      predicate satisfies never;
      throw new Error(`Unsupported predicate type: ${predicate}`);
  }
}

function simplifyComparison(predicate: ComparisonPredicate): Predicate {
  let cmp = normalizeComparisonOperands(predicate);

  // Literal folding (incl. list comparisons)
  const folded = evaluateComparisonIfLiteral(cmp);
  if (folded) {
    return folded;
  }

  // Right-side list handling
  if (Array.isArray(cmp.right)) {
    const { fields, others } = partitionListByField(cmp.right);

    if (cmp.operation === 'array-contains-any' && fields.length > 0) {
      throw new Error('array-contains-any with a Field on the right side is unsupported without schema information.');
    }

    if (cmp.operation === 'in' && fields.length > 0) {
      const parts: Predicate[] = [];
      if (others.length > 0) {
        parts.push(simplifyComparison({ ...cmp, right: dedupeExpressions(others) }));
      }
      for (const f of fields) {
        parts.push(eq(cmp.left, f));
      }
      return simplifyComposite(or(parts));
    }

    if (cmp.operation === 'not-in' && fields.length > 0) {
      const parts: Predicate[] = [];
      if (others.length > 0) {
        parts.push(simplifyComparison({ ...cmp, right: dedupeExpressions(others) }));
      }
      for (const f of fields) {
        parts.push(ne(cmp.left, f));
      }
      return simplifyComposite(and(parts));
    }

    cmp = { ...cmp, right: dedupeExpressions(others.length ? others : cmp.right) };
  }

  // Empty list fast-paths
  if (Array.isArray(cmp.right) && cmp.right.length === 0) {
    switch (cmp.operation) {
      case 'in':
        return constant(false);
      case 'not-in':
        return constant(true);
      case 'array-contains-any':
        return constant(false);
    }
  }

  // Single-element list simplifications
  if (Array.isArray(cmp.right) && cmp.right.length === 1) {
    const only = cmp.right[0];
    if (isLiteralExpr(only)) {
      if (cmp.operation === 'in') {
        return simplifyComparison(eq(cmp.left, only));
      }
      if (cmp.operation === 'not-in') {
        return simplifyComparison(ne(cmp.left, only));
      }
      if (cmp.operation === 'array-contains-any') {
        return simplifyComparison(arrayContains(cmp.left, only));
      }
    }
  }

  // Split oversized lists for Firestore limits
  if (Array.isArray(cmp.right)) {
    const limit = getListLimit(cmp.operation);
    if (limit !== undefined && cmp.right.length > limit) {
      return splitListComparison(cmp, limit);
    }
  }

  return cmp;
}

function simplifyNot(predicate: NotPredicate): Predicate {
  const operand = simplifyOnce(predicate.operand);

  // !!A => A
  if (operand.type === 'NOT') {
    return operand.operand;
  }

  // !TRUE => FALSE, !FALSE => TRUE
  if (operand.type === 'CONSTANT') {
    return constant(!operand.value);
  }

  // De Morgan's laws: !(A AND B) => !A OR !B
  if (operand.type === 'AND') {
    return or(operand.conditions.map(c => simplifyOnce(not(c))));
  }

  // De Morgan's laws: !(A OR B) => !A AND !B
  if (operand.type === 'OR') {
    return and(operand.conditions.map(c => simplifyOnce(not(c))));
  }

  return not(operand);
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

  // Merge membership/scalar rules before duplicate removal to enable aggregation
  if (type === 'AND') {
    conditions = mergeAndConditions(conditions);
    if (conditions.length === 1) {
      return conditions[0];
    }
  } else {
    conditions = mergeOrConditions(conditions);
  }

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
    return constant(absorbingValue);
  }

  // Extra OR pair simplification (e.g., not-in vs eq).
  if (type === 'OR') {
    const paired = simplifyOrPairs(conditions);
    if (paired) {
      return paired;
    }
  }

  // Check for contradictions and tautologies
  if (type === 'AND') {
    // A && !A => FALSE
    for (let i = 0; i < conditions.length; i++) {
      for (let j = i + 1; j < conditions.length; j++) {
        if (isNegationOf(conditions[i], conditions[j])) {
          return constant(false);
        }
      }
    }
  } else if (type === 'OR') {
    // A || !A => TRUE
    for (let i = 0; i < conditions.length; i++) {
      for (let j = i + 1; j < conditions.length; j++) {
        if (isNegationOf(conditions[i], conditions[j])) {
          return constant(true);
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
    return constant(identityValue);
  }

  if (conditions.length === 1) {
    return conditions[0];
  }

  return { type, conditions };
}

function simplifyOrPairs(conditions: Predicate[]): Predicate | null {
  if (conditions.length !== 2) return null;
  const [a, b] = conditions;
  if (a.type === 'COMPARISON' && b.type === 'COMPARISON') {
    const merged = simplifyNotInEqPair(a, b);
    if (merged) return merged;
  }
  return null;
}

function mergeOrConditions(conditions: Predicate[]): Predicate[] {
  const result: Predicate[] = [];
  const processed = new Array(conditions.length).fill(false);

  for (let i = 0; i < conditions.length; i++) {
    if (processed[i]) continue;

    const current = conditions[i];
    const { base, membership } = splitBaseAndMembership(current);

    if (!membership) {
      // Special-case not-in combinations (OR).
      if (current.type === 'COMPARISON') {
        const field = asField(current.left);
        const right = current.right;
        if (field && current.operation === 'not-in' && Array.isArray(right) && isLiteralArray(right)) {
          const notInArray = right;
          const peers: number[] = [];
          let merged: Predicate | undefined;
          for (let j = i + 1; j < conditions.length; j++) {
            if (processed[j]) continue;
            const other = conditions[j];
            if (other.type !== 'COMPARISON') continue;
            const otherField = asField(other.left);
            if (!otherField || !fieldsEqual(field, otherField)) continue;

            // not-in with equality
            if (!Array.isArray(other.right) && isLiteralExpr(other.right) && other.operation === '==') {
              const lit = other.right;
              peers.push(j);
              if (notInArray.some(v => expressionsEqual(v, lit))) {
                merged = constant(true);
              } else if (notInArray.length === 1) {
                merged = ne(field, notInArray[0]);
              } else {
                merged = notInList(field, notInArray);
              }
              break;
            }

            // not-in with in-list
            if (other.operation === 'in' && Array.isArray(other.right) && isLiteralArray(other.right)) {
              const allowList = other.right;
              peers.push(j);
              const remaining = notInArray.filter(v => !allowList.some(a => expressionsEqual(a, v)));
              if (remaining.length === 0) {
                merged = constant(true);
              } else if (remaining.length === 1) {
                merged = ne(field, remaining[0]);
              } else {
                merged = notInList(field, remaining);
              }
              break;
            }
          }

          if (merged) {
            result.push(merged);
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            continue;
          }
        }
      }

      result.push(current);
      processed[i] = true;
      continue;
    }

    const baseKey = predicatesListKey(base);
    const familyLimit = membership.kind === 'in' ? IN_LIST_MAX : ARRAY_CONTAINS_ANY_MAX;
    const mergedValues = [...membership.values];
    const groupIndices: number[] = [i];

    for (let j = i + 1; j < conditions.length; j++) {
      if (processed[j]) continue;
      const candidate = splitBaseAndMembership(conditions[j]);
      if (!candidate.membership) continue;
      if (candidate.membership.kind !== membership.kind) continue;
      if (candidate.membership.fieldKey !== membership.fieldKey) continue;
      if (predicatesListKey(candidate.base) !== baseKey) continue;

      groupIndices.push(j);
      for (const v of candidate.membership.values) {
        pushUniqueExpression(mergedValues, v);
      }
    }

    // No merge opportunities; preserve original condition to avoid reordering churn.
    if (groupIndices.length === 1) {
      result.push(current);
      processed[i] = true;
      continue;
    }

    if (familyLimit && mergedValues.length > familyLimit) {
      // Do not merge to avoid exceeding Firestore limits; keep the first as-is.
      result.push(current);
      processed[i] = true;
      continue;
    }

    const mergedMembership = buildMembershipPredicate(membership.field, membership.kind, mergedValues);
    const combined = base.length > 0
      ? simplifyComposite(and([...base, mergedMembership]))
      : mergedMembership;

    result.push(combined);
    groupIndices.forEach(idx => processed[idx] = true);
  }

  // Scalar OR rewrite: (< A || > A) -> != A (same field & literal)
  // Also canonicalizes (=< or =>) paired with equality when possible.
  return rewriteScalarOr(result);
}

function mergeAndConditions(conditions: Predicate[]): Predicate[] {
  interface FieldBucket {
    field: Field;
    firstIndex: number;
    eq?: Literal;
    inValues?: Literal[];
    neqValues: Literal[];
    notInValues: Literal[];
    inequalities: Array<{ op: WhereFilterOp; right: Literal }>;
    others: Predicate[];
    contradiction?: boolean;
  }

  const buckets = new Map<string, FieldBucket>();
  const bucketOrder: string[] = [];
  const passthrough: Array<{ index: number; predicate: Predicate }> = [];

  for (let i = 0; i < conditions.length; i++) {
    const cond = conditions[i];
    if (cond.type !== 'COMPARISON') {
      passthrough.push({ index: i, predicate: cond });
      continue;
    }

    const field = asField(cond.left);
    if (!field) {
      passthrough.push({ index: i, predicate: cond });
      continue;
    }

    const key = fieldKey(field);
    let bucket = buckets.get(key);
    if (!bucket) {
      bucket = {
        field,
        firstIndex: i,
        neqValues: [],
        notInValues: [],
        inequalities: [],
        others: [],
      };
      buckets.set(key, bucket);
      bucketOrder.push(key);
    } else {
      bucket.firstIndex = Math.min(bucket.firstIndex, i);
    }

    const right = cond.right;
    switch (cond.operation) {
      case '==':
        if (!Array.isArray(right) && isLiteralExpr(right)) {
          if (bucket.eq && !expressionsEqual(bucket.eq, right)) {
            bucket.contradiction = true;
          } else {
            bucket.eq = right;
          }
        } else {
          bucket.others.push(cond);
        }
        break;
      case '!=':
        if (!Array.isArray(right) && isLiteralExpr(right)) {
          pushUniqueExpression(bucket.neqValues, right);
        } else {
          bucket.others.push(cond);
        }
        break;
      case 'not-in':
        if (Array.isArray(right) && right.every(isLiteralExpr)) {
          right.forEach(r => pushUniqueExpression(bucket.notInValues, r));
        } else {
          bucket.others.push(cond);
        }
        break;
      case 'in':
        if (Array.isArray(right) && right.every(isLiteralExpr)) {
          const literals = dedupeExpressions(right);
          if (bucket.inValues) {
            const intersected = intersectLiterals(bucket.inValues, literals);
            if (intersected.length === 0) {
              bucket.contradiction = true;
            }
            bucket.inValues = intersected;
          } else {
            bucket.inValues = literals;
          }
        } else {
          bucket.others.push(cond);
        }
        break;
      case '<':
      case '<=':
      case '>':
      case '>=':
        if (!Array.isArray(right) && isLiteralExpr(right)) {
          bucket.inequalities.push({ op: cond.operation, right });
        } else {
          bucket.others.push(cond);
        }
        break;
      default:
        bucket.others.push(cond);
        break;
    }
  }

  const assembled: Array<{ index: number; predicates: Predicate[] }> = [];

  for (const key of bucketOrder) {
    const bucket = buckets.get(key)!;
    if (bucket.contradiction) {
      return [constant(false)];
    }

    const built = buildBucketPredicates(bucket);
    if (built.contradiction) {
      return [constant(false)];
    }
    assembled.push({ index: bucket.firstIndex, predicates: built.predicates });
  }

  passthrough.forEach(item => assembled.push({ index: item.index, predicates: [item.predicate] }));
  assembled.sort((a, b) => a.index - b.index);

  return assembled.flatMap(e => e.predicates);
}

function buildBucketPredicates(bucket: {
  field: Field;
  eq?: Literal;
  inValues?: Literal[];
  neqValues: Literal[];
  notInValues: Literal[];
  inequalities: Array<{ op: WhereFilterOp; right: Literal }>;
  others: Predicate[];
}): { predicates: Predicate[]; contradiction?: boolean } {
  // If we have equality, validate it against other constraints and keep only the equality + others.
  if (bucket.eq) {
    const eqExpr = bucket.eq;

    // Equality clashes with != or not-in
    if (bucket.neqValues.some(v => expressionsEqual(v, eqExpr))) {
      return { predicates: [], contradiction: true };
    }
    if (bucket.notInValues.some(v => expressionsEqual(v, eqExpr))) {
      return { predicates: [], contradiction: true };
    }

    // Equality must satisfy all inequalities
    for (const ineq of bucket.inequalities) {
      if (!evaluateLiteralVsLiteral(eqExpr, ineq.op, ineq.right)) {
        return { predicates: [], contradiction: true };
      }
    }

    // Equality dominates scalar inequalities and NE constraints.
    return {
      predicates: [
        eq(bucket.field, eqExpr),
        ...bucket.others,
      ],
    };
  }

  // Track negative literals (neq + not-in) for pruning and emission.
  const negativeValues = dedupeExpressions([...bucket.neqValues, ...bucket.notInValues]);
  const predicates: Predicate[] = [];

  const inequalityResult = reduceInequalities(bucket.inequalities, bucket.field);
  if (inequalityResult.contradiction) {
    return { predicates: [], contradiction: true };
  }

  let lower = inequalityResult.lower;
  let upper = inequalityResult.upper;

  if (lower && lower.inclusive && negativeValues.some(v => expressionsEqual(v, lower!.literal))) {
    lower = { ...lower, inclusive: false };
  }
  if (upper && upper.inclusive && negativeValues.some(v => expressionsEqual(v, upper!.literal))) {
    upper = { ...upper, inclusive: false };
  }

  const inequalityPredicates: Predicate[] = [];
  if (lower) {
    inequalityPredicates.push(lower.inclusive ? gte(bucket.field, lower.literal) : gt(bucket.field, lower.literal));
  }
  if (upper) {
    inequalityPredicates.push(upper.inclusive ? lte(bucket.field, upper.literal) : lt(bucket.field, upper.literal));
  }

  if (bucket.inValues) {
    const filtered = bucket.inValues.filter(lit => {
      if (negativeValues.some(v => expressionsEqual(v, lit))) return false;
      if (lower && !evaluateLiteralVsLiteral(lit, lower.inclusive ? '>=' : '>', lower.literal)) return false;
      if (upper && !evaluateLiteralVsLiteral(lit, upper.inclusive ? '<=' : '<', upper.literal)) return false;
      return true;
    });

    if (filtered.length === 0) {
      return { predicates: [], contradiction: true };
    }

    if (filtered.length === 1) {
      predicates.push(inList(bucket.field, filtered));
    } else {
      predicates.push(inList(bucket.field, filtered));
    }
  } else {
    predicates.push(...inequalityPredicates);
  }

  // Emit remaining negative predicates only when no IN is present (IN already pruned).
  if (!bucket.inValues && negativeValues.length > 0) {
    const remainingNegatives = negativeValues.filter(lit => {
      if (lower && !evaluateLiteralVsLiteral(lit, lower.inclusive ? '>=' : '>', lower.literal)) return false;
      if (upper && !evaluateLiteralVsLiteral(lit, upper.inclusive ? '<=' : '<', upper.literal)) return false;
      return true;
    });

    if (remainingNegatives.length === 1) {
      predicates.push(ne(bucket.field, remainingNegatives[0]));
    } else if (remainingNegatives.length > 1) {
      const limit = NOT_IN_LIST_MAX;
      if (remainingNegatives.length > limit) {
        const chunks = chunkArray(remainingNegatives, limit);
        for (const chunk of chunks) {
          if (chunk.length === 1) {
            predicates.push(ne(bucket.field, chunk[0]));
          } else {
            predicates.push(notInList(bucket.field, chunk));
          }
        }
      } else if (remainingNegatives.length === 1) {
        predicates.push(ne(bucket.field, remainingNegatives[0]));
      } else {
        predicates.push(notInList(bucket.field, remainingNegatives));
      }
    }
  }
  predicates.push(...bucket.others);

  return { predicates };
}

function reduceInequalities(
  inequalities: Array<{ op: WhereFilterOp; right: Literal }>,
  field: Field
): {
  predicates: Predicate[];
  contradiction?: boolean;
  lower?: { value: any; inclusive: boolean; literal: Literal };
  upper?: { value: any; inclusive: boolean; literal: Literal };
} {
  if (inequalities.length === 0) {
    return { predicates: [] };
  }

  type Bound = { value: any; inclusive: boolean; literal: Literal };
  let lower: Bound | undefined;
  let upper: Bound | undefined;

  for (const ineq of inequalities) {
    const value = ineq.right.value as any;
    if (ineq.op === '>' || ineq.op === '>=') {
      const candidate: Bound = { value, inclusive: ineq.op === '>=', literal: ineq.right };
      if (!lower || value > lower.value || value === lower.value && !lower.inclusive && candidate.inclusive) {
        lower = candidate;
      }
    } else if (ineq.op === '<' || ineq.op === '<=') {
      const candidate: Bound = { value, inclusive: ineq.op === '<=', literal: ineq.right };
      if (!upper || value < upper.value || value === upper.value && !upper.inclusive && candidate.inclusive) {
        upper = candidate;
      }
    }
  }

  if (lower && upper) {
    if (lower.value > upper.value) {
      return { predicates: [], contradiction: true };
    }
    if (lower.value === upper.value) {
      if (lower.inclusive && upper.inclusive) {
        return {
          predicates: [eq(field, lower.literal)],
          lower,
          upper,
        };
      }
      return { predicates: [], contradiction: true };
    }
  }

  const predicates: Predicate[] = [];
  if (lower) {
    predicates.push(
      lower.inclusive ? gte(field, lower.literal) : gt(field, lower.literal)
    );
  }
  if (upper) {
    predicates.push(
      upper.inclusive ? lte(field, upper.literal) : lt(field, upper.literal)
    );
  }

  return { predicates, lower, upper };
}

function rewriteScalarOr(conditions: Predicate[]): Predicate[] {
  const processed = new Array(conditions.length).fill(false);
  const result: Predicate[] = [];

  for (let i = 0; i < conditions.length; i++) {
    if (processed[i]) continue;
    const first = conditions[i];
    if (first.type === 'COMPARISON') {
      const field = asField(first.left);
      const right = first.right;
      if (field && !Array.isArray(right) && isLiteralExpr(right)) {
        const op1 = first.operation;
        const lit = right;

        // Try to merge with an equality/inequality partner.
        const peers: number[] = [];
        for (let j = i + 1; j < conditions.length; j++) {
          if (processed[j]) continue;
          const other = conditions[j];
          if (other.type !== 'COMPARISON') continue;
          const otherField = asField(other.left);
          if (!otherField || !fieldsEqual(field, otherField)) continue;
          if (Array.isArray(other.right) || !isLiteralExpr(other.right)) continue;

          const op2 = other.operation;
          const otherLit = other.right as Literal;
          const sameLiteral = expressionsEqual(lit, otherLit);
          const op1IsLt = op1 === '<' || op1 === '<=';
          const op1IsGt = op1 === '>' || op1 === '>=';
          const op2IsLt = op2 === '<' || op2 === '<=';
          const op2IsGt = op2 === '>' || op2 === '>=';

          // (< A || == A) => <= A; (> A || == A) => >= A and symmetric orders.
          const isEqFirst = op1 === '==';
          const isEqOther = op2 === '==';
          if (sameLiteral && (isEqFirst && op2IsLt || isEqOther && op1IsLt)) {
            peers.push(j);
            const merged: ComparisonPredicate = lte(field, lit);
            result.push(merged);
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }
          if (sameLiteral && (isEqFirst && op2IsGt || isEqOther && op1IsGt)) {
            peers.push(j);
            const merged: ComparisonPredicate = gte(field, lit);
            result.push(merged);
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }

          // (< A || > A) => != A
          if (sameLiteral && (op1 === '<' && op2 === '>' || op1 === '>' && op2 === '<')) {
            peers.push(j);
            const merged: ComparisonPredicate = ne(field, lit);
            result.push(merged);
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }

          // (!= A || == A) => TRUE
          if (sameLiteral && (op1 === '!=' && op2 === '==' || op2 === '!=' && op1 === '==')) {
            peers.push(j);
            result.push(constant(true));
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }

          // (!= A || >=/<= A) => TRUE; (!= A || >/</>=/<= A) collapse accordingly
          if (sameLiteral && (op1 === '!=' && op2IsGt || op2 === '!=' && op1IsGt || op1 === '!=' && op2IsLt || op2 === '!=' && op1IsLt)) {
            peers.push(j);
            const mergedOp = op1 === '!=' && op2 === '>' || op2 === '!=' && op1 === '>' || op1 === '!=' && op2 === '<' || op2 === '!=' && op1 === '<' ? '!=' : 'CONSTANT_TRUE';
            if (mergedOp === 'CONSTANT_TRUE') {
              result.push(constant(true));
            } else {
              const merged: ComparisonPredicate = ne(field, lit);
              result.push(merged);
            }
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }
          if (sameLiteral && (op1 === '!=' && op2 === '>=' || op2 === '!=' && op1 === '>=' || op1 === '!=' && op2 === '<=' || op2 === '!=' && op1 === '<=')) {
            peers.push(j);
            result.push(constant(true));
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }

          // (!= A || == B) where A != B => simplifies to != A (or != B if reversed)
          if (op1 === '!=' && op2 === '==' || op2 === '!=' && op1 === '==') {
            if (!sameLiteral) {
              peers.push(j);
              const merged: ComparisonPredicate = op1 === '!='
                ? ne(field, lit)
                : ne(field, otherLit);
              result.push(merged);
              processed[i] = true;
              peers.forEach(idx => processed[idx] = true);
              break;
            }
          }
        }
        if (processed[i]) {
          continue;
        }
      }
      if (field && !Array.isArray(right) && isLiteralExpr(right) && (first.operation === '<' || first.operation === '>')) {
        const peers: number[] = [];
        for (let j = i + 1; j < conditions.length; j++) {
          if (processed[j]) continue;
          const other = conditions[j];
          if (other.type !== 'COMPARISON') continue;
          const otherField = asField(other.left);
          if (!otherField || !fieldsEqual(field, otherField)) continue;
          if (Array.isArray(other.right) || !isLiteralExpr(other.right)) continue;
          if (!expressionsEqual(right, other.right)) continue;
          if (first.operation === '<' && other.operation === '>' || first.operation === '>' && other.operation === '<') {
            peers.push(j);
            const merged: ComparisonPredicate = ne(field, right);
            result.push(merged);
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }
        }
        if (processed[i]) {
          continue;
        }
      }
      // Handle not-in with equality or in-list
      if (field && first.operation === 'not-in' && Array.isArray(right) && isLiteralArray(right)) {
        const notInArray = right;
        const peers: number[] = [];
        for (let j = i + 1; j < conditions.length; j++) {
          if (processed[j]) continue;
          const other = conditions[j];
          if (other.type !== 'COMPARISON') continue;
          const otherField = asField(other.left);
          if (!otherField || !fieldsEqual(field, otherField)) continue;

          // not-in with equality
          if (!Array.isArray(other.right) && isLiteralExpr(other.right) && other.operation === '==') {
            const lit = other.right;
            peers.push(j);
            const remaining = notInArray.filter(v => !expressionsEqual(v, lit));
            if (remaining.length === notInArray.length) {
              // eq is already allowed; OR reduces to the not-in predicate.
              if (notInArray.length === 1) {
                result.push(ne(field, notInArray[0]));
              } else {
                result.push(notInList(field, notInArray));
              }
            } else if (remaining.length === 0) {
              result.push(constant(true));
            } else if (remaining.length === 1) {
              result.push(ne(field, remaining[0]));
            } else {
              result.push(notInList(field, remaining));
            }
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }

          // not-in with in-list
          if (other.operation === 'in' && Array.isArray(other.right) && isLiteralArray(other.right)) {
            const allowList = other.right;
            peers.push(j);
            const remaining = notInArray.filter(v => !allowList.some(a => expressionsEqual(a, v)));
            if (remaining.length === 0) {
              result.push(constant(true));
            } else {
              result.push(notInList(field, remaining));
            }
            processed[i] = true;
            peers.forEach(idx => processed[idx] = true);
            break;
          }
        }
        if (processed[i]) {
          continue;
        }
      }

      // Symmetric: equality with a not-in on the same field.
      if (field && first.operation === '==' && !Array.isArray(right) && isLiteralExpr(right)) {
        const lit = right;
        const peers: number[] = [];
        for (let j = i + 1; j < conditions.length; j++) {
          if (processed[j]) continue;
          const other = conditions[j];
          if (other.type !== 'COMPARISON') continue;
          const otherField = asField(other.left);
          if (!otherField || !fieldsEqual(field, otherField)) continue;
          if (other.operation !== 'not-in' || !Array.isArray(other.right) || !isLiteralArray(other.right)) continue;
          const notInArray = other.right;
          peers.push(j);

          if (notInArray.some(v => expressionsEqual(v, lit))) {
            result.push(constant(true));
          } else if (notInArray.length === 1) {
            result.push(ne(field, notInArray[0]));
          } else {
            result.push(notInList(field, notInArray));
          }

          processed[i] = true;
          peers.forEach(idx => processed[idx] = true);
          break;
        }
        if (processed[i]) continue;
      }
    }

    result.push(first);
    processed[i] = true;
  }

  // Final pass: collapse any remaining OR pairs of (not-in, eq) on same field.
  for (let i = 0; i < result.length; i++) {
    const a = result[i];
    if (a.type !== 'COMPARISON') continue;
    const aField = asField(a.left);
    if (!aField) continue;
    for (let j = i + 1; j < result.length; j++) {
      const b = result[j];
      if (b.type !== 'COMPARISON') continue;
      const bField = asField(b.left);
      if (!bField || !fieldsEqual(aField, bField)) continue;

      const pair = simplifyNotInEqPair(a, b);
      if (pair) {
        const mergedResult = result.filter((_, idx) => idx !== i && idx !== j);
        mergedResult.push(pair);
        return mergedResult;
      }
    }
  }

  return result;
}

function simplifyNotInEqPair(a: ComparisonPredicate, b: ComparisonPredicate): Predicate | null {
  const combos: [ComparisonPredicate, ComparisonPredicate][] = [];
  combos.push([a, b], [b, a]);

  for (const [first, second] of combos) {
    const field = asField(first.left);
    if (!field) continue;
    if (first.operation !== 'not-in' || !Array.isArray(first.right) || !isLiteralArray(first.right)) continue;
    if (second.operation !== '==') continue;
    const lit = second.right;
    if (Array.isArray(lit) || !isLiteralExpr(lit)) continue;

    const list = first.right;
    if (list.some(v => expressionsEqual(v, lit))) {
      return constant(true);
    }

    if (list.length === 1) {
      return ne(field, list[0]);
    }

    return notInList(field, list);
  }

  return null;
}

function splitBaseAndMembership(predicate: Predicate): { base: Predicate[]; membership?: MembershipInfo } {
  const conjuncts = predicate.type === 'AND' ? predicate.conditions : [predicate];
  const base: Predicate[] = [];
  const memberships: Array<{ info: MembershipInfo; predicate: Predicate }> = [];

  for (const cond of conjuncts) {
    const m = asMembership(cond);
    if (m) {
      memberships.push({ info: m, predicate: cond });
    } else {
      base.push(cond);
    }
  }

  if (memberships.length === 0) {
    return { base, membership: undefined };
  }

  const chosen = memberships.find(m => m.info.op !== '==') ?? memberships[memberships.length - 1];
  const membership = chosen.info;
  const baseWithOthers = [
    ...base,
    ...memberships.filter(m => m !== chosen).map(m => m.predicate),
  ];

  return { base: baseWithOthers, membership };
}

type MembershipKind = 'in' | 'array';
interface MembershipInfo {
  field: Field;
  fieldKey: string;
  kind: MembershipKind;
  values: Expression[];
  op: WhereFilterOp;
}

function asMembership(predicate: Predicate): MembershipInfo | undefined {
  if (predicate.type !== 'COMPARISON') return undefined;
  const field = asField(predicate.left);
  if (!field) return undefined;

  switch (predicate.operation) {
    case '==':
      if (isFieldExpr(predicate.right as Expression)) return undefined;
      return { field, fieldKey: fieldKey(field), kind: 'in', values: [predicate.right as Expression], op: predicate.operation };
    case 'in':
      if (Array.isArray(predicate.right) && predicate.right.every(v => !isFieldExpr(v))) {
        return { field, fieldKey: fieldKey(field), kind: 'in', values: predicate.right, op: predicate.operation };
      }
      return undefined;
    case 'array-contains':
      if (isFieldExpr(predicate.right as Expression)) return undefined;
      return { field, fieldKey: fieldKey(field), kind: 'array', values: [predicate.right as Expression], op: predicate.operation };
    case 'array-contains-any':
      if (Array.isArray(predicate.right) && predicate.right.every(v => !isFieldExpr(v))) {
        return { field, fieldKey: fieldKey(field), kind: 'array', values: predicate.right, op: predicate.operation };
      }
      return undefined;
    default:
      return undefined;
  }
}

function buildMembershipPredicate(field: Field, kind: MembershipKind, values: Expression[]): Predicate {
  const deduped = dedupeExpressions(values);
  if (kind === 'in') {
    if (deduped.length === 1) {
      return eq(field, deduped[0]);
    }
    return inList(field, deduped);
  }

  if (deduped.length === 1) {
    return arrayContains(field, deduped[0]);
  }
  return arrayContainsAny(field, deduped);
}

function normalizeComparisonOperands(predicate: ComparisonPredicate): ComparisonPredicate {
  const rightIsList = Array.isArray(predicate.right);
  const leftField = asField(predicate.left);
  const rightField = rightIsList ? null : asField(predicate.right as Expression);

  if (!leftField && rightField && !rightIsList) {
    const inverted = invertComparisonOp(predicate.operation);
    if (inverted) {
      return {
        type: 'COMPARISON',
        operation: inverted,
        left: predicate.right as Expression,
        right: predicate.left,
      };
    }
  }

  return predicate;
}

function evaluateComparisonIfLiteral(predicate: ComparisonPredicate): Predicate | null {
  const leftLit = asLiteral(predicate.left);
  const right = predicate.right;

  let rightLit = false;
  let rightVal: any;

  if (Array.isArray(right)) {
    rightLit = right.every(isLiteralExpr);
    if (rightLit) {
      rightVal = (right as Literal[]).map(r => r.value);
    }
  } else {
    rightLit = isLiteralExpr(right);
    if (rightLit) {
      rightVal = (right as Literal).value;
    }
  }

  if (!leftLit || !rightLit) {
    return null;
  }

  const leftVal = leftLit.value!;
  const op = predicate.operation;
  const comparator = createOperationComparator(op);
  try {
    const result = comparator(leftVal, rightVal);
    return constant(result);
  } catch {
    return null;
  }
}

function partitionListByField(items: Expression[]): { fields: Expression[]; others: Expression[] } {
  const fields: Expression[] = [];
  const others: Expression[] = [];
  for (const item of items) {
    if (isFieldExpr(item)) {
      fields.push(item);
    } else {
      others.push(item);
    }
  }
  return { fields, others };
}

function dedupeExpressions<T extends Expression>(list: T[]): T[] {
  const out: T[] = [];
  list.forEach(expr => pushUniqueExpression(out, expr));
  return out;
}

function pushUniqueExpression<T extends Expression>(list: T[], expr: T) {
  if (!list.some(e => expressionsEqual(e, expr))) {
    list.push(expr);
  }
}

function isLiteralArray(list: Expression[]): list is Literal[] {
  return list.every(isLiteralExpr);
}

function intersectLiterals(a: Literal[], b: Literal[]): Literal[] {
  return a.filter(item => b.some(other => expressionsEqual(item, other)));
}

function getListLimit(op: WhereFilterOp): number | undefined {
  switch (op) {
    case 'in':
      return IN_LIST_MAX;
    case 'not-in':
      return NOT_IN_LIST_MAX;
    case 'array-contains-any':
      return ARRAY_CONTAINS_ANY_MAX;
    default:
      return undefined;
  }
}

function splitListComparison(predicate: ComparisonPredicate, limit: number): Predicate {
  const right = predicate.right;
  if (!Array.isArray(right)) {
    return predicate;
  }

  const chunks = chunkArray(right, limit);
  const comparisons = chunks.map(chunk => ({ ...predicate, right: chunk }));

  if (predicate.operation === 'not-in') {
    return simplifyComposite(and(comparisons));
  }

  return simplifyComposite(or(comparisons));
}

function chunkArray<T>(arr: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}

function evaluateLiteralVsLiteral(left: Literal, op: WhereFilterOp, right: Literal): boolean {
  const comparator = createOperationComparator(op);
  return comparator(left.value, right.value);
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

    return simplifyComposite(or(flattened));
  }

  // AND case - need to distribute over OR if present
  if (predicate.type === 'AND') {
    const conditions = predicate.conditions.map(toDNFInternal);

    // Find first OR among conditions
    const orIndex = conditions.findIndex(c => c.type === 'OR');

    if (orIndex === -1) {
      // No OR found, already in DNF
      return simplifyComposite(and(conditions));
    }

    // Distribute: (A AND (B OR C)) => (A AND B) OR (A AND C)
    const orCondition = conditions[orIndex] as CompositePredicate;
    const otherConditions = conditions.filter((_, i) => i !== orIndex);

    const distributed = orCondition.conditions.map(orChild => {
      return toDNFInternal(and([...otherConditions, orChild]));
    });

    return toDNFInternal(or(distributed));
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
        expressionsOrListEqual(a.right, bComp.right) &&
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

function predicateKey(p: Predicate): string {
  switch (p.type) {
    case 'CONSTANT':
      return `C:${p.value}`;
    case 'COMPARISON':
      return `CMP:${p.operation}:${exprKey(p.left)}:${Array.isArray(p.right) ? '[' + p.right.map(exprKey).join(',') + ']' : exprKey(p.right)}`;
    case 'NOT':
      return `NOT:${predicateKey(p.operand)}`;
    case 'AND':
      return `AND:${p.conditions.map(predicateKey).sort().join('|')}`;
    case 'OR':
      return `OR:${p.conditions.map(predicateKey).sort().join('|')}`;
    default:
      return 'UNKNOWN';
  }
}

function predicatesListKey(list: Predicate[]): string {
  if (list.length === 0) return 'EMPTY';
  return list.map(predicateKey).sort().join('|');
}

function exprKey(expr: Expression): string {
  if (isFieldExpr(expr)) {
    return `F:${expr.source ?? ''}:${expr.path.join('.')}`;
  }
  if (isLiteralExpr(expr)) {
    return `L:${(expr as Literal).value}`;
  }
  if (expr.kind === 'Param') {
    return `P:${(expr as Param).name}`;
  }
  return 'UNK';
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

function expressionsOrListEqual(a: Expression | Expression[], b: Expression | Expression[]): boolean {
  const aIsArray = Array.isArray(a);
  const bIsArray = Array.isArray(b);

  if (aIsArray || bIsArray) {
    if (!aIsArray || !bIsArray) return false;
    return expressionsArrayEqual(a, b);
  }

  return expressionsEqual(a, b);
}

function expressionsArrayEqual(left: Expression[], right: Expression[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((expr, idx) => expressionsEqual(expr, right[idx]));
}

function arrayEquals(left: unknown[], right: unknown[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, idx) => value === right[idx]);
}

function isFieldExpr(expr: Expression): expr is Field {
  return expr?.kind === 'Field';
}

function asField(expr: Expression): Field | null {
  return isFieldExpr(expr) ? expr as Field : null;
}

function isLiteralExpr(expr: Expression): expr is Literal {
  return expr?.kind === 'Literal';
}

function asLiteral(expr: Expression): Literal | null {
  return isLiteralExpr(expr) ? expr as Literal : null;
}

function fieldKey(field: Field): string {
  return `${field.source ?? ''}:${field.path.join('.')}`;
}

function fieldsEqual(a: Field, b: Field): boolean {
  return fieldKey(a) === fieldKey(b);
}
