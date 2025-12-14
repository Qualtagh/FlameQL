import { WhereFilterOp } from '@google-cloud/firestore';
import { literal } from '../../api/api';
import { ComparisonPredicate, Expression, Field, Predicate } from '../../api/expression';
import { ScanNode } from '../ast';
import { IndexManager } from '../indexes/index-manager';
import { invertComparisonOp } from './operation-comparator';

export type IndexedNestedLoopLookupPlan =
  | {
    mode: 'batch';
    lookupOp: 'in' | 'array-contains-any';
    leftExpr: Expression;
    rightField: Field;
  }
  | {
    mode: 'perRow';
    lookupOp: WhereFilterOp;
    leftExpr: Expression;
    rightField: Field;
  };

export function collectConjunctiveComparisons(predicate: Predicate): ComparisonPredicate[] {
  switch (predicate.type) {
    case 'COMPARISON':
      return [predicate];
    case 'AND':
      return predicate.conditions.flatMap(collectConjunctiveComparisons);
    default:
      return [];
  }
}

export function pickIndexedNestedLoopLookupPlan(
  condition: Predicate,
  rightScan: Pick<ScanNode, 'alias' | 'collectionPath'>,
  indexManager?: IndexManager,
  opts?: { requireIndex?: boolean }
): IndexedNestedLoopLookupPlan | null {
  const requireIndex = opts?.requireIndex ?? false;
  const candidates = buildIndexedNestedLoopCandidates(condition, rightScan.alias);
  if (candidates.length === 0) return null;

  if (!indexManager) {
    // Prefer batch candidates when we can't rank by indexes.
    return candidates.find(c => c.mode === 'batch') ?? candidates[0];
  }

  const rank = (type: string) => type === 'exact' ? 2 : type === 'partial' ? 1 : 0;
  let best: IndexedNestedLoopLookupPlan | null = null;
  let bestScore = -Infinity;

  for (const c of candidates) {
    const match = indexManager.match(rightScan.collectionPath, [{
      field: c.rightField,
      op: c.lookupOp,
      value: literal(null),
    }]);

    const r = rank(match.type);
    if (requireIndex && r === 0) continue;

    const modeBonus = c.mode === 'batch' ? 100 : 0;
    const score = modeBonus + r * 10 + match.matchedFields;
    if (score > bestScore) {
      best = c;
      bestScore = score;
    }
  }

  return best;
}

function buildIndexedNestedLoopCandidates(condition: Predicate, rightAlias: string): IndexedNestedLoopLookupPlan[] {
  const comparisons = collectConjunctiveComparisons(condition);
  const out: IndexedNestedLoopLookupPlan[] = [];

  for (const cmp of comparisons) {
    const a = asField(cmp.left);
    const b = asField(cmp.right);
    if (!a || !b) continue;

    const aRight = a.source === rightAlias;
    const bRight = b.source === rightAlias;
    if (aRight === bRight) continue; // not a join predicate

    const rightField = aRight ? a : b;
    const leftExpr = aRight ? b : a;
    const rightIsLeftOperand = aRight;

    const op = cmp.operation;

    // Equality join: batch with IN on the right field.
    if (op === '==') {
      out.push({
        mode: 'batch',
        lookupOp: 'in',
        leftExpr,
        rightField,
      });
      continue;
    }

    // Membership join: leftScalar in rightArrayField -> batch with array-contains-any on right array field.
    if (op === 'in' && !rightIsLeftOperand) {
      out.push({
        mode: 'batch',
        lookupOp: 'array-contains-any',
        leftExpr,
        rightField,
      });
      continue;
    }

    // Reversed IN: rightScalar in leftArray -> per-row IN on the right scalar field with the left array as constant.
    if (op === 'in' && rightIsLeftOperand) {
      out.push({
        mode: 'perRow',
        lookupOp: 'in',
        leftExpr,
        rightField,
      });
      continue;
    }

    // array-contains: two cases
    // - rightArray contains leftScalar  => per-row array-contains
    // - leftArray contains rightScalar  => per-row IN on rightScalar field with leftArray as constant
    if (op === 'array-contains') {
      if (rightIsLeftOperand) {
        out.push({
          mode: 'perRow',
          lookupOp: 'array-contains',
          leftExpr,
          rightField,
        });
      } else {
        out.push({
          mode: 'perRow',
          lookupOp: 'in',
          leftExpr,
          rightField,
        });
      }
      continue;
    }

    // array-contains-any (array intersection): per-row array-contains-any on right array field with left array as constant.
    if (op === 'array-contains-any') {
      out.push({
        mode: 'perRow',
        lookupOp: 'array-contains-any',
        leftExpr,
        rightField,
      });
      continue;
    }

    // not-in only makes sense as: rightScalar not-in leftArray (right field is left operand).
    if (op === 'not-in') {
      if (rightIsLeftOperand) {
        out.push({
          mode: 'perRow',
          lookupOp: 'not-in',
          leftExpr,
          rightField,
        });
      }
      continue;
    }

    // Inequalities / !=: per-row comparison on right field (invert op when right field is on the RHS).
    const whereOp = rightIsLeftOperand ? op : invertComparisonOp(op);
    if (whereOp) {
      out.push({
        mode: 'perRow',
        lookupOp: whereOp as WhereFilterOp,
        leftExpr,
        rightField,
      });
    }
  }

  return out;
}

function asField(expr: any): Field | null {
  if (expr && typeof expr === 'object' && expr.kind === 'Field' && expr.source) {
    return expr as Field;
  }
  return null;
}
