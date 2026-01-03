import { WhereFilterOp } from '@google-cloud/firestore';
import { ComparisonPredicate, Field } from '../../api/expression';
import { JoinNode } from '../ast';
import { getValueFromField } from '../evaluator';
import { getSortOrderFromNode, isSortedBy, mergeJoin } from '../utils/merge-join-utils';
import { isMergeJoinCompatible } from '../utils/operation-comparator';
import { ensureField } from '../utils/predicate-utils';
import { Operator } from './operator';

/**
 * MergeJoinOperator
 *
 * Sorts both the LEFT and RIGHT collections by their join fields.
 * Uses a two-pointer algorithm to scan through both sorted collections in parallel.
 *
 * Complexity: O(N log N + M log M) for sorting, O(N + M) for merging with two pointers.
 * Memory: O(N + M) - Both collections must fit in memory.
 * Requirement: Comparison operations (==, <, <=, >, >=).
 */
export class MergeJoinOperator implements Operator {
  private leftField: Field;
  private rightField: Field;
  private operation: WhereFilterOp;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    private node: JoinNode
  ) {
    if (!isMergeJoinCompatible(node.condition)) {
      throw new Error(
        `MergeJoin strategy requires comparison operation (==, <, <=, >, >=), got: ${node.condition}`
      );
    }
    const condition = node.condition as ComparisonPredicate;
    this.operation = condition.operation;
    this.leftField = ensureField(condition.left);
    this.rightField = ensureField(condition.right);
  }

  async *[Symbol.asyncIterator]() {
    const leftSort = getSortOrderFromNode(this.node.left);
    const rightSort = getSortOrderFromNode(this.node.right);

    const sortLeft = !isSortedBy(leftSort, this.leftField, 'asc');
    const sortRight = !isSortedBy(rightSort, this.rightField, 'asc');

    yield* mergeJoin(this.leftSource, this.rightSource, {
      leftKey: (row) => getValueFromField(row, this.leftField),
      rightKey: (row) => getValueFromField(row, this.rightField),
      operation: this.operation,
      sortLeft,
      sortRight,
    });
  }
}
