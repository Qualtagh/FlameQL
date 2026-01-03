import { WhereFilterOp } from '@google-cloud/firestore';
import { ComparisonPredicate, Field } from '../../api/expression';
import { JoinNode } from '../ast';
import { getValueFromField } from '../evaluator';
import { JoinHashTable } from '../utils/hash-join-utils';
import { isHashJoinCompatible } from '../utils/operation-comparator';
import { ensureField } from '../utils/predicate-utils';
import { Operator } from './operator';

/**
 * HashJoinOperator
 *
 * Builds an in-memory Hash Map (Index) of the RIGHT collection.
 * Streams the LEFT collection and looks up matches in the Hash Map.
 *
 * Complexity: O(N + M) where N is left size, M is right size.
 * Memory: O(M) - Right collection must fit in memory.
 * Requirement: Hash-compatible operations (==, in, array-contains, array-contains-any).
 */
export class HashJoinOperator implements Operator {
  private leftField: Field;
  private rightField: Field;
  private operation: WhereFilterOp;

  constructor(
    private leftSource: Operator,
    private rightSource: Operator,
    node: JoinNode
  ) {
    if (!isHashJoinCompatible(node.condition)) {
      throw new Error(
        `HashJoin strategy requires hash-compatible operation (==, in, array-contains, array-contains-any), got: ${node.condition}`
      );
    }
    const condition = node.condition as ComparisonPredicate;
    this.operation = condition.operation;
    this.leftField = ensureField(condition.left);
    this.rightField = ensureField(condition.right);
  }

  async *[Symbol.asyncIterator]() {
    const hashTable = new JoinHashTable(this.operation);

    // Build hash table
    for await (const row of this.rightSource) {
      const val = getValueFromField(row, this.rightField);
      hashTable.add(val, row);
    }

    // Probe hash table
    for await (const leftRow of this.leftSource) {
      const leftValue = getValueFromField(leftRow, this.leftField);
      const matches = hashTable.get(leftValue);

      if (!matches) continue;

      for (const rightRow of matches) {
        yield { ...leftRow, ...rightRow };
      }
    }
  }
}
