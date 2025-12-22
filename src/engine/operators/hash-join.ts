import { WhereFilterOp } from '@google-cloud/firestore';
import { ComparisonPredicate, Field } from '../../api/expression';
import { JoinNode } from '../ast';
import { getValueFromField } from '../evaluator';
import { JoinHashTable } from '../utils/hash-join-utils';
import { isHashJoinCompatible } from '../utils/operation-comparator';
import { Operator, SortOrder } from './operator';

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
  private hashTable: JoinHashTable | null = null;
  private initialized = false;
  private currentLeftRow: any | null = null;
  private currentMatches: any[] | null = null;
  private matchIndex = 0;
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
    this.leftField = this.ensureField(condition.left);
    this.rightField = this.ensureField(condition.right);
  }

  async next(): Promise<any | null> {
    if (!this.initialized) {
      await this.buildHashTable();
      this.initialized = true;
    }

    while (true) {
      if (this.currentMatches && this.matchIndex < this.currentMatches.length) {
        const rightRow = this.currentMatches[this.matchIndex++];
        return { ...this.currentLeftRow, ...rightRow };
      }

      this.currentLeftRow = await this.leftSource.next();
      if (!this.currentLeftRow) return null;

      const leftValue = getValueFromField(this.currentLeftRow, this.leftField);
      this.currentMatches = this.hashTable!.get(leftValue);
      this.matchIndex = 0;
    }
  }

  private async buildHashTable() {
    this.hashTable = new JoinHashTable(this.operation);
    let row;
    while (row = await this.rightSource.next()) {
      const val = getValueFromField(row, this.rightField);
      this.hashTable.add(val, row);
    }
  }

  getSortOrder(): SortOrder | undefined {
    // Hash join preserves the order of the LEFT input stream.
    return this.leftSource.getSortOrder();
  }

  private ensureField(expr: any): Field {
    if (expr && typeof expr === 'object' && expr.kind === 'Field' && expr.source) {
      return expr as Field;
    }
    throw new Error('Hash join requires Field operands.');
  }
}
