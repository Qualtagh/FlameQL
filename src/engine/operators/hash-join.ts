import { ComparisonPredicate, Field } from '../../api/expression';
import { JoinNode } from '../ast';
import { getValueFromField } from '../evaluator';
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
  private hashTable: Map<string, any[]> = new Map();
  private initialized = false;
  private currentLeftRow: any | null = null;
  private currentMatches: any[] | null = null;
  private matchIndex = 0;
  private leftField: Field;
  private rightField: Field;
  private operation: string;
  private buildField: Field;
  private probeField: Field;
  private oriented = false;

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
    this.buildField = this.rightField;
    this.probeField = this.leftField;
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

      const leftValue = getValueFromField(this.currentLeftRow, this.probeField);
      this.currentMatches = this.findMatches(leftValue);
      this.matchIndex = 0;
    }
  }

  private async buildHashTable() {
    let row;
    while (row = await this.rightSource.next()) {
      if (!this.oriented) {
        this.orientFields(row);
      }
      const val = getValueFromField(row, this.buildField);

      if (val !== undefined && val !== null) {
        if ((this.operation === 'in' || this.operation === 'array-contains-any') && Array.isArray(val)) {
          for (const element of val) {
            const key = String(element);
            if (!this.hashTable.has(key)) {
              this.hashTable.set(key, []);
            }
            this.hashTable.get(key)!.push(row);
          }
        } else {
          const key = String(val);
          if (!this.hashTable.has(key)) {
            this.hashTable.set(key, []);
          }
          this.hashTable.get(key)!.push(row);
        }
      }
    }
  }

  private findMatches(leftValue: any): any[] | null {
    if (leftValue === undefined || leftValue === null) {
      return null;
    }

    switch (this.operation) {
      case '==':
      case 'in': {
        // For 'in', leftValue is a scalar, rightValue is an array
        // During buildHashTable, we indexed each element of the right array
        // Now we simply look up the scalar leftValue in the hash table
        const key = String(leftValue);
        return this.hashTable.get(key) || null;
      }
      case 'array-contains':
      case 'array-contains-any':
        // Hash table indexed by right values, look up leftValue
        if (Array.isArray(leftValue)) {
          const matches = new Set<any>();
          for (const element of leftValue) {
            const elementKey = String(element);
            const elementMatches = this.hashTable.get(elementKey);
            if (elementMatches) {
              elementMatches.forEach(m => matches.add(m));
            }
          }
          return matches.size > 0 ? Array.from(matches) : null;
        }
        return null;
      default:
        return null;
    }
  }

  getSortOrder(): SortOrder | undefined {
    return undefined;
  }

  private orientFields(sample: any) {
    const rightHasRightField = this.hasSource(sample, this.rightField);
    const rightHasLeftField = this.hasSource(sample, this.leftField);

    if (rightHasRightField) {
      this.buildField = this.rightField;
      this.probeField = this.leftField;
    } else if (rightHasLeftField) {
      this.buildField = this.leftField;
      this.probeField = this.rightField;
    }

    this.oriented = true;
  }

  private hasSource(row: any, field: Field): boolean {
    return row && field.source in row;
  }

  private ensureField(expr: any): Field {
    if (expr && typeof expr === 'object' && expr.kind === 'Field' && expr.source) {
      return expr as Field;
    }
    throw new Error('Hash join requires Field operands.');
  }
}
