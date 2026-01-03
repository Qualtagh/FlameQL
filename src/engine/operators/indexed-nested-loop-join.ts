import { Field } from '../../api/expression';
import { IndexedNestedLoopJoinNode } from '../ast';
import { batchRows } from '../codegen/runtime';
import { evaluate, evaluatePredicate } from '../evaluator';
import { IndexedNestedLoopLookupPlan } from '../utils/indexed-nested-loop-utils';
import { maxPerOperation } from '../utils/predicate-utils';
import { Operator, SortOrder } from './operator';
import { PreparedFirestoreScan } from './prepared-firestore-scan';

/**
 * IndexedNestedLoopJoinOperator
 *
 * Uses Firestore index lookups on the RIGHT side, driven by values from the LEFT side.
 *
 * This implementation delegates all Firestore query construction + doc→row mapping
 * to a prepared scan (`PreparedFirestoreScan`) created from the RIGHT plan node.
 */
export class IndexedNestedLoopJoinOperator extends Operator {
  private readonly rightPrepared: PreparedFirestoreScan;
  private readonly driver: IndexedNestedLoopLookupPlan;

  constructor(
    private leftSource: Operator,
    rightSource: Operator,
    private joinNode: IndexedNestedLoopJoinNode,
    private parameters: Record<string, any>
  ) {
    super();
    if (!(rightSource instanceof PreparedFirestoreScan)) {
      throw new Error('Indexed nested-loop join requires a PreparedFirestoreScan operator on the right side.');
    }
    this.rightPrepared = rightSource;

    // Use pre-calculated plan from planner
    const alias = this.rightPrepared.plan.scan.alias;
    this.driver = {
      mode: joinNode.indexJoin.mode,
      leftExpr: joinNode.indexJoin.leftExpr,
      rightField: new Field(alias, this.rightPrepared.plan.driver!.fieldPath.split('.')),
      lookupOp: this.rightPrepared.plan.driver!.op,
    };
  }

  async *[Symbol.asyncIterator]() {
    const generator = this.driver.mode === 'batch'
      ? this.batchModeGenerator()
      : this.perRowModeGenerator();

    yield* generator;
  }

  getSortOrder(): SortOrder | undefined {
    // Preserves the order of the LEFT input stream.
    return this.leftSource.getSortOrder();
  }

  private async *batchModeGenerator() {
    const batchSize = maxPerOperation(this.driver.lookupOp);
    const leftSelector = (row: any) => evaluate(this.driver.leftExpr, row, this.parameters);

    const iterator = this.leftSource[Symbol.asyncIterator]() as AsyncGenerator<any>;

    for await (const leftRows of batchRows(iterator, batchSize)) {
      const lookupValues = leftRows
        .map(row => leftSelector(row))
        .filter(v => v !== undefined && v !== null);

      if (lookupValues.length === 0) continue;

      const rightBuffer: any[] = [];
      for await (const rightRow of this.rightPrepared.run(lookupValues)) {
        rightBuffer.push(rightRow);
      }

      for (const leftRow of leftRows) {
        for (const rightRow of rightBuffer) {
          const row = { ...leftRow, ...rightRow };
          if (evaluatePredicate(this.joinNode.condition, row, this.parameters)) {
            yield row;
          }
        }
      }
    }
  }

  private async *perRowModeGenerator() {
    const leftSelector = (row: any) => evaluate(this.driver.leftExpr, row, this.parameters);

    for await (const leftRow of this.leftSource) {
      const lookupValue = leftSelector(leftRow);

      if (lookupValue === undefined || lookupValue === null) continue;

      for await (const rightRow of this.rightPrepared.run(lookupValue)) {
        const row = { ...leftRow, ...rightRow };
        if (evaluatePredicate(this.joinNode.condition, row, this.parameters)) {
          yield row;
        }
      }
    }
  }
}
