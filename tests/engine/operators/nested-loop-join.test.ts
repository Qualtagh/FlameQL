import { JoinType } from '../../../src/api/hints';
import { JoinNode, NodeType } from '../../../src/engine/ast';
import { NestedLoopJoinOperator } from '../../../src/engine/operators/nested-loop-join';
import { Operator } from '../../../src/engine/operators/operator';

class MockOperator implements Operator {
  constructor(private data: any[]) { }
  private index = 0;
  async next(): Promise<any | null> {
    if (this.index < this.data.length) {
      return this.data[this.index++];
    }
    return null;
  }
}

describe('NestedLoopJoinOperator', () => {
  it('should join using NestedLoopStrategy (function condition)', async () => {
    const left = new MockOperator([{ id: 1, val: 'a' }, { id: 2, val: 'b' }]);
    const right = new MockOperator([{ id: 1, other: 'x' }, { id: 3, other: 'y' }]);
    const node: JoinNode = {
      type: NodeType.JOIN,
      left: {} as any,
      right: {} as any,
      joinType: JoinType.NestedLoop,
      on: (l: any, r: any) => l.id === r.id,
    };

    const join = new NestedLoopJoinOperator(left, right, node);
    const result = [];
    let row;
    while (row = await join.next()) {
      result.push(row);
    }

    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ id: 1, val: 'a', other: 'x' });
  });
});
