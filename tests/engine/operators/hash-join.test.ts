import { JoinType } from '../../../src/api/hints';
import { JoinNode, NodeType } from '../../../src/engine/ast';
import { HashJoinOperator } from '../../../src/engine/operators/hash-join';
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

describe('HashJoinOperator', () => {
  it('should join using HashJoinStrategy (equality condition)', async () => {
    const left = new MockOperator([
      { id: 1, val: 'a' },
      { id: 2, val: 'b' },
      { id: 1, val: 'c' }, // Duplicate key in left
    ]);
    const right = new MockOperator([
      { id: 1, other: 'x' },
      { id: 3, other: 'y' },
      { id: 1, other: 'z' }, // Duplicate key in right
    ]);

    const node: JoinNode = {
      type: NodeType.JOIN,
      left: {} as any,
      right: {} as any,
      joinType: JoinType.Hash,
      on: { left: 'id', right: 'id' },
    };

    const join = new HashJoinOperator(left, right, node);
    const result = [];
    let row;
    while (row = await join.next()) {
      result.push(row);
    }

    expect(result).toHaveLength(4);
    expect(result).toContainEqual({ id: 1, val: 'a', other: 'x' });
    expect(result).toContainEqual({ id: 1, val: 'a', other: 'z' });
    expect(result).toContainEqual({ id: 1, val: 'c', other: 'x' });
    expect(result).toContainEqual({ id: 1, val: 'c', other: 'z' });
  });

  it('should throw error if condition is not equality', () => {
    const left = new MockOperator([]);
    const right = new MockOperator([]);
    const node: JoinNode = {
      type: NodeType.JOIN,
      left: {} as any,
      right: {} as any,
      joinType: JoinType.Hash,
      on: (_l: any, _r: any) => true as any,
    };

    expect(() => new HashJoinOperator(left, right, node)).toThrow('HashJoin strategy requires an equality condition');
  });
});
