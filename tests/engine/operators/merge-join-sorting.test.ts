import { eq, field, JoinStrategy } from '../../../src/api/api';
import { JoinNode, NodeType, ScanNode } from '../../../src/engine/ast';
import { MergeJoinOperator } from '../../../src/engine/operators/merge-join';
import { Operator } from '../../../src/engine/operators/operator';

class ArraySource implements Operator {
  constructor(
    private rows: any[]
  ) { }

  async *[Symbol.asyncIterator]() {
    for (const row of this.rows) {
      yield row;
    }
  }
}

describe('MergeJoinOperator (sorting behavior)', () => {
  it('sorts inputs locally when inputs are not declared as sorted by the join keys', async () => {
    const left = new ArraySource([
      { a: { k: 2, left: 'L2' } },
      { a: { k: 1, left: 'L1' } },
    ]);
    const right = new ArraySource([
      { b: { k: 1, right: 'R1' } },
      { b: { k: 2, right: 'R2' } },
    ]);

    const joinNode: JoinNode = {
      type: NodeType.JOIN,
      left: { type: NodeType.SCAN, alias: 'a' } as ScanNode,
      right: { type: NodeType.SCAN, alias: 'b' } as ScanNode,
      joinType: JoinStrategy.Merge,
      condition: eq(field('a.k'), field('b.k')),
    };

    const originalSort = Array.prototype.sort;
    const sortSpy = jest.spyOn(Array.prototype, 'sort').mockImplementation(function (this: any, compareFn?: any) {
      return originalSort.call(this, compareFn);
    });

    const op = new MergeJoinOperator(left, right, joinNode);
    const out: any[] = [];

    for await (const row of op) {
      out.push(row);
    }

    // Both inputs lacked matching sort metadata -> both buffers should have been sorted.
    const sortedArrays = sortSpy.mock.instances
      .filter(arr => Array.isArray(arr))
      .filter(arr => arr.some(x => x && typeof x === 'object' && ('a' in x || 'b' in x)));
    expect(sortedArrays.length).toBeGreaterThanOrEqual(2);

    sortSpy.mockRestore();

    expect(out).toContainEqual({ a: { k: 1, left: 'L1' }, b: { k: 1, right: 'R1' } });
    expect(out).toContainEqual({ a: { k: 2, left: 'L2' }, b: { k: 2, right: 'R2' } });
    expect(out).toHaveLength(2);
  });

  it('does not sort locally when both inputs report being sorted by the join keys (ASC)', async () => {
    const left = new ArraySource([
      { a: { k: 1, left: 'L1' } },
      { a: { k: 2, left: 'L2' } },
    ]);

    const right = new ArraySource([
      { b: { k: 1, right: 'R1' } },
      { b: { k: 2, right: 'R2' } },
    ]);

    const joinNode: JoinNode = {
      type: NodeType.JOIN,
      left: {
        type: NodeType.SCAN,
        alias: 'a',
        orderBy: [{ field: field('a.k'), direction: 'asc' }],
      } as ScanNode,
      right: {
        type: NodeType.SCAN,
        alias: 'b',
        orderBy: [{ field: field('b.k'), direction: 'asc' }],
      } as ScanNode,
      joinType: JoinStrategy.Merge,
      condition: eq(field('a.k'), field('b.k')),
    };

    const originalSort = Array.prototype.sort;
    const sortSpy = jest.spyOn(Array.prototype, 'sort').mockImplementation(function (this: any, compareFn?: any) {
      return originalSort.call(this, compareFn);
    });

    const op = new MergeJoinOperator(left, right, joinNode);
    const out: any[] = [];

    for await (const row of op) {
      out.push(row);
    }

    // The merge join should trust upstream sort metadata and avoid local sorting.
    const sortedArrays = sortSpy.mock.instances
      .filter(arr => Array.isArray(arr))
      .filter(arr => arr.some(x => x && typeof x === 'object' && ('a' in x || 'b' in x)));
    expect(sortedArrays.length).toBe(0);

    sortSpy.mockRestore();
    expect(out).toHaveLength(2);
  });
});
