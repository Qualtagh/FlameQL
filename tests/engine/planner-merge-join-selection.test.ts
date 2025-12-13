import { collection, field, projection } from '../../src/api/api';
import { JoinStrategy } from '../../src/api/hints';
import { JoinNode, NodeType, ProjectNode, ScanNode } from '../../src/engine/ast';
import { IndexManager } from '../../src/engine/indexes/index-manager';
import { Planner } from '../../src/engine/planner';

describe('Planner (merge join selection)', () => {
  it('uses NestedLoop for inequality joins when required ordering is not satisfied by indexes', () => {
    const p = projection({
      id: 'mj-no-index',
      from: { u: collection('users'), o: collection('orders') },
      where: { type: 'COMPARISON', left: field('u.id'), right: field('o.userId'), operation: '>' },
      select: { id: field('u.#id') },
    });

    const planner = new Planner(); // empty IndexManager
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.type).toBe(NodeType.PROJECT);
    expect(plan.source.type).toBe(NodeType.JOIN);

    const join = plan.source as JoinNode;
    expect(join.joinType).toBe(JoinStrategy.NestedLoop);
  });

  it('uses MergeJoin for inequality joins when both sides can be scanned ordered by the join keys (exact index match)', () => {
    const indexManager = new IndexManager();
    indexManager.loadFromFirestoreJson(JSON.stringify({
      indexes: [
        { collectionGroup: 'users', queryScope: 'COLLECTION', fields: [{ fieldPath: 'id', order: 'ASCENDING' }] },
        { collectionGroup: 'orders', queryScope: 'COLLECTION', fields: [{ fieldPath: 'userId', order: 'ASCENDING' }] },
      ],
    }));

    const p = projection({
      id: 'mj-with-index',
      from: { u: collection('users'), o: collection('orders') },
      where: { type: 'COMPARISON', left: field('u.id'), right: field('o.userId'), operation: '>' },
      select: { id: field('u.#id') },
    });

    const planner = new Planner(indexManager);
    const plan = planner.plan(p) as ProjectNode;
    const join = plan.source as JoinNode;

    expect(join.joinType).toBe(JoinStrategy.Merge);

    // Planner should push the required ordering down into both scan nodes (so merge join doesn't sort in-memory).
    expect(join.left.type).toBe(NodeType.SCAN);
    expect(join.right.type).toBe(NodeType.SCAN);

    const leftScan = join.left as ScanNode;
    const rightScan = join.right as ScanNode;

    expect(leftScan.orderBy?.[0].field.source).toBe('u');
    expect(leftScan.orderBy?.[0].field.path.join('.')).toBe('id');
    expect(leftScan.orderBy?.[0].direction).toBe('asc');

    expect(rightScan.orderBy?.[0].field.source).toBe('o');
    expect(rightScan.orderBy?.[0].field.path.join('.')).toBe('userId');
    expect(rightScan.orderBy?.[0].direction).toBe('asc');
  });

  it('supports 2 JOINs: merge join output is considered sorted and can feed a second merge join', () => {
    const indexManager = new IndexManager();
    indexManager.loadFromFirestoreJson(JSON.stringify({
      indexes: [
        { collectionGroup: 'users', queryScope: 'COLLECTION', fields: [{ fieldPath: 'id', order: 'ASCENDING' }] },
        { collectionGroup: 'orders', queryScope: 'COLLECTION', fields: [{ fieldPath: 'userId', order: 'ASCENDING' }] },
        { collectionGroup: 'payments', queryScope: 'COLLECTION', fields: [{ fieldPath: 'userId', order: 'ASCENDING' }] },
      ],
    }));

    const p = projection({
      id: 'mj-two-joins',
      from: { u: collection('users'), o: collection('orders'), p: collection('payments') },
      where: {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: field('u.id'), right: field('o.userId'), operation: '>' },
          { type: 'COMPARISON', left: field('u.id'), right: field('p.userId'), operation: '>' },
        ],
      },
      select: { id: field('u.#id') },
    });

    const planner = new Planner(indexManager);
    const plan = planner.plan(p) as ProjectNode;

    expect(plan.source.type).toBe(NodeType.JOIN);
    const topJoin = plan.source as JoinNode;
    expect(topJoin.joinType).toBe(JoinStrategy.Merge);

    // Second join's left input is the first join.
    expect(topJoin.left.type).toBe(NodeType.JOIN);
    const firstJoin = topJoin.left as JoinNode;
    expect(firstJoin.joinType).toBe(JoinStrategy.Merge);

    // First join should have pushed scan ordering into BOTH scans.
    expect(firstJoin.left.type).toBe(NodeType.SCAN);
    expect(firstJoin.right.type).toBe(NodeType.SCAN);
    const uScan = firstJoin.left as ScanNode;
    const oScan = firstJoin.right as ScanNode;

    expect(uScan.alias).toBe('u');
    expect(uScan.orderBy?.[0].field.path.join('.')).toBe('id');
    expect(uScan.orderBy?.[0].direction).toBe('asc');

    expect(oScan.alias).toBe('o');
    expect(oScan.orderBy?.[0].field.path.join('.')).toBe('userId');
    expect(oScan.orderBy?.[0].direction).toBe('asc');

    // Second join should have pushed ordering into the remaining scan, but not needed for the join output side.
    expect(topJoin.right.type).toBe(NodeType.SCAN);
    const pScan = topJoin.right as ScanNode;
    expect(pScan.alias).toBe('p');
    expect(pScan.orderBy?.[0].field.path.join('.')).toBe('userId');
    expect(pScan.orderBy?.[0].direction).toBe('asc');
  });
});
