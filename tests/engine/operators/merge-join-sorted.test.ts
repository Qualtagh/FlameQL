import { JoinType } from '../../../src/api/hints';
import { JoinNode, NodeType, ScanNode } from '../../../src/engine/ast';
import { IndexManager } from '../../../src/engine/indexes/index-manager';
import { FirestoreScan } from '../../../src/engine/operators/firestore-scan';
import { MergeJoinOperator } from '../../../src/engine/operators/merge-join';
import { clearDatabase, db } from '../../setup';

describe('Merge Join Optimization', () => {
  let indexManager: IndexManager;

  beforeEach(async () => {
    await clearDatabase();
    indexManager = new IndexManager();
  });

  test('should request sort from FirestoreScan sources', async () => {
    // Setup data
    await db.collection('users').doc('u1').set({ id: 1, name: 'Alice' });
    await db.collection('orders').doc('o1').set({ userId: 1, total: 100 });

    // Create ScanNodes
    const leftScanNode: ScanNode = {
      type: NodeType.SCAN,
      collectionPath: 'users',
      alias: 'u',
      constraints: [],
    };

    const rightScanNode: ScanNode = {
      type: NodeType.SCAN,
      collectionPath: 'orders',
      alias: 'o',
      constraints: [],
    };

    // Create FirestoreScans
    const leftScan = new FirestoreScan(db, leftScanNode, indexManager);
    const rightScan = new FirestoreScan(db, rightScanNode, indexManager);

    // Spy on requestSort
    const leftRequestSortSpy = jest.spyOn(leftScan, 'requestSort');
    const rightRequestSortSpy = jest.spyOn(rightScan, 'requestSort');

    // Create MergeJoinOperator
    const joinNode: JoinNode = {
      type: NodeType.JOIN,
      left: leftScanNode,
      right: rightScanNode,
      joinType: JoinType.Merge,
      condition: {
        type: 'COMPARISON',
        left: 'u.id',
        right: 'o.userId',
        operation: '==',
      },
    };

    const mergeJoin = new MergeJoinOperator(leftScan, rightScan, joinNode);

    // Execute - just get one row to trigger initialization
    await mergeJoin.next();

    // Verify sort requests were made
    expect(leftRequestSortSpy).toHaveBeenCalledWith('u.id', 'asc');
    expect(rightRequestSortSpy).toHaveBeenCalledWith('o.userId', 'asc');
  });

  test('should accept sort requests and return sort order', async () => {
    const scanNode: ScanNode = {
      type: NodeType.SCAN,
      collectionPath: 'users',
      alias: 'u',
      constraints: [],
    };

    const scan = new FirestoreScan(db, scanNode, indexManager);

    // Request sort
    const result = scan.requestSort('id', 'asc');

    // Should return true (accepted)
    expect(result).toBe(true);

    // Should return the sort order
    expect(scan.getSortOrder()).toEqual({ field: 'id', direction: 'asc' });
  });

  test('should reject sort requests after execution started', async () => {
    await db.collection('users').doc('u1').set({ id: 1, name: 'Alice' });

    const scanNode: ScanNode = {
      type: NodeType.SCAN,
      collectionPath: 'users',
      alias: 'u',
      constraints: [],
    };

    const scan = new FirestoreScan(db, scanNode, indexManager);

    // Start execution
    await scan.next();

    // Try to request sort after execution started
    const result = scan.requestSort('id', 'asc');

    // Should return false (rejected)
    expect(result).toBe(false);
  });
});
