import { NodeType, ScanNode } from '../../../src/engine/ast';
import { FirestoreScan } from '../../../src/engine/operators/firestore-scan';
import { DOC_COLLECTION, DOC_ID, DOC_PARENT, DOC_PATH } from '../../../src/engine/symbols';
import { clearDatabase, db } from '../../setup';

describe('FirestoreScan with metadata', () => {
  beforeEach(async () => {
    await clearDatabase();
    // Seed data in a nested collection
    await db.collection('users').doc('user1').set({ name: 'Alice' });
    await db.collection('users').doc('user1').collection('orders').doc('order1').set({ item: 'Book' });
  });

  test('should include metadata fields in root collection', async () => {
    const scanNode = {
      type: NodeType.SCAN,
      collectionPath: 'users',
      alias: 'u',
      constraints: [],
    } as ScanNode;

    const scan = new FirestoreScan(db, scanNode);
    const result = await scan.next();

    expect(result).toBeDefined();
    expect(result.u).toBeDefined();

    // Check metadata fields using symbols
    expect(result.u[DOC_ID]).toBe('user1');
    expect(result.u[DOC_PATH]).toBe('users/user1');
    expect(result.u[DOC_COLLECTION]).toBe('users');
    expect(result.u[DOC_PARENT]).toBeNull();

    // Check regular data field
    expect(result.u.name).toBe('Alice');
  });

  test('should include parent metadata in nested collection', async () => {
    const scanNode = {
      type: NodeType.SCAN,
      collectionPath: 'users/user1/orders',
      alias: 'o',
      constraints: [],
    } as ScanNode;

    const scan = new FirestoreScan(db, scanNode);
    const result = await scan.next();

    expect(result).toBeDefined();
    expect(result.o).toBeDefined();

    // Check metadata fields
    expect(result.o[DOC_ID]).toBe('order1');
    expect(result.o[DOC_PATH]).toBe('users/user1/orders/order1');
    expect(result.o[DOC_COLLECTION]).toBe('orders');

    // Check parent metadata
    const parent = result.o[DOC_PARENT];
    expect(parent).toBeDefined();
    expect(parent![DOC_ID]).toBe('user1');
    expect(parent![DOC_PATH]).toBe('users/user1');
    expect(parent![DOC_COLLECTION]).toBe('users');
    expect(parent![DOC_PARENT]).toBeNull();

    // Check regular data field
    expect(result.o.item).toBe('Book');
  });
});
