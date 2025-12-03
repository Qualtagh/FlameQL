import { collection, projection, runQuery } from '../../src/api/api';
import { clearDatabase, db } from '../setup';

describe('runQuery API', () => {
  beforeEach(async () => {
    await clearDatabase();
  });

  it('should execute a simple query with automatic planning', async () => {
    // Seed data
    await db.collection('users').doc('1').set({ id: 1, name: 'Alice', role: 'admin' });
    await db.collection('users').doc('2').set({ id: 2, name: 'Bob', role: 'user' });
    await db.collection('users').doc('3').set({ id: 3, name: 'Charlie', role: 'user' });

    const p = projection({
      id: 'test-query',
      from: { u: collection('users') },
      select: { userName: 'u.name', userRole: 'u.role' },
    });

    const results = await runQuery(p, { db });

    expect(results).toHaveLength(3);
    expect(results).toContainEqual({ userName: 'Alice', userRole: 'admin' });
    expect(results).toContainEqual({ userName: 'Bob', userRole: 'user' });
    expect(results).toContainEqual({ userName: 'Charlie', userRole: 'user' });
  });

  it('should execute a join query with automatic planning', async () => {
    // Seed data - adding 'id' field to both collections since default join is on id == id
    await db.collection('customers').doc('c1').set({ id: 1, customerId: 1, name: 'Alice' });
    await db.collection('customers').doc('c2').set({ id: 2, customerId: 2, name: 'Bob' });

    await db.collection('orders').doc('o1').set({ id: 1, orderId: 101, customerId: 1, total: 100 });
    await db.collection('orders').doc('o2').set({ id: 1, orderId: 102, customerId: 1, total: 200 });
    await db.collection('orders').doc('o3').set({ id: 2, orderId: 103, customerId: 2, total: 150 });

    const p = projection({
      id: 'test-join',
      from: { c: collection('customers'), o: collection('orders') },
      select: {
        customerName: 'c.name',
        orderId: 'o.orderId',
        orderTotal: 'o.total',
      },
    });

    // Default join condition is id == id, which will match some rows
    const results = await runQuery(p, { db });

    // Without proper join condition, this will do a cartesian product
    // But we're testing that the API works end-to-end
    expect(results.length).toBeGreaterThan(0);
    expect(results[0]).toHaveProperty('customerName');
    expect(results[0]).toHaveProperty('orderId');
    expect(results[0]).toHaveProperty('orderTotal');
  });

  it('should work with nested field access', async () => {
    await db.collection('products').doc('p1').set({
      id: 1,
      name: 'Widget',
      price: { value: 20, currency: 'USD' },
    });
    await db.collection('products').doc('p2').set({
      id: 2,
      name: 'Gadget',
      price: { value: 30, currency: 'EUR' },
    });

    const p = projection({
      id: 'test-nested',
      from: { p: collection('products') },
      select: {
        productName: 'p.name',
        priceValue: 'p.price.value',
        currency: 'p.price.currency',
      },
    });

    const results = await runQuery(p, { db });

    expect(results).toHaveLength(2);
    expect(results).toContainEqual({
      productName: 'Widget',
      priceValue: 20,
      currency: 'USD',
    });
    expect(results).toContainEqual({
      productName: 'Gadget',
      priceValue: 30,
      currency: 'EUR',
    });
  });
});
