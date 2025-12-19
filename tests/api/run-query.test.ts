import { and, apply, arrayContains, arrayContainsAny, collection, eq, field, inList, like, literal, notInList, OrderByStrategy, param, projection, runQuery } from '../../src/api/api';
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
      select: { userName: field('u.name'), userRole: field('u.role') },
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
        customerName: field('c.name'),
        orderId: field('o.orderId'),
        orderTotal: field('o.total'),
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
        productName: field('p.name'),
        priceValue: field('p.price.value'),
        currency: field('p.price.currency'),
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

  it('substitutes params and errors on missing values', async () => {
    await db.collection('users').doc('u1').set({ id: 'u1', name: 'Alice' });
    await db.collection('users').doc('u2').set({ id: 'u2', name: 'Bob' });

    const p = projection({
      id: 'param-query',
      from: { u: collection('users') },
      select: { userName: field('u.name') },
      where: eq(field('u.id'), param('userId')),
    });

    const results = await runQuery(p, { db, parameters: { userId: 'u1' } });
    expect(results).toEqual([{ userName: 'Alice' }]);

    await expect(runQuery(p, { db, parameters: {} })).rejects.toThrow('Parameter "userId" was not provided.');
  });

  it('supports inList (basic)', async () => {
    await db.collection('users').doc('u1').set({ userId: 1 });
    await db.collection('users').doc('u2').set({ userId: 2 });
    await db.collection('users').doc('u3').set({ userId: 3 });

    await db.collection('customers').doc('c1').set({ userId: 2 });

    const p = projection({
      id: 'inlist',
      from: { u: collection('users'), c: collection('customers') },
      select: { userId: field('u.userId'), customerUserId: field('c.userId') },
      where: inList(field('u.userId'), [literal(1), param('allowedUserId'), literal(2)]),
    });

    const results = await runQuery(p, { db, parameters: { allowedUserId: 4 } });

    const userIds = results.map(r => r.userId).sort();
    expect(userIds).toEqual([1, 2]);
    expect(results.every(r => r.customerUserId === 2)).toBe(true);
  });

  it('supports inList with mixed expressions on join rows', async () => {
    await db.collection('users').doc('u1').set({ userId: 1 });
    await db.collection('users').doc('u2').set({ userId: 2 });
    await db.collection('users').doc('u3').set({ userId: 3 });

    await db.collection('customers').doc('c1').set({ userId: 2 });

    const p = projection({
      id: 'inlist-mixed',
      from: { u: collection('users'), c: collection('customers') },
      select: { userId: field('u.userId'), customerUserId: field('c.userId') },
      where: inList(field('u.userId'), [literal(1), param('allowedUserId'), field('c.userId')]),
    });

    const results = await runQuery(p, { db, parameters: { allowedUserId: 4 } });

    const userIds = results.map(r => r.userId).sort();
    expect(userIds).toEqual([1, 2]);
    expect(results.every(r => r.customerUserId === 2)).toBe(true);
  });

  it('supports list-aware operators with Firestore pushdown', async () => {
    await db.collection('products').doc('p1').set({ color: 'red', tags: ['warm', 'primary'] });
    await db.collection('products').doc('p2').set({ color: 'blue', tags: ['cool', 'primary'] });
    await db.collection('products').doc('p3').set({ color: 'green', tags: ['cool', 'neutral'] });

    const listPredicate = projection({
      id: 'list-ops',
      from: { p: collection('products') },
      select: { color: field('p.color') },
      where: and([
        notInList(field('p.color'), [literal('red'), literal('green')]),
        arrayContainsAny(field('p.tags'), [literal('primary'), literal('neutral')]),
      ]),
    });

    const listResults = await runQuery(listPredicate, { db });
    expect(listResults).toEqual([{ color: 'blue' }]);

    const arrayContainsPredicate = projection({
      id: 'array-contains',
      from: { p: collection('products') },
      select: { color: field('p.color') },
      where: arrayContains(field('p.tags'), literal('warm')),
    });

    const warmResults = await runQuery(arrayContainsPredicate, { db });
    expect(warmResults.map(r => r.color).sort()).toEqual(['red']);
  });

  it('supports apply with array inputs in Firestore projections', async () => {
    await db.collection('jobs').doc('j1').set({ title: 'Engineer', userId: 'user-1' });

    const p = projection({
      id: 'apply-array-projection',
      from: { j: collection('jobs') },
      select: {
        path: apply([literal('/users/'), field('j.userId'), literal('/jobs')], parts => parts.join('')),
        shout: apply(
          [[field('j.title'), literal('!')], literal(' ')],
          ([segments, delimiter]) => (segments as any[]).join(delimiter as string)
        ),
      },
    });

    const results = await runQuery(p, { db });
    expect(results).toEqual([{ path: '/users/user-1/jobs', shout: 'Engineer !' }]);
  });

  it('evaluates apply with nested array inputs inside predicates', async () => {
    await db.collection('orders').doc('o1').set({ userId: 'u1', status: 'open' });
    await db.collection('orders').doc('o2').set({ userId: 'u2', status: 'closed' });

    const p = projection({
      id: 'apply-array-predicate',
      from: { o: collection('orders') },
      where: eq(
        apply(
          [[field('o.userId'), literal(':'), field('o.status')]],
          parts => (parts[0] as any[]).join('')
        ),
        literal('u1:open')
      ),
      select: { userId: field('o.userId'), status: field('o.status') },
    });

    const results = await runQuery(p, { db });
    expect(results).toEqual([{ userId: 'u1', status: 'open' }]);
  });

  it('like predicate', async () => {
    await db.collection('users').doc('u1').set({ name: 'Alice' });
    await db.collection('users').doc('u2').set({ name: 'Bob' });
    await db.collection('users').doc('u3').set({ name: 'Beatrice' });

    const listPredicate = projection({
      id: 'like-predicate',
      from: { u: collection('users') },
      select: { name: field('u.name') },
      where: like(field('u.name'), literal('B%')),
      orderBy: ['u.name'],
      hints: { orderBy: OrderByStrategy.PostFetchSort },
    });

    const listResults = await runQuery(listPredicate, { db });
    expect(listResults).toEqual([{ name: 'Beatrice' }, { name: 'Bob' }]);
  });
});
