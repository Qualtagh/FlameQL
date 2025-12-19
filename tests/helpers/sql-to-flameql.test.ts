import { and, collection, eq, field, gte, literal, or, projection } from '../../src/api/api';
import { translateSqlToFlame } from './sql-to-flameql';

const schema = {
  users: ['id', 'name', 'role', 'active'],
  orders: ['id', 'user_id', 'total'],
};

describe('translateSqlToFlame', () => {
  it('parses where/order/limit/offset', () => {
    const sql = `
      SELECT u.id AS id, u.name AS name
      FROM users u
      WHERE u.role = 'user' AND u.active = 1
      ORDER BY u.name DESC
      LIMIT 5
      OFFSET 2
    `;
    const t = translateSqlToFlame(sql, schema);
    const expected = projection({
      id: 'expected-1',
      select: { id: field('u.id'), name: field('u.name') },
      from: { u: collection('users') },
      where: and([
        eq(field('u.role'), literal('user')),
        eq(field('u.active'), literal(1)),
      ]),
      orderBy: [{ field: field('u.name'), direction: 'desc' }],
      limit: 5,
      offset: 2,
    });

    expect(normalizeProjection(t.projection)).toEqual(normalizeProjection(expected));
  });

  it('parses joins and select aliases', () => {
    const sql = `
      SELECT u.id as uid, o.total
      FROM users u
      JOIN orders o
        ON u.id = o.user_id
      WHERE o.total >= 100
    `;
    const t = translateSqlToFlame(sql, schema);
    const expected = projection({
      id: 'expected-2',
      select: { uid: field('u.id'), 'o.total': field('o.total') },
      from: { u: collection('users'), o: collection('orders') },
      where: and([
        eq(field('u.id'), field('o.user_id')),
        gte(field('o.total'), literal(100)),
      ]),
    });

    expect(normalizeProjection(t.projection)).toEqual(normalizeProjection(expected));
  });

  it('handles IN lists', () => {
    const sql = `
      SELECT u.id as id
      FROM users u
      WHERE u.id IN (1, 2, 3)
      ORDER BY u.id
    `;
    const t = translateSqlToFlame(sql, schema);
    const expected = projection({
      id: 'expected-3',
      select: { id: field('u.id') },
      from: { u: collection('users') },
      where: or([
        eq(field('u.id'), literal(1)),
        eq(field('u.id'), literal(2)),
        eq(field('u.id'), literal(3)),
      ]),
      orderBy: [{ field: field('u.id'), direction: 'asc' }],
    });

    expect(normalizeProjection(t.projection)).toEqual(normalizeProjection(expected));
  });
});

function normalizeProjection(p: any) {
  const clone = JSON.parse(JSON.stringify(p));
  delete clone.id;
  return clone;
}
