import { runSqlCompatCase, type SqlCase } from '../helpers/sql-compat';
import { clearDatabase } from '../setup';

const cases: SqlCase[] = [
  {
    name: 'filters and ordering',
    fixture: `
      CREATE TABLE users (id INTEGER, name TEXT, role TEXT, active INTEGER);
      INSERT INTO users VALUES
        (1, 'Alice', 'admin', 1),
        (2, 'Bob', 'user', 0),
        (3, 'Carol', 'user', 1);
    `,
    query: `
      SELECT id, name
      FROM users
      WHERE role = 'user'
        AND active = 1
      ORDER BY name
    `,
  },
  {
    name: 'joins with limit/offset',
    fixture: `
      CREATE TABLE users (id INTEGER, name TEXT);
      CREATE TABLE orders (id INTEGER, user_id INTEGER, total INTEGER);
      INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
      INSERT INTO orders VALUES (101, 1, 50), (102, 1, 200), (103, 2, 150), (104, 3, 75);
    `,
    query: `
      SELECT u.id, u.name, o.total
      FROM users u
      JOIN orders o
        ON u.id = o.user_id
      WHERE o.total >= 100
      ORDER BY o.total DESC
      LIMIT 2
      OFFSET 0
    `,
  },
  {
    name: 'in-list and pagination',
    fixture: `
      CREATE TABLE products (id INTEGER, name TEXT, price INTEGER);
      INSERT INTO products VALUES (1, 'Widget', 10), (2, 'Gadget', 20), (3, 'Thing', 30);
    `,
    query: `
      SELECT id, name
      FROM products
      WHERE id IN (2, 3)
      ORDER BY id
      LIMIT 1
      OFFSET 1
    `,
  },
  {
    name: 'group by count (expected to surface missing aggregate support)',
    fixture: `
      CREATE TABLE users (id INTEGER, role TEXT);
      INSERT INTO users VALUES (1, 'admin'), (2, 'user'), (3, 'user'), (4, 'guest');
    `,
    query: `
      SELECT role, COUNT(*) as total
      FROM users
      GROUP BY role
      ORDER BY role
    `,
    // TODO: Add support for group by
    expectFailure: true,
  },
];

describe('SQL compatibility via translation to FlameQL', () => {
  for (const testCase of cases) {
    it(testCase.name, async () => {
      await clearDatabase();

      if (testCase.expectFailure) {
        await expect(runSqlCompatCase(testCase)).rejects.toThrow();
        return;
      }

      const result = await runSqlCompatCase(testCase);
      expect(result.flameRows).toEqual(result.sqlRows);
    });
  }
});
