import { and, collection, eq, field, gte, literal, projection } from '../../../src/api/api';
import { planToCode } from '../../../src/engine/codegen/plan-to-code';
import { Planner } from '../../../src/engine/planner';
import { align } from '../../../src/engine/utils/string-utils';

describe('planToCode', () => {
  it('generates code for join with filter and projection', () => {
    const proj = projection({
      id: 'order-user-query',
      from: { o: collection('orders'), u: collection('users') },
      where: and([
        eq(field('o.status'), literal('active')),
        eq(field('o.userId'), field('u.#id')),
        gte(field('u.age'), literal(18)),
      ]),
      select: {
        orderId: field('o.#id'),
        userName: field('u.name'),
      },
      limit: 10,
    });

    const planner = new Planner();
    const plan = planner.plan(proj);
    const code = planToCode(plan, proj.id);

    const expectedCode = align`
      import { Firestore } from '@google-cloud/firestore';
      import { getData, evaluatePredicate, evaluate, getValue, unionRows, sortRows } from 'flameql/codegen/runtime';

      export async function orderUserQuery(
        db: Firestore,
        params: Record<string, any>
      ): Promise<any[]> {
        async function* scan_o() {
          let query: FirebaseFirestore.Query = db.collection('orders');
          query = query.where('status', '==', 'active');
          for await (const doc of query.stream()) {
            yield { o: getData(doc as any) };
          }
        }

        async function* scan_u() {
          let query: FirebaseFirestore.Query = db.collection('users');
          query = query.where('age', '>=', 18);
          for await (const doc of query.stream()) {
            yield { u: getData(doc as any) };
          }
        }

        async function* join_0() {
          // WARNING: Join strategy 'hash' handled as Nested Loop.
          const rightBuffer: any[] = [];
          for await (const row of scan_u()) {
            rightBuffer.push(row);
          }

          for await (const leftRow of scan_o()) {
            for (const rightRow of rightBuffer) {
              const row = { ...leftRow, ...rightRow };
              if (getValue(row.o, ['userId']) === getValue(row.u, ['#id'])) {
                yield row;
              }
            }
          }
        }

        async function* limit_0() {
          let count = 0;
          for await (const row of join_0()) {
            if (count >= 10) return;
            yield row;
            count++;
          }
        }

        async function* project_0() {
          for await (const row of limit_0()) {
            yield {
              orderId: getValue(row.o, ['#id']),
              userName: getValue(row.u, ['name']),
            };
          }
        }

        // Collect results
        const results: any[] = [];
        for await (const row of project_0()) {
          results.push(row);
        }
        return results;
      }
    `;
    // Normalize newlines for comparison
    expect(code.trim()).toBe(expectedCode.trim());
  });
});
