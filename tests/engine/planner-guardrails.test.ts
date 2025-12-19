import { and, arrayContains, collection, field, gt, JoinStrategy, literal, lt, projection } from '../../src/api/api';
import { Executor } from '../../src/engine/executor';
import { Planner } from '../../src/engine/planner';
import { clearDatabase, db } from '../setup';

describe('Planner guardrails', () => {
  beforeEach(async () => {
    await clearDatabase();
  });

  it('rejects multiple inequality fields in a single scan', () => {
    const p = projection({
      id: 'ineq-multi',
      from: { j: collection('jobs') },
      where: and([
        gt(field('j.age'), literal(10)),
        lt(field('j.score'), literal(5)),
      ]),
      select: { id: field('j.#id') },
    });

    const planner = new Planner();
    expect(() => planner.plan(p)).toThrow('Firestore allows at most one inequality field per query');
  });

  it('rejects orderBy that does not match inequality field', () => {
    const p = projection({
      id: 'ineq-order',
      from: { j: collection('jobs') },
      where: gt(field('j.age'), literal(10)),
      orderBy: [field('j.title')],
      select: { id: field('j.#id') },
    });

    const planner = new Planner();
    expect(() => planner.plan(p)).toThrow('first orderBy field must match the inequality field');
  });

  it('rejects incompatible join hint', () => {
    const p = projection({
      id: 'bad-merge-hint',
      from: { p: collection('posts'), s: collection('searches') },
      where: arrayContains(field('p.tags'), field('s.tag')),
      select: { tag: field('s.tag') },
      hints: { join: JoinStrategy.Merge },
    });

    const planner = new Planner();
    expect(() => planner.plan(p)).toThrow('Merge join hint is incompatible with the provided join predicate.');
  });

  it('accepts indexed nested-loop join hint for inequality join predicates', () => {
    const p = projection({
      id: 'indexed-nl-hint-ineq',
      from: { u: collection('users'), o: collection('orders') },
      where: gt(field('u.id'), field('o.userId')),
      select: { id: field('u.#id') },
      hints: { join: JoinStrategy.IndexedNestedLoop },
    });

    const planner = new Planner();
    expect(() => planner.plan(p)).not.toThrow();
  });

  it('emits a warning for cross-product joins', async () => {
    const p = projection({
      id: 'cross-product',
      from: { a: collection('users'), b: collection('orders') },
      select: { aId: field('a.#id'), bId: field('b.#id') },
    });

    const planner = new Planner();
    const plan = planner.plan(p);

    const warnSpy = jest.spyOn(console, 'log').mockImplementation(() => { });
    const executor = new Executor(db);
    await executor.execute(plan, {});
    expect(warnSpy).toHaveBeenCalled();
    warnSpy.mockRestore();
  });
});
