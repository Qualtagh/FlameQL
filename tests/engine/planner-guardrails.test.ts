import { collection, field, literal, projection } from '../../src/api/api';
import { JoinStrategy } from '../../src/api/hints';
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
      where: {
        type: 'AND',
        conditions: [
          { type: 'COMPARISON', left: field('j.age'), right: literal(10), operation: '>' },
          { type: 'COMPARISON', left: field('j.score'), right: literal(5), operation: '<' },
        ],
      },
      select: { id: field('j.#id') },
    });

    const planner = new Planner();
    expect(() => planner.plan(p)).toThrow('Firestore allows at most one inequality field per query');
  });

  it('rejects orderBy that does not match inequality field', () => {
    const p = projection({
      id: 'ineq-order',
      from: { j: collection('jobs') },
      where: { type: 'COMPARISON', left: field('j.age'), right: literal(10), operation: '>' },
      orderBy: ['j.title'],
      select: { id: field('j.#id') },
    });

    const planner = new Planner();
    expect(() => planner.plan(p)).toThrow('first orderBy field must match the inequality field');
  });

  it('rejects incompatible join hint', () => {
    const p = projection({
      id: 'bad-merge-hint',
      from: { p: collection('posts'), s: collection('searches') },
      where: { type: 'COMPARISON', left: field('p.tags'), right: field('s.tag'), operation: 'array-contains' },
      select: { tag: field('s.tag') },
      hints: { join: JoinStrategy.Merge },
    });

    const planner = new Planner();
    expect(() => planner.plan(p)).toThrow('Merge join hint is incompatible with the provided join predicate.');
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
    await executor.execute(plan);
    expect(warnSpy).toHaveBeenCalled();
    warnSpy.mockRestore();
  });
});
