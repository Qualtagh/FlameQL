import { field, literal } from '../../src/api/api';
import { Predicate } from '../../src/api/expression';
import { IndexManager } from '../../src/engine/indexes/index-manager';
import { Optimizer } from '../../src/engine/optimizer';

describe('Optimizer', () => {
  const indexManager = new IndexManager();

  indexManager.addIndex({
    collectionId: 'users',
    queryScope: 'COLLECTION',
    fields: [
      { fieldPath: 'age', mode: 'ASCENDING' },
      { fieldPath: 'city', mode: 'ASCENDING' },
    ],
  });

  indexManager.addIndex({
    collectionId: 'users',
    queryScope: 'COLLECTION',
    fields: [
      { fieldPath: 'status', mode: 'ASCENDING' },
      { fieldPath: 'role', mode: 'ASCENDING' },
    ],
  });

  const optimizer = new Optimizer(indexManager);

  test('should prefer single scan when index matches simple AND', () => {
    // age == 25 AND city == 'NY'
    const predicate: Predicate = {
      type: 'AND',
      conditions: [
        { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
        { type: 'COMPARISON', left: field('u.city'), operation: '==', right: literal('NY') },
      ],
    };

    const result = optimizer.optimize(predicate, 'users');

    expect(result.strategy).toBe('SINGLE_SCAN');
    expect(result.scans.length).toBe(1);
  });

  test('should prefer union scan when DNF matches multiple indexes', () => {
    // (age == 25 AND city == 'NY') OR (status == 'active' AND role == 'admin')
    const predicate: Predicate = {
      type: 'OR',
      conditions: [
        {
          type: 'AND',
          conditions: [
            { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
            { type: 'COMPARISON', left: field('u.city'), operation: '==', right: literal('NY') },
          ],
        },
        {
          type: 'AND',
          conditions: [
            { type: 'COMPARISON', left: field('u.status'), operation: '==', right: literal('active') },
            { type: 'COMPARISON', left: field('u.role'), operation: '==', right: literal('admin') },
          ],
        },
      ],
    };

    const result = optimizer.optimize(predicate, 'users');

    expect(result.strategy).toBe('UNION_SCAN');
    expect(result.scans.length).toBe(2);
  });

  test('should fallback to single scan (full scan) if no index matches', () => {
    // name == 'John' (no index on name)
    const predicate: Predicate = {
      type: 'COMPARISON', left: field('u.name'), operation: '==', right: literal('John'),
    };

    const result = optimizer.optimize(predicate, 'users');

    expect(result.strategy).toBe('SINGLE_SCAN');
    // Score should be high, but strategy is single scan because DNF doesn't help either
  });

  test('should handle complex DNF conversion', () => {
    // age == 25 AND (city == 'NY' OR city == 'LA')
    // Should become (age==25 AND city=='NY') OR (age==25 AND city=='LA')
    // Both branches match the (age, city) index.
    const predicate: Predicate = {
      type: 'AND',
      conditions: [
        { type: 'COMPARISON', left: field('u.age'), operation: '==', right: literal(25) },
        {
          type: 'OR',
          conditions: [
            { type: 'COMPARISON', left: field('u.city'), operation: '==', right: literal('NY') },
            { type: 'COMPARISON', left: field('u.city'), operation: '==', right: literal('LA') },
          ],
        },
      ],
    };

    const result = optimizer.optimize(predicate, 'users');

    expect(result.strategy).toBe('UNION_SCAN');
    expect(result.scans.length).toBe(2);
  });
});
