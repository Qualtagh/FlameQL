import { getValueFromPath } from '../../src/engine/evaluator';

describe('Evaluator', () => {
  const data = {
    simple: 'value',
    nested: {
      field: 'nestedValue',
      deep: {
        val: 123,
      },
    },
    list: [
      { id: 1, name: 'A' },
      { id: 2, name: 'B' },
    ],
    mixedList: [
      { items: [{ val: 1 }, { val: 2 }] },
      { items: [{ val: 3 }] },
    ],
  };

  test('should access simple field', () => {
    expect(getValueFromPath(data, 'simple')).toBe('value');
  });

  test('should access nested field', () => {
    expect(getValueFromPath(data, 'nested.field')).toBe('nestedValue');
    expect(getValueFromPath(data, 'nested.deep.val')).toBe(123);
  });

  test('should return null for missing field', () => {
    expect(getValueFromPath(data, 'missing')).toBeUndefined();
    expect(getValueFromPath(data, 'nested.missing')).toBeUndefined();
  });

  test('should map over array', () => {
    expect(getValueFromPath(data, 'list.name')).toEqual(['A', 'B']);
  });

  test('should flatten nested arrays', () => {
    expect(getValueFromPath(data, 'mixedList.items.val')).toEqual([1, 2, 3]);
  });
});
