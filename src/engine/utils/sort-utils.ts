import { OrderByDirection } from '@google-cloud/firestore';

export interface SortComparator {
  getValue: (row: any) => any;
  direction: OrderByDirection;
}

export function sortBuffer(buffer: any[], comparators: SortComparator[]): void {
  if (buffer.length <= 1 || comparators.length === 0) return;

  buffer.sort((a, b) => {
    for (const cmp of comparators) {
      const left = cmp.getValue(a);
      const right = cmp.getValue(b);
      const dir = cmp.direction === 'asc' ? 1 : -1;
      const result = compareValues(left, right);
      if (result !== 0) return result * dir;
    }
    return 0;
  });
}

export function compareValues(a: any, b: any): number {
  if (a === b) return 0;
  if (a === undefined && b === null) return 0;
  if (a === null && b === undefined) return 0;
  if (a === undefined || a === null) return -1;
  if (b === undefined || b === null) return 1;
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
