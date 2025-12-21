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
      if (left === right) continue;
      if (left === undefined && right === null) continue;
      if (left === null && right === undefined) continue;
      if (left === undefined || left === null) return -dir;
      if (right === undefined || right === null) return dir;
      if (left < right) return -dir;
      if (left > right) return dir;
    }
    return 0;
  });
}
