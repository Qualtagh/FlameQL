import { WhereFilterOp } from '@google-cloud/firestore';

/**
 * Helper class for Hash Join operations.
 * Encapsulates the logic for building the hash table and finding matches
 * based on different operations (==, in, array-contains, etc.).
 */
export class JoinHashTable {
  private hashTable: Map<string, any[]> = new Map();

  constructor(private operation: WhereFilterOp) { }

  /**
   * Adds a row to the hash table based on the key value.
   * Handles array expansion for 'in' and 'array-contains-any' operations.
   */
  add(key: any, row: any) {
    if (key === undefined || key === null) return;
    if ((this.operation === 'in' || this.operation === 'array-contains-any') && Array.isArray(key)) {
      for (const element of key) {
        this.insert(String(element), row);
      }
    } else {
      this.insert(String(key), row);
    }
  }

  private insert(key: string, row: any) {
    if (!this.hashTable.has(key)) {
      this.hashTable.set(key, []);
    }
    this.hashTable.get(key)!.push(row);
  }

  /**
   * Finds matches for a probe value.
   * Handles array expansion for 'array-contains' and 'array-contains-any'.
   */
  get(probeValue: any): any[] | null {
    if (probeValue === undefined || probeValue === null) return null;
    switch (this.operation) {
      case '==':
      case 'in': {
        const key = String(probeValue);
        return this.hashTable.get(key) || null;
      }
      case 'array-contains':
      case 'array-contains-any': {
        const matches = new Set<any>();
        for (const element of probeValue) {
          const elementKey = String(element);
          const elementMatches = this.hashTable.get(elementKey);
          if (elementMatches) {
            elementMatches.forEach(m => matches.add(m));
          }
        }
        return matches.size > 0 ? Array.from(matches) : null;
      }
      default:
        return null;
    }
  }
}
