import { Constraint } from '../ast';
import { SortOrder } from '../operators/operator';
import { Index, IndexField, IndexFieldMode, QueryScope } from './index-definitions';

export interface FirestoreIndexJson {
  indexes?: Array<{
    collectionGroup: string;
    queryScope: string;
    fields: Array<{
      fieldPath: string;
      order?: string;
      arrayConfig?: string;
    }>;
  }>;
  fieldOverrides?: unknown[];
}

export interface IndexMatch {
  type: 'exact' | 'partial' | 'none';
  index: Index | null;
  matchedFields: number;
}

export class IndexManager {
  private indexes: Index[] = [];

  addIndex(index: Index) {
    this.indexes.push(index);
  }

  getIndexes(collectionId: string): Index[] {
    return this.indexes.filter(
      (index) => index.collectionId === collectionId
    );
  }

  loadFromFirestoreJson(jsonContent: string) {
    const parsed: FirestoreIndexJson = JSON.parse(jsonContent);
    this.loadIndexes(parsed);
  }

  loadIndexes(parsed: FirestoreIndexJson) {
    if (parsed.indexes) {
      for (const idx of parsed.indexes) {
        const fields: IndexField[] = idx.fields.map((f) => {
          let mode: IndexFieldMode = 'ASCENDING';
          if (f.order === 'DESCENDING') {
            mode = 'DESCENDING';
          } else if (f.arrayConfig === 'CONTAINS') {
            mode = 'ARRAY_CONTAINS';
          }
          return {
            fieldPath: f.fieldPath,
            mode: mode,
          };
        });

        this.addIndex({
          collectionId: idx.collectionGroup,
          queryScope: idx.queryScope as QueryScope,
          fields: fields,
        });
      }
    }
  }

  /**
   * Finds the best matching index for the given constraints and sort order.
   */
  match(collectionId: string, constraints: Constraint[], sortOrder?: SortOrder): IndexMatch {
    const indexes = this.getIndexes(collectionId);
    let bestMatch: IndexMatch = { type: 'none', index: null, matchedFields: 0 };

    // Extract equality and inequality fields from constraints
    const equalityFields = new Set<string>();
    const inequalityFields = new Set<string>();

    for (const c of constraints) {
      const fieldPath = c.field.path.join('.');
      if (c.op === '==' || c.op === 'array-contains') {
        equalityFields.add(fieldPath);
      } else {
        inequalityFields.add(fieldPath);
      }
    }

    for (const index of indexes) {
      let indexIdx = 0;
      let equalityMatched = 0;
      let inequalityMatched = false;
      let sortMatched = false;
      let fieldsUsed = 0;

      // 1. Match Equality Fields (prefix)
      for (; indexIdx < index.fields.length; indexIdx++) {
        const field = index.fields[indexIdx];
        if (equalityFields.has(field.fieldPath)) {
          equalityMatched++;
          fieldsUsed++;
        } else {
          break;
        }
      }

      // 2. Match Inequality Field (if any)
      if (inequalityFields.size > 0) {
        const ineqField = Array.from(inequalityFields)[0];
        if (indexIdx < index.fields.length && index.fields[indexIdx].fieldPath === ineqField) {
          inequalityMatched = true;
          fieldsUsed++;
          indexIdx++;
        }
      } else {
        inequalityMatched = true; // No inequality to match
      }

      // 3. Match Sort Order
      if (sortOrder) {
        // If we matched inequality, the sort order MUST be on the same field (Firestore limitation).
        // If so, it's implicitly matched.
        if (inequalityFields.size > 0 && inequalityMatched) {
          const ineqField = Array.from(inequalityFields)[0];
          if (sortOrder.field === ineqField) {
            const field = index.fields[indexIdx - 1];
            if (field.mode === 'ASCENDING' && sortOrder.direction === 'asc' ||
              field.mode === 'DESCENDING' && sortOrder.direction === 'desc') {
              sortMatched = true;
            }
          }
        } else {
          // No inequality, or inequality not matched by this index.
          // Check if next index field matches sort.
          if (indexIdx < index.fields.length) {
            const field = index.fields[indexIdx];
            if (field.fieldPath === sortOrder.field) {
              if (field.mode === 'ASCENDING' && sortOrder.direction === 'asc' ||
                field.mode === 'DESCENDING' && sortOrder.direction === 'desc') {
                sortMatched = true;
                fieldsUsed++;
              }
            }
          }
        }
      } else {
        sortMatched = true; // No sort to match
      }

      // Determine type
      let type: 'exact' | 'partial' | 'none' = 'none';

      const hasMatch = equalityMatched > 0 || inequalityFields.size > 0 && inequalityMatched || sortOrder && sortMatched;

      if (hasMatch) {
        if (equalityMatched === equalityFields.size &&
          (inequalityFields.size === 0 || inequalityMatched) &&
          (!sortOrder || sortMatched)) {
          type = 'exact';
        } else {
          type = 'partial';
        }
      }

      if (type !== 'none') {
        // Prefer exact over partial
        if (bestMatch.type === 'none' || type === 'exact' && bestMatch.type === 'partial') {
          bestMatch = { type, index, matchedFields: fieldsUsed };
        } else if (type === bestMatch.type && fieldsUsed > bestMatch.matchedFields) {
          // Prefer more matched fields
          bestMatch = { type, index, matchedFields: fieldsUsed };
        }
      }
    }

    return bestMatch;
  }
}
