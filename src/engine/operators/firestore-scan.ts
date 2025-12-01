import * as admin from 'firebase-admin';
import { ScanNode } from '../ast';
import { Operator } from './operator';

export class FirestoreScan implements Operator {
  private snapshot: admin.firestore.QuerySnapshot | null = null;
  private index = 0;

  constructor(
    private db: admin.firestore.Firestore,
    private node: ScanNode
  ) { }

  async next(): Promise<any | null> {
    if (!this.snapshot) {
      let query: admin.firestore.Query = this.db.collection(this.node.collectionPath);

      // Apply constraints
      for (const constraint of this.node.constraints) {
        // TODO: Handle field paths correctly
        const fieldPath = constraint.field.path.join('.');
        query = query.where(fieldPath, constraint.op, constraint.value.value);
      }

      this.snapshot = await query.get();
    }

    if (this.index < this.snapshot!.docs.length) {
      const doc = this.snapshot!.docs[this.index++];
      return { [this.node.alias]: { id: doc.id, ...doc.data() } };
    }

    return null;
  }
}
