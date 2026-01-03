import * as admin from 'firebase-admin';
import { ScanNode } from '../ast';
import { Operator } from './operator';
import { PreparedFirestoreScan } from './prepared-firestore-scan';

export class FirestoreScan implements Operator {
  private prepared: PreparedFirestoreScan;

  constructor(
    db: admin.firestore.Firestore,
    node: ScanNode,
    parameters: Record<string, any>
  ) {
    this.prepared = new PreparedFirestoreScan(db, node, parameters);
    this.prepared.setOptions({
      includeScanOrderBy: true,
      includeScanLimitOffset: true,
    });
  }

  async *[Symbol.asyncIterator]() {
    yield* this.prepared;
  }
}
