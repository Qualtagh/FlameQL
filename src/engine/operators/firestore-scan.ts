import * as admin from 'firebase-admin';
import { ScanNode } from '../ast';
import { Operator, SortOrder } from './operator';
import { PreparedFirestoreScan } from './prepared-firestore-scan';

export class FirestoreScan extends Operator {
  private prepared: PreparedFirestoreScan;

  constructor(
    private db: admin.firestore.Firestore,
    private node: ScanNode,
    private parameters: Record<string, any>
  ) {
    super();
    this.prepared = new PreparedFirestoreScan(db, node, parameters);
    this.prepared.setOptions({
      includeScanOrderBy: true,
      includeScanLimitOffset: true,
    });
  }

  async next(): Promise<any | null> {
    return this.prepared.next();
  }

  getSortOrder(): SortOrder | undefined {
    return this.prepared.getSortOrder();
  }
}
