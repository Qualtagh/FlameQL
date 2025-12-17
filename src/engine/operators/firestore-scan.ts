import * as admin from 'firebase-admin';
import { ScanNode } from '../ast';
import { IndexManager } from '../indexes/index-manager';
import { Operator, SortOrder } from './operator';
import { PreparedFirestoreCursor, PreparedFirestoreScan } from './prepared-firestore-scan';

export class FirestoreScan implements Operator {
  private cursor: PreparedFirestoreCursor | null = null;
  private sortOrder?: SortOrder;

  constructor(
    private db: admin.firestore.Firestore,
    private node: ScanNode,
    private indexManager?: IndexManager
  ) { }

  async next(): Promise<any | null> {
    if (!this.cursor) {
      if (this.node.orderBy && this.node.orderBy.length > 0) {
        this.sortOrder = {
          // Operators reason about sort order on the *row stream*, which is aliased:
          // `{ [alias]: docData }`. Use alias-qualified field refs for consistency with
          // `Sort` and `MergeJoinOperator`.
          field: `${this.node.alias}.${this.node.orderBy[0].field.path.join('.')}`,
          direction: this.node.orderBy[0].direction,
        };
      }

      const prepared = new PreparedFirestoreScan(this.db, this.node);
      this.cursor = prepared.createCursor({
        includeBaseWhere: true,
        includeScanOrderBy: true,
        includeScanLimitOffset: true,
      });
    }

    return this.cursor.next();
  }

  getSortOrder(): SortOrder | undefined {
    return this.sortOrder;
  }
}
