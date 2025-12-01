import * as admin from 'firebase-admin';
import { ScanNode } from '../ast';
import { DOC_COLLECTION, DOC_ID, DOC_PARENT, DOC_PATH, DocumentMetadata } from '../symbols';
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
      const docData = {
        ...createMetadata(doc.ref.path),
        ...doc.data(),
      };
      return { [this.node.alias]: docData };
    }

    return null;
  }
}

/**
 * Creates document metadata from a document path.
 * Recursively creates parent metadata.
 */
function createMetadata(path: string): DocumentMetadata {
  const segments = path.split('/');
  const id = segments[segments.length - 1];
  const collection = segments.length >= 2 ? segments[segments.length - 2] : '';

  // Parent path calculation
  // If segments.length <= 2 (e.g. "coll/doc"), there is no parent document.
  let parentMetadata: DocumentMetadata | null = null;
  if (segments.length > 2) {
    const parentPath = segments.slice(0, -2).join('/');
    parentMetadata = createMetadata(parentPath);
  }

  return {
    [DOC_ID]: id,
    [DOC_PATH]: path,
    [DOC_COLLECTION]: collection,
    [DOC_PARENT]: parentMetadata,
  };
}
