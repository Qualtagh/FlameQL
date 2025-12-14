import * as admin from 'firebase-admin';
import { OrderByDirection, WhereFilterOp } from '@google-cloud/firestore';
import { DOC_COLLECTION, DOC_ID, DOC_PARENT, DOC_PATH, DocumentMetadata } from '../symbols';

export type FirestoreWhereConstraint = {
  fieldPath: string;
  op: WhereFilterOp;
  value: any;
};

export type FirestoreOrderBy = {
  fieldPath: string;
  direction: OrderByDirection;
};

export function buildFirestoreQuery(
  db: admin.firestore.Firestore,
  opts: {
    collectionPath: string;
    collectionGroup?: boolean;
    where?: FirestoreWhereConstraint[];
    orderBy?: FirestoreOrderBy[];
    offset?: number;
    limit?: number;
  }
): admin.firestore.Query {
  let query: admin.firestore.Query = opts.collectionGroup
    ? db.collectionGroup(opts.collectionPath)
    : db.collection(opts.collectionPath);

  for (const c of opts.where ?? []) {
    query = query.where(c.fieldPath, c.op, c.value);
  }

  for (const o of opts.orderBy ?? []) {
    query = query.orderBy(o.fieldPath, o.direction);
  }

  if (opts.offset !== undefined && opts.offset > 0) {
    query = query.offset(opts.offset);
  }

  if (opts.limit !== undefined && opts.limit !== Infinity) {
    query = query.limit(opts.limit);
  }

  return query;
}

export function docToAliasedRow(alias: string, doc: admin.firestore.QueryDocumentSnapshot): any {
  const docData = {
    ...createMetadata(doc.ref.path),
    ...doc.data(),
  };
  return { [alias]: docData };
}

/**
 * Creates document metadata from a document path.
 * Recursively creates parent metadata.
 */
export function createMetadata(path: string): DocumentMetadata {
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
