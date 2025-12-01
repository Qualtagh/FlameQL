/**
 * Special symbols used for document metadata fields.
 * These prevent naming conflicts with user-defined fields.
 *
 * In queries, these are referenced with # prefix:
 * - #id: Document ID
 * - #path: Full document path
 * - #collection: Innermost collection name
 * - #parent: Parent document metadata (recursive)
 */

export const DOC_ID = Symbol('DOC_ID');
export const DOC_PATH = Symbol('DOC_PATH');
export const DOC_COLLECTION = Symbol('DOC_COLLECTION');
export const DOC_PARENT = Symbol('DOC_PARENT');

/**
 * Document metadata structure
 */
export interface DocumentMetadata {
  [DOC_ID]: string;
  [DOC_PATH]: string;
  [DOC_COLLECTION]: string;
  [DOC_PARENT]: DocumentMetadata | null;
}
