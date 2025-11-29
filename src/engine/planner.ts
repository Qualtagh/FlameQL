import { Collection, Field, JoinType, Literal, LiteralType } from '../api/expression';
import { Projection } from '../api/projection';
import { ExecutionNode, JoinNode, NodeType, ScanNode } from './ast';

export interface FirestoreIndex {
  collectionGroup: string;
  queryScope: 'COLLECTION' | 'COLLECTION_GROUP';
  fields: { fieldPath: string; order: 'ASCENDING' | 'DESCENDING' }[];
}

export class Planner {

  constructor(_indexes: FirestoreIndex[] = []) {
  }

  plan(projection: Projection): ExecutionNode {
    // 1. Identify sources
    const sources = projection.from || {};
    const sourceKeys = Object.keys(sources);

    if (sourceKeys.length === 0) {
      throw new Error('No sources defined in projection');
    }

    // 2. Create ScanNodes for each source
    const scanNodes: Record<string, ScanNode> = {};
    for (const alias of sourceKeys) {
      const source = sources[alias] as Collection;

      // Extract path string from Collection object
      let path = '';
      if (source.path && source.path.length > 0 && source.path[0] instanceof Literal) {
        path = (source.path[0] as Literal).value as string;
      } else {
        path = 'unknown';
      }

      scanNodes[alias] = {
        type: NodeType.SCAN,
        collectionPath: path,
        alias: alias,
        constraints: [],
      };
    }

    // 4. Handle Joins
    let root: ExecutionNode = scanNodes[sourceKeys[0]];

    for (let i = 1; i < sourceKeys.length; i++) {
      const alias = sourceKeys[i];
      const rightNode = scanNodes[alias];

      const joinType = projection.hints?.joinType || JoinType.NestedLoop;

      root = {
        type: NodeType.JOIN,
        left: root,
        right: rightNode,
        joinType: joinType,
        on: null,
      } as JoinNode;
    }

    // 5. Add Projection (Select)
    if (projection.select) {
      const fields: Record<string, any> = {};
      for (const [key, value] of Object.entries(projection.select as Record<string, any>)) {
        if (typeof value === 'string') {
          const parts = value.split('.');
          if (parts.length > 1) {
            fields[key] = new Field(parts[0], parts.slice(1));
          } else {
            fields[key] = new Literal(value, LiteralType.String);
          }
        } else {
          fields[key] = value;
        }
      }

      root = {
        type: NodeType.PROJECT,
        source: root,
        fields: fields,
      } as any;
    }

    return root;
  }
}
