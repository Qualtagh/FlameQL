import { WhereFilterOp } from '@google-cloud/firestore';
import { Expression, Field, JoinStrategy, OrderBySpec, Predicate } from '../api/expression';

export enum NodeType {
  SCAN = 'SCAN',
  FILTER = 'FILTER',
  PROJECT = 'PROJECT',
  JOIN = 'JOIN',
  AGGREGATE = 'AGGREGATE',
  UNION = 'UNION',
  SORT = 'SORT',
  LIMIT = 'LIMIT',
}

export interface ExecutionNode {
  type: NodeType;
}

export interface Constraint {
  field: Field;
  op: WhereFilterOp;
  value: Expression | Expression[];
}

export interface ScanNode extends ExecutionNode {
  type: NodeType.SCAN;
  collectionPath: string;
  collectionGroup?: boolean;
  alias: string;
  constraints: Constraint[];
  orderBy?: OrderBySpec[];
  limit?: number;
  offset?: number;
}

export interface FilterNode extends ExecutionNode {
  type: NodeType.FILTER;
  source: ExecutionNode;
  predicate: Predicate;
}

export interface ProjectNode extends ExecutionNode {
  type: NodeType.PROJECT;
  source: ExecutionNode;
  fields: Record<string, Expression>;
}

export interface JoinNode extends ExecutionNode {
  type: NodeType.JOIN;
  left: ExecutionNode;
  right: ExecutionNode;
  joinType: JoinStrategy;
  condition: Predicate;
  crossProduct?: boolean;
}

export interface AggregateNode extends ExecutionNode {
  type: NodeType.AGGREGATE;
  source: ExecutionNode;
  groupBy: Field[];
  aggregates: Record<string, Expression>;
}

export enum UnionDistinctStrategy {
  None = 'none',
  DocPath = 'doc_path',
  HashMap = 'hash_map',
}

export interface UnionNode extends ExecutionNode {
  type: NodeType.UNION;
  inputs: ExecutionNode[];
  distinct: UnionDistinctStrategy;
}

export interface SortNode extends ExecutionNode {
  type: NodeType.SORT;
  source: ExecutionNode;
  orderBy: OrderBySpec[];
}

export interface LimitNode extends ExecutionNode {
  type: NodeType.LIMIT;
  source: ExecutionNode;
  limit: number;
  offset?: number;
}

export function getExecutionNodeChildren(node: ExecutionNode): ExecutionNode[] {
  switch (node.type) {
    case NodeType.SCAN:
      return [];
    case NodeType.FILTER:
      return [(node as FilterNode).source];
    case NodeType.PROJECT:
      return [(node as ProjectNode).source];
    case NodeType.JOIN: {
      const join = node as JoinNode;
      return [join.left, join.right];
    }
    case NodeType.AGGREGATE:
      return [(node as AggregateNode).source];
    case NodeType.UNION:
      return (node as UnionNode).inputs;
    case NodeType.SORT:
      return [(node as SortNode).source];
    case NodeType.LIMIT:
      return [(node as LimitNode).source];
    default:
      node.type satisfies never;
      return [];
  }
}

export function traverseExecutionNode(root: ExecutionNode, visit: (node: ExecutionNode) => void) {
  visit(root);
  for (const child of getExecutionNodeChildren(root)) {
    traverseExecutionNode(child, visit);
  }
}
