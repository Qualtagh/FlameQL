import { WhereFilterOp } from '@google-cloud/firestore';
import { Expression, Field, JoinStrategy, Literal, OrderBySpec, Predicate } from '../api/expression';

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
  value: Literal;
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
