import { WhereFilterOp } from '@google-cloud/firestore';
import { Field, JoinType, Literal } from '../api/expression';

export enum NodeType {
  SCAN = 'SCAN',
  FILTER = 'FILTER',
  PROJECT = 'PROJECT',
  JOIN = 'JOIN',
  AGGREGATE = 'AGGREGATE',
}

export interface ExecutionNode {
  type: NodeType;
}

export interface ScanNode extends ExecutionNode {
  type: NodeType.SCAN;
  collectionPath: string;
  alias: string;
  // Predicates pushed down to Firestore
  constraints: Constraint[];
}

export interface Constraint {
  field: Field;
  op: WhereFilterOp;
  value: Literal;
}

export interface FilterNode extends ExecutionNode {
  type: NodeType.FILTER;
  source: ExecutionNode;
  predicate: any; // TODO: Define Predicate AST
}

export interface ProjectNode extends ExecutionNode {
  type: NodeType.PROJECT;
  source: ExecutionNode;
  fields: Record<string, any>; // TODO: Define Expression AST
}

export interface JoinNode extends ExecutionNode {
  type: NodeType.JOIN;
  left: ExecutionNode;
  right: ExecutionNode;
  joinType: JoinType;
  on: JoinCondition;
}

export type JoinCondition =
  | ((l: any, r: any) => boolean)
  | { left: string; right: string }
  | null;

export interface AggregateNode extends ExecutionNode {
  type: NodeType.AGGREGATE;
  source: ExecutionNode;
  groupBy: Field[];
  aggregates: Record<string, any>; // TODO: Define Aggregate Function
}
