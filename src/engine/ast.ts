import { WhereFilterOp } from '@google-cloud/firestore';
import { Field, JoinType, Literal } from '../api/expression';

export enum NodeType {
  SCAN = 'SCAN',
  FILTER = 'FILTER',
  PROJECT = 'PROJECT',
  JOIN = 'JOIN',
  AGGREGATE = 'AGGREGATE',
  UNION = 'UNION',
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
  condition: Predicate;
}

export type Predicate = ComparisonPredicate | CompositePredicate | NotPredicate | ConstantPredicate;

export interface ComparisonPredicate {
  type: 'COMPARISON';
  left: string;
  right: string;
  operation: WhereFilterOp;
}

export interface CompositePredicate {
  type: 'AND' | 'OR';
  conditions: Predicate[];
}

export interface NotPredicate {
  type: 'NOT';
  operand: Predicate;
}

export interface ConstantPredicate {
  type: 'CONSTANT';
  value: boolean;
}

export interface AggregateNode extends ExecutionNode {
  type: NodeType.AGGREGATE;
  source: ExecutionNode;
  groupBy: Field[];
  aggregates: Record<string, any>; // TODO: Define Aggregate Function
}

export interface UnionNode extends ExecutionNode {
  type: NodeType.UNION;
  inputs: ExecutionNode[];
  /** SQL-style DISTINCT: deduplicate by comparing all fields (uses hashing + equality) */
  distinct?: boolean;
  /** Optimizer-safe: deduplicate by DOC_PATH only. Only safe when all inputs scan same fields. */
  deduplicateByDocPath?: boolean;
}
