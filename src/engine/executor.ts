import * as admin from 'firebase-admin';
import { JoinType } from '../api/hints';
import { AggregateNode, ExecutionNode, FilterNode, JoinNode, NodeType, ProjectNode, ScanNode } from './ast';
import { Aggregate, Filter, FirestoreScan, HashJoinOperator, MergeJoinOperator, NestedLoopJoinOperator, Operator, Project } from './operators/operators';
import { isHashJoinCompatible } from './utils/operation-comparator';

export class Executor {
  constructor(private db: admin.firestore.Firestore) { }

  async execute(plan: ExecutionNode): Promise<any[]> {
    const rootOperator = this.buildOperatorTree(plan);
    const results: any[] = [];
    let row;
    while (row = await rootOperator.next()) {
      results.push(row);
    }
    return results;
  }

  private buildOperatorTree(node: ExecutionNode): Operator {
    switch (node.type) {
      case NodeType.SCAN:
        return new FirestoreScan(this.db, node as ScanNode);
      case NodeType.JOIN:
        const joinNode = node as JoinNode;
        const left = this.buildOperatorTree(joinNode.left);
        const right = this.buildOperatorTree(joinNode.right);

        const canUseHash = isHashJoinCompatible(joinNode.condition.operation);
        const hint = joinNode.joinType;

        if (hint === JoinType.Hash) {
          return new HashJoinOperator(left, right, joinNode);
        } else if (hint === JoinType.Merge) {
          return new MergeJoinOperator(left, right, joinNode);
        } else if (hint === JoinType.NestedLoop) {
          return new NestedLoopJoinOperator(left, right, joinNode);
        } else {
          // Auto-selection
          if (canUseHash) {
            return new HashJoinOperator(left, right, joinNode);
          } else {
            return new NestedLoopJoinOperator(left, right, joinNode);
          }
        }
      case NodeType.PROJECT:
        const projectNode = node as ProjectNode;
        return new Project(
          this.buildOperatorTree(projectNode.source),
          projectNode
        );
      case NodeType.FILTER:
        const filterNode = node as FilterNode;
        return new Filter(
          this.buildOperatorTree(filterNode.source),
          filterNode
        );
      case NodeType.AGGREGATE:
        const aggregateNode = node as AggregateNode;
        return new Aggregate(
          this.buildOperatorTree(aggregateNode.source),
          aggregateNode
        );
      default:
        throw new Error(`Unsupported node type: ${node.type}`);
    }
  }

}
