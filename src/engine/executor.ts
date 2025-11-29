import * as admin from 'firebase-admin';
import { AggregateNode, ExecutionNode, FilterNode, JoinNode, NodeType, ProjectNode, ScanNode } from './ast';
import { Aggregate, Filter, FirestoreScan, NestedLoopJoin, Operator, Project } from './operators/operators';

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
        return new NestedLoopJoin(
          this.buildOperatorTree(joinNode.left),
          this.buildOperatorTree(joinNode.right),
          joinNode
        );
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
