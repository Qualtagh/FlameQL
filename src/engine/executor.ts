import * as admin from 'firebase-admin';
import { JoinStrategy } from '../api/hints';
import { AggregateNode, ExecutionNode, FilterNode, JoinNode, LimitNode, NodeType, ProjectNode, ScanNode, SortNode, UnionNode } from './ast';
import { IndexManager } from './indexes/index-manager';
import { Aggregate, Filter, FirestoreScan, HashJoinOperator, Limit, MergeJoinOperator, NestedLoopJoinOperator, Operator, Project, Sort } from './operators/operators';
import { Union } from './operators/union';

export class Executor {
  constructor(
    private db: admin.firestore.Firestore,
    private indexManager?: IndexManager
  ) { }

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
        return new FirestoreScan(this.db, node as ScanNode, this.indexManager);
      case NodeType.JOIN:
        const joinNode = node as JoinNode;
        const left = this.buildOperatorTree(joinNode.left);
        const right = this.buildOperatorTree(joinNode.right);
        const hint = joinNode.joinType;
        if (joinNode.crossProduct) {
          console.warn('FlameQL: executing cross-product join (no predicate provided).');
        }
        switch (hint) {
          case JoinStrategy.Hash:
            return new HashJoinOperator(left, right, joinNode);
          case JoinStrategy.Merge:
            return new MergeJoinOperator(left, right, joinNode);
          case JoinStrategy.NestedLoop:
            return new NestedLoopJoinOperator(left, right, joinNode);
          case JoinStrategy.IndexedNestedLoop:
            throw new Error('IndexNestedLoop join type has not been implemented yet');
          case JoinStrategy.Auto:
            throw new Error('Auto join should have been resolved by the planner');
          default:
            hint satisfies never;
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
      case NodeType.UNION:
        const unionNode = node as UnionNode;
        return new Union(
          unionNode.inputs.map(input => this.buildOperatorTree(input)),
          unionNode.distinct
        );
      case NodeType.SORT:
        const sortNode = node as SortNode;
        return new Sort(
          this.buildOperatorTree(sortNode.source),
          sortNode
        );
      case NodeType.LIMIT:
        const limitNode = node as LimitNode;
        return new Limit(
          this.buildOperatorTree(limitNode.source),
          limitNode
        );
      default:
        node.type satisfies never;
    }
    throw new Error(`Unsupported node: ${node}`);
  }

}
