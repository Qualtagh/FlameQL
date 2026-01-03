import * as admin from 'firebase-admin';
import { JoinStrategy } from '../api/hints';
import { AggregateNode, ExecutionNode, FilterNode, IndexedNestedLoopJoinNode, JoinNode, LimitNode, NodeType, PreparedScanNode, ProjectNode, ScanNode, SortNode, UnionNode } from './ast';
import { IndexManager } from './indexes/index-manager';
import { Aggregate, Filter, FirestoreScan, HashJoinOperator, IndexedNestedLoopJoinOperator, Limit, MergeJoinOperator, NestedLoopJoinOperator, Operator, PreparedFirestoreScan, Project, Sort } from './operators/operators';
import { Union } from './operators/union';

export class Executor {
  constructor(
    private db: admin.firestore.Firestore,
    private indexManager?: IndexManager
  ) { }

  async *execute(plan: ExecutionNode, parameters: Record<string, any>): AsyncGenerator<any, void, unknown> {
    const rootOperator = this.buildOperatorTree(plan, parameters);
    let row;
    while (row = await rootOperator.next()) {
      yield row;
    }
  }

  async executeAll(plan: ExecutionNode, parameters: Record<string, any>): Promise<any[]> {
    const results: any[] = [];
    for await (const row of this.execute(plan, parameters)) {
      results.push(row);
    }
    return results;
  }

  private buildOperatorTree(node: ExecutionNode, parameters: Record<string, any>): Operator {
    switch (node.type) {
      case NodeType.SCAN:
        return new FirestoreScan(this.db, node as ScanNode, parameters);
      case NodeType.PREPARED_SCAN:
        return new PreparedFirestoreScan(this.db, node as PreparedScanNode, parameters);
      case NodeType.JOIN:
        const joinNode = node as JoinNode;
        const hint = joinNode.joinType;
        const left = this.buildOperatorTree(joinNode.left, parameters);
        const right = this.buildOperatorTree(joinNode.right, parameters);
        // TODO: don't log, instead throw an error unless an explicit hint is provided
        if (joinNode.crossProduct) {
          console.log('FlameQL: executing cross-product join (no predicate provided).');
        }
        switch (hint) {
          case JoinStrategy.Hash:
            return new HashJoinOperator(left, right, joinNode);
          case JoinStrategy.Merge:
            return new MergeJoinOperator(left, right, joinNode);
          case JoinStrategy.NestedLoop:
            return new NestedLoopJoinOperator(left, right, joinNode, parameters);
          case JoinStrategy.IndexedNestedLoop:
            return new IndexedNestedLoopJoinOperator(left, right, joinNode as IndexedNestedLoopJoinNode, parameters);
          case JoinStrategy.Auto:
            throw new Error('Auto join should have been resolved by the planner');
          default:
            hint satisfies never;
        }
      case NodeType.PROJECT:
        const projectNode = node as ProjectNode;
        return new Project(
          this.buildOperatorTree(projectNode.source, parameters),
          projectNode,
          parameters
        );
      case NodeType.FILTER:
        const filterNode = node as FilterNode;
        return new Filter(
          this.buildOperatorTree(filterNode.source, parameters),
          filterNode,
          parameters
        );
      case NodeType.AGGREGATE:
        const aggregateNode = node as AggregateNode;
        return new Aggregate(
          this.buildOperatorTree(aggregateNode.source, parameters),
          aggregateNode
        );
      case NodeType.UNION:
        const unionNode = node as UnionNode;
        return new Union(
          unionNode.inputs.map(input => this.buildOperatorTree(input, parameters)),
          unionNode.distinct
        );
      case NodeType.SORT:
        const sortNode = node as SortNode;
        return new Sort(
          this.buildOperatorTree(sortNode.source, parameters),
          sortNode,
          parameters
        );
      case NodeType.LIMIT:
        const limitNode = node as LimitNode;
        return new Limit(
          this.buildOperatorTree(limitNode.source, parameters),
          limitNode
        );
      default:
        node.type satisfies never;
        throw new Error(`Unsupported node: ${node}`);
    }
  }

}
