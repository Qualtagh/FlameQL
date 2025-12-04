import { Collection, Field, JoinType, Literal, LiteralType } from '../api/expression';
import { Projection } from '../api/projection';
import { Constraint, ExecutionNode, JoinNode, NodeType, Predicate, ProjectNode, ScanNode, UnionNode } from './ast';
import { Optimizer } from './optimizer';
import { isHashJoinCompatible } from './utils/operation-comparator';
import { simplifyPredicate } from './utils/predicate-utils';

import { IndexManager } from './indexes/index-manager';

export class Planner {
  private optimizer: Optimizer;

  constructor(indexManager: IndexManager = new IndexManager()) {
    this.optimizer = new Optimizer(indexManager);
  }

  plan(projection: Projection): ExecutionNode {
    // 1. Identify sources
    const sources = projection.from || {};
    const sourceKeys = Object.keys(sources);

    if (sourceKeys.length === 0) {
      throw new Error('No sources defined in projection');
    }

    // 2. Create ScanNodes for each source
    const scanNodes: Record<string, ExecutionNode> = {};
    const wherePredicate = projection.where as Predicate | undefined;

    for (const alias of sourceKeys) {
      const source = sources[alias] as Collection;

      // Extract path string from Collection object
      let path = '';
      if (source.path && source.path.length > 0 && source.path[0] instanceof Literal) {
        path = (source.path[0] as Literal).value as string;
      } else {
        path = 'unknown';
      }

      // Extract predicate for this source
      const sourcePredicate = wherePredicate ? this.extractPredicateForSource(wherePredicate, alias) : undefined;
      if (sourcePredicate) {
        // Optimize scan strategy
        const optimization = this.optimizer.optimize(sourcePredicate, path);

        if (optimization.strategy === 'UNION_SCAN') {
          const inputs: ExecutionNode[] = optimization.scans.map(scanPred =>
            this.createScanNode(alias, path, scanPred)
          );
          scanNodes[alias] = {
            type: NodeType.UNION,
            inputs,
            deduplicateByDocPath: true, // DNF optimization: safe to use DOC_PATH since all scans have same fields
          } as UnionNode;
        } else {
          // Single Scan
          scanNodes[alias] = this.createScanNode(alias, path, optimization.scans[0]);
        }
      } else {
        // No predicate, full scan
        scanNodes[alias] = this.createScanNode(alias, path, undefined);
      }
    }

    // 4. Handle Joins
    let root: ExecutionNode = scanNodes[sourceKeys[0]];

    for (let i = 1; i < sourceKeys.length; i++) {
      const alias = sourceKeys[i];
      const rightNode = scanNodes[alias];

      // TODO: Extract join conditions from projection.where correctly.
      // Currently assuming wherePredicate contains everything, but we need to separate join conditions.
      // For now, using a placeholder TRUE condition or extracting from where if possible.

      const condition: Predicate = {
        type: 'CONSTANT',
        value: true,
      };

      let joinType = projection.hints?.joinType;
      if (!joinType) {
        if (isHashJoinCompatible(condition)) {
          joinType = JoinType.Hash;
        } else {
          joinType = JoinType.NestedLoop;
        }
      }

      const joinNode: JoinNode = {
        type: NodeType.JOIN,
        left: root,
        right: rightNode,
        joinType: joinType,
        condition,
      };

      // Apply logical simplification to join conditions
      joinNode.condition = simplifyPredicate(joinNode.condition);

      root = joinNode;
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

      return {
        type: NodeType.PROJECT,
        source: root,
        fields,
      } as ProjectNode;
    }

    return root;
  }

  private createScanNode(alias: string, path: string, predicate: Predicate | undefined): ScanNode {
    const constraints: Constraint[] = [];
    if (predicate) {
      this.extractConstraints(predicate, constraints);
    }
    return {
      type: NodeType.SCAN,
      collectionPath: path,
      alias: alias,
      constraints: constraints,
    };
  }

  private extractConstraints(predicate: Predicate, constraints: Constraint[]) {
    if (predicate.type === 'AND') {
      predicate.conditions.forEach(c => this.extractConstraints(c, constraints));
    } else if (predicate.type === 'COMPARISON') {
      // Assuming left is field, right is value (simplified)
      // TODO: Handle field vs field comparisons (joins) vs field vs literal
      // For now, assuming right is a literal value string
      // We need to strip the alias from the field path
      const fieldParts = predicate.left.split('.');
      // const fieldName = fieldParts.length > 1 ? fieldParts.slice(1).join('.') : predicate.left;

      constraints.push({
        field: new Field(fieldParts[0], fieldParts.slice(1)), // This might be wrong if alias is included
        op: predicate.operation,
        value: new Literal(predicate.right, LiteralType.String), // Simplified
      });
    }
  }

  private extractPredicateForSource(predicate: Predicate, alias: string): Predicate | null {
    // Recursively find conditions that ONLY refer to this alias
    if (predicate.type === 'AND') {
      const conditions = predicate.conditions
        .map(c => this.extractPredicateForSource(c, alias))
        .filter((c): c is Predicate => c !== null);

      if (conditions.length === 0) return null;
      if (conditions.length === 1) return conditions[0];
      return { type: 'AND', conditions };
    } else if (predicate.type === 'OR') {
      const conditions = predicate.conditions
        .map(c => this.extractPredicateForSource(c, alias))
        .filter((c): c is Predicate => c !== null);

      // For OR, if any branch doesn't apply to this alias, we can't push it down easily
      // unless it's a full scan.
      // But if ALL branches apply to this alias, we can keep it.
      if (conditions.length !== predicate.conditions.length) return null;

      return { type: 'OR', conditions };
    } else if (predicate.type === 'COMPARISON') {
      if (predicate.left.startsWith(alias + '.')) {
        return predicate;
      }
    }
    return null;
  }
}
