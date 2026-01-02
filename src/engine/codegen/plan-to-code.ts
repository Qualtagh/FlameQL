import { ComparisonPredicate, JoinStrategy } from '../../api/expression';
import { AggregateNode, ExecutionNode, FilterNode, IndexedNestedLoopJoinNode, JoinNode, LimitNode, NodeType, PreparedScanNode, ProjectNode, ScanNode, SortNode, UnionNode } from '../ast';
import { maxPerOperation } from '../utils/predicate-utils';
import { align, indent, toCamelCase } from '../utils/string-utils';
import { generateExpressionCode, generateExpressionSelector, generatePredicateCode } from './expression-codegen';

export interface CodeGenOptions {
  functionName?: string;
  includeImports?: boolean;
  includeSignature?: boolean;
}

export function planToCode(
  plan: ExecutionNode,
  projectionId: string = 'execute_query',
  options: CodeGenOptions = {}
): string {
  const {
    functionName = toCamelCase(projectionId),
    includeImports = true,
    includeSignature = true,
  } = options;
  const ctx = new CodeGenContext();
  const rootGenerator = ctx.generateNode(plan);
  let code = '';
  if (includeImports) {
    code += align`
      import { Firestore } from '@google-cloud/firestore';
      import { getDocData, evaluatePredicate, evaluate, getValue, unionRows, sortRows, batchRows, JoinHashTable } from 'flameql/codegen/runtime';

    `;
  }

  if (includeSignature) {
    code += align`
    export async function ${functionName}(
      db: Firestore,
      params: Record<string, any>
    ): Promise<any[]> {
  `;
  }

  const definitions = ctx.getDefinitions();
  if (definitions) code += `${definitions}\n`;

  code += indent(align`
    // Collect results
    const results: any[] = [];
    for await (const row of ${rootGenerator}()) {
      results.push(row);
    }
    return results;
  `, 2);

  if (includeSignature) {
    code += '}\n';
  }

  return code;
}

class CodeGenContext {
  private definitions: string[] = [];
  private nodeCounters: Record<string, number> = {};

  getDefinitions(): string {
    return this.definitions.join('\n');
  }

  generateNode(node: ExecutionNode): string {
    switch (node.type) {
      case NodeType.SCAN:
        return this.generateScan(node as ScanNode);
      case NodeType.FILTER:
        return this.generateFilter(node as FilterNode);
      case NodeType.PROJECT:
        return this.generateProject(node as ProjectNode);
      case NodeType.JOIN:
        return this.generateJoin(node as JoinNode);
      case NodeType.UNION:
        return this.generateUnion(node as UnionNode);
      case NodeType.SORT:
        return this.generateSort(node as SortNode);
      case NodeType.LIMIT:
        return this.generateLimit(node as LimitNode);
      case NodeType.AGGREGATE:
        return this.generateAggregate(node as AggregateNode);
      case NodeType.PREPARED_SCAN:
        return this.generatePreparedScan(node as PreparedScanNode);
      default:
        throw new Error(`Unsupported node type: ${node.type}`);
    }
  }

  private nextName(prefix: string): string {
    const count = this.nodeCounters[prefix] || 0;
    this.nodeCounters[prefix] = count + 1;
    return `${prefix}_${count}`;
  }

  private addDefinition(code: string) {
    this.definitions.push(code);
  }

  private generateScan(node: ScanNode): string {
    return this.generatePreparedScanImplementation(node, undefined, undefined);
  }

  private generatePreparedScan(node: PreparedScanNode): string {
    return this.generatePreparedScanImplementation(node.scan, node.driver, node.postFilter);
  }

  /**
   * Generates a reusable Firestore scan function that supports parameterized execution (INLJ).
   */
  private generatePreparedScanImplementation(
    scan: ScanNode,
    driver?: { fieldPath: string; op: string },
    postFilter?: PreparedScanNode['postFilter']
  ): string {
    const name = this.nextName(`scan_${scan.alias.replace(/[^a-zA-Z0-9]/g, '_')}`);
    const method = scan.collectionGroup ? 'collectionGroup' : 'collection';

    const constraints = (scan.constraints ?? [])
      .map(c => {
        const fieldPath = c.field.path.join('.');
        const valueCode = Array.isArray(c.value)
          ? `[${c.value.map(v => generateExpressionCode(v)).join(', ')}]`
          : generateExpressionCode(c.value as any);
        return `query = query.where('${fieldPath}', '${c.op}', ${valueCode});`;
      })
      .join('\n');

    const orderBy = (scan.orderBy ?? [])
      .map(order => {
        if (order.field.kind !== 'Field') return '';
        const fieldPath = order.field.path.join('.');
        return `query = query.orderBy('${fieldPath}', '${order.direction}');`;
      })
      .filter(Boolean)
      .join('\n');

    const limit = scan.limit !== undefined ? `query = query.limit(${scan.limit});` : '';
    const offset = scan.offset !== undefined ? `query = query.offset(${scan.offset});` : '';

    let middle = [constraints, orderBy, limit, offset].filter(Boolean).join('\n');

    // Inject driver logic
    if (driver) {
      const driverLogic = align`
        if (drivingValue !== undefined) {
          query = query.where('${driver.fieldPath}', '${driver.op}', drivingValue);
        }
      `;
      middle = `${middle}\n${driverLogic}`;
    }

    const alias = /[^a-zA-Z0-9]/.test(scan.alias) ? `'${scan.alias.replace(/'/g, '\\\'')}'` : scan.alias;
    const postFilterCode = postFilter && (postFilter.type !== 'CONSTANT' || !postFilter.value)
      ? `if (!(${generatePredicateCode(postFilter)})) continue;`
      : '';
    const row = `{ ${alias}: getDocData(doc) }`;
    const yieldRow = postFilterCode ? align`
      const row = ${row};
      ${postFilterCode}
      yield row;
    ` : `yield ${row};`;

    const args = driver ? 'drivingValue?: any' : '';
    const body = align`
      async function* ${name}(${args}) {
        let query: FirebaseFirestore.Query = db.${method}('${scan.collectionPath}');
        ${middle}
        for await (const doc of query.stream()) {
          ${yieldRow}
        }
      }
    `;

    this.addDefinition(indent(body, 2));
    return name;
  }

  private generateFilter(node: FilterNode): string {
    const sourceName = this.generateNode(node.source);
    const name = this.nextName('filter');
    const predicateCode = generatePredicateCode(node.predicate);

    const body = align`
      async function* ${name}() {
        for await (const row of ${sourceName}()) {
          if (${predicateCode}) {
            yield row;
          }
        }
      }
    `;

    this.addDefinition(indent(body, 2));
    return name;
  }

  private generateProject(node: ProjectNode): string {
    const sourceName = this.generateNode(node.source);
    const name = this.nextName('project');

    const fields = Object.entries(node.fields)
      .map(([key, expr]) => `${key}: ${generateExpressionCode(expr)},`)
      .join('\n');

    const body = align`
      async function* ${name}() {
        for await (const row of ${sourceName}()) {
          yield {
            ${fields}
          };
        }
      }
    `;

    this.addDefinition(indent(body, 2));
    return name;
  }

  private generateJoin(node: JoinNode): string {
    const leftName = this.generateNode(node.left);
    const rightName = this.generateNode(node.right);
    const name = this.nextName('join');
    const conditionCode = generatePredicateCode(node.condition);

    let body: string;

    if (node.joinType === JoinStrategy.NestedLoop) {
      body = align`
        async function* ${name}() {
          const rightBuffer: any[] = [];
          for await (const row of ${rightName}()) {
            rightBuffer.push(row);
          }

          for await (const leftRow of ${leftName}()) {
            for (const rightRow of rightBuffer) {
              const row = { ...leftRow, ...rightRow };
              if (!(${conditionCode})) continue;
              yield row;
            }
          }
        }
      `;
    } else if (node.joinType === JoinStrategy.IndexedNestedLoop) {
      const indexJoin = (node as IndexedNestedLoopJoinNode).indexJoin;
      const leftSelectorCode = generateExpressionSelector(indexJoin.leftExpr);
      const isBatch = indexJoin.mode === 'batch';
      const batchSize = maxPerOperation((node.right as PreparedScanNode).driver?.op);

      if (isBatch && batchSize > 1) {
        body = align`
          async function* ${name}() {
            const leftSelector = ${leftSelectorCode};
            for await (const leftRows of batchRows(${leftName}(), ${batchSize})) {
              const lookupValues = leftRows
                .map(row => leftSelector(row, params))
                .filter(v => v !== undefined && v !== null);

              if (lookupValues.length === 0) continue;

              const rightBuffer: any[] = [];
              for await (const rightRow of ${rightName}(lookupValues)) {
                rightBuffer.push(rightRow);
              }

              for (const leftRow of leftRows) {
                for (const rightRow of rightBuffer) {
                  const row = { ...leftRow, ...rightRow };
                  if (!(${conditionCode})) continue;
                  yield row;
                }
              }
            }
          }
        `;
      } else {
        body = align`
          async function* ${name}() {
            const leftSelector = ${leftSelectorCode};
            for await (const leftRow of ${leftName}()) {
              const lookupValue = leftSelector(leftRow, params);

              if (lookupValue === undefined || lookupValue === null) continue;
              for await (const rightRow of ${rightName}(lookupValue)) {
                const row = { ...leftRow, ...rightRow };
                if (!(${conditionCode})) continue;
                yield row;
              }
            }
          }
        `;
      }
    } else if (node.joinType === JoinStrategy.Hash) {
      const condition = node.condition as ComparisonPredicate;
      const leftExpr = condition.left;
      const rightExpr = condition.right;

      if (Array.isArray(rightExpr)) {
        throw new Error('Hash Join right operand cannot be an array');
      }

      const leftCode = generateExpressionCode(leftExpr);
      const rightCode = generateExpressionCode(rightExpr);

      body = align`
        async function* ${name}() {
          const hashTable = new JoinHashTable('${condition.operation}');

          for await (const row of ${rightName}()) {
            const key = ${rightCode};
            hashTable.add(key, row);
          }

          for await (const row of ${leftName}()) {
            const probeValue = ${leftCode};
            const matches = hashTable.get(probeValue);
            if (!matches) continue;
            for (const match of matches) {
              yield { ...row, ...match };
            }
          }
        }
      `;
    } else if (node.joinType === JoinStrategy.Merge) {
      body = align`
        async function* ${name}() {
          // WARNING: Join strategy '${node.joinType}' handled as Nested Loop.
          const rightBuffer: any[] = [];
          for await (const row of ${rightName}()) {
            rightBuffer.push(row);
          }

          for await (const leftRow of ${leftName}()) {
            for (const rightRow of rightBuffer) {
              const row = { ...leftRow, ...rightRow };
              if (!(${conditionCode})) continue;
              yield row;
            }
          }
        }
      `;
    } else if (node.joinType === JoinStrategy.Auto) {
      throw new Error('Automatic selection of join strategy should have been handled by the planner');
    } else {
      node.joinType satisfies never;
      throw new Error(`Unsupported join strategy: ${node.joinType}`);
    }

    this.addDefinition(indent(body, 2));
    return name;
  }

  private generateUnion(node: UnionNode): string {
    const inputNames = node.inputs.map(i => this.generateNode(i));
    const name = this.nextName('union');
    const strategy = node.distinct; // 'none' | 'doc_path' | 'hash_map'

    const generatorLines = inputNames.map(input => `${input},`).join('\n');

    const body = align`
      async function* ${name}() {
        const generators = [
          ${generatorLines}
        ];
        for await (const row of unionRows(generators, '${strategy}')) {
          yield row;
        }
      }
    `;

    this.addDefinition(indent(body, 2));
    return name;
  }

  private generateSort(node: SortNode): string {
    const sourceName = this.generateNode(node.source);
    const name = this.nextName('sort');

    // Sort requires buffering. Use runtime helper and pass expressions for late evaluation.
    const orderByParams = node.orderBy
      .map(o => {
        const selector = generateExpressionSelector(o.field);
        return `{ selector: ${selector}, direction: '${o.direction}' }`;
      })
      .join(', ');

    const body = align`
      async function* ${name}() {
        for await (const row of sortRows(${sourceName}(), [${orderByParams}], params)) {
          yield row;
        }
      }
    `;

    this.addDefinition(indent(body, 2));
    return name;
  }

  private generateLimit(node: LimitNode): string {
    const sourceName = this.generateNode(node.source);
    const name = this.nextName('limit');

    const offsetInit = node.offset ? 'let skipped = 0;\n' : '';
    const offsetBlock = node.offset ? align`
      if (skipped < ${node.offset}) {
        skipped++;
        continue;
      }
    ` : '';

    const body = align`
      async function* ${name}() {
        let count = 0;
        ${offsetInit}
        for await (const row of ${sourceName}()) {
          ${offsetBlock}
          if (count >= ${node.limit}) return;
          yield row;
          count++;
        }
      }
    `;

    this.addDefinition(indent(body, 2));
    return name;
  }

  private generateAggregate(_node: AggregateNode): string {
    // Not fully specified in plan, but required for completeness.
    // Aggregation usually consumes all rows and yields one or more result rows.
    throw new Error('Aggregate code generation not yet implemented');
  }
}
