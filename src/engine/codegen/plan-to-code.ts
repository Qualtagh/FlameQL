import { ComparisonPredicate, JoinStrategy } from '../../api/expression';
import { AggregateNode, ExecutionNode, FilterNode, JoinNode, LimitNode, NodeType, ProjectNode, ScanNode, SortNode, UnionNode } from '../ast';
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
      import { getData, evaluatePredicate, evaluate, getValue, unionRows, sortRows, JoinHashTable } from 'flameql/codegen/runtime';

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
    const name = `scan_${node.alias}`;
    const method = node.collectionGroup ? 'collectionGroup' : 'collection';

    const constraints = (node.constraints ?? [])
      .map(c => {
        const fieldPath = c.field.path.join('.');
        const valueCode = Array.isArray(c.value)
          ? `[${c.value.map(v => generateExpressionCode(v)).join(', ')}]`
          : generateExpressionCode(c.value as any);
        return `query = query.where('${fieldPath}', '${c.op}', ${valueCode});`;
      })
      .join('\n');

    const orderBy = (node.orderBy ?? [])
      .map(order => {
        if (order.field.kind !== 'Field') return '';
        const fieldPath = order.field.path.join('.');
        return `query = query.orderBy('${fieldPath}', '${order.direction}');`;
      })
      .filter(Boolean)
      .join('\n');

    const limit = node.limit !== undefined ? `query = query.limit(${node.limit});` : '';
    const offset = node.offset !== undefined ? `query = query.offset(${node.offset});` : '';

    const middle = [constraints, orderBy, limit, offset].filter(Boolean).join('\n');

    const body = align`
      async function* ${name}() {
        let query: FirebaseFirestore.Query = db.${method}('${node.collectionPath}');
        ${middle}
        for await (const doc of query.stream()) {
          yield { ${node.alias}: getData(doc as any) };
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
              if (${conditionCode}) {
                yield row;
              }
            }
          }
        }
      `;
    } else if (node.joinType === JoinStrategy.IndexedNestedLoop) {
      body = align`
        async function* ${name}() {
          // WARNING: Join strategy '${node.joinType}' handled as Nested Loop for code generation simplicity.
          const rightBuffer: any[] = [];
          for await (const row of ${rightName}()) {
            rightBuffer.push(row);
          }

          for await (const leftRow of ${leftName}()) {
            for (const rightRow of rightBuffer) {
              const row = { ...leftRow, ...rightRow };
              if (${conditionCode}) {
                yield row;
              }
            }
          }
        }
      `;
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
            if (matches) {
              for (const match of matches) {
                yield { ...row, ...match };
              }
            }
          }
        }
      `;
    } else {
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
              if (${conditionCode}) {
                yield row;
              }
            }
          }
        }
      `;
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
