import { OrderByDirection, WhereFilterOp } from '@google-cloud/firestore';
import { JoinStrategy } from '../../api/expression';
import { ExecutionNode, FilterNode, JoinNode, LimitNode, NodeType, PreparedScanNode, ProjectNode, ScanNode, SortNode, UnionNode } from '../ast';
import { SortOrder } from '../indexes/index-manager';
import { asField } from './predicate-utils';
import { compareValues } from './sort-utils';

export interface MergeJoinOptions {
  leftKey: (row: any) => any;
  rightKey: (row: any) => any;
  operation: WhereFilterOp; // '==', '<', '<=', '>', '>='
  sortLeft?: boolean;
  sortRight?: boolean;
}

export async function* mergeJoin(
  leftSource: AsyncIterable<any> | any[],
  rightSource: AsyncIterable<any> | any[],
  options: MergeJoinOptions
): AsyncGenerator<any, void, unknown> {
  const { leftKey, rightKey, operation, sortLeft = true, sortRight = true } = options;
  const leftBuffer: any[] = [];
  const rightBuffer: any[] = [];

  // 1. Build buffers
  if (Array.isArray(leftSource)) {
    leftBuffer.push(...leftSource);
  } else {
    for await (const row of leftSource) {
      leftBuffer.push(row);
    }
  }

  if (Array.isArray(rightSource)) {
    rightBuffer.push(...rightSource);
  } else {
    for await (const row of rightSource) {
      rightBuffer.push(row);
    }
  }

  // 2. Sort buffers if needed
  if (sortLeft) {
    leftBuffer.sort((a, b) => compareValues(leftKey(a), leftKey(b)));
  }

  if (sortRight) {
    rightBuffer.sort((a, b) => compareValues(rightKey(a), rightKey(b)));
  }

  // 3. Merge Logic
  let leftIndex = 0;
  let idxGe = 0; // index of first element where rightValue >= leftValue
  let idxGt = 0; // index of first element where rightValue > leftValue

  while (leftIndex < leftBuffer.length) {
    const leftRow = leftBuffer[leftIndex];
    const leftValue = leftKey(leftRow);

    // Collect all left rows with this same value (handle duplicates)
    const currentLeftMatches: any[] = [leftRow];
    leftIndex++;

    while (leftIndex < leftBuffer.length) {
      const nextRow = leftBuffer[leftIndex];
      const nextValue = leftKey(nextRow);
      if (compareValues(nextValue, leftValue) === 0) {
        currentLeftMatches.push(nextRow);
        leftIndex++;
      } else {
        break;
      }
    }

    // Update idxGe: find first right element >= leftValue
    while (idxGe < rightBuffer.length) {
      const rightValue = rightKey(rightBuffer[idxGe]);
      if (compareValues(rightValue, leftValue) >= 0) {
        break;
      }
      idxGe++;
    }

    // Update idxGt: find first right element > leftValue
    // Optimization: idxGt must be >= idxGe
    if (idxGt < idxGe) {
      idxGt = idxGe;
    }
    while (idxGt < rightBuffer.length) {
      const rightValue = rightKey(rightBuffer[idxGt]);
      if (compareValues(rightValue, leftValue) > 0) {
        break;
      }
      idxGt++;
    }

    // Determine matching range based on operation
    let rightMatchStart = 0;
    let rightMatchEnd = 0;

    switch (operation) {
      case '==':
        // right == left  => [idxGe, idxGt)
        rightMatchStart = idxGe;
        rightMatchEnd = idxGt;
        break;
      case '<':
        // left < right <=> right > left => [idxGt, end)
        rightMatchStart = idxGt;
        rightMatchEnd = rightBuffer.length;
        break;
      case '<=':
        // left <= right <=> right >= left => [idxGe, end)
        rightMatchStart = idxGe;
        rightMatchEnd = rightBuffer.length;
        break;
      case '>':
        // left > right <=> right < left => [0, idxGe)
        rightMatchStart = 0;
        rightMatchEnd = idxGe;
        break;
      case '>=':
        // left >= right <=> right <= left => [0, idxGt)
        rightMatchStart = 0;
        rightMatchEnd = idxGt;
        break;
      default:
        throw new Error(`Unsupported merge join operation: ${operation}`);
    }

    // Yield all combinations
    for (const lRow of currentLeftMatches) {
      for (let r = rightMatchStart; r < rightMatchEnd; r++) {
        const rightRow = rightBuffer[r];
        yield { ...lRow, ...rightRow };
      }
    }
  }
}

export function getSortOrderFromNode(node: ExecutionNode): SortOrder | undefined {
  switch (node.type) {
    case NodeType.SCAN: {
      const scan = node as ScanNode;
      const primary = scan.orderBy?.[0];
      if (!primary) return undefined;
      if (primary.field.kind !== 'Field') return undefined;
      return {
        field: `${scan.alias}.${primary.field.path.join('.')}`,
        direction: primary.direction,
      };
    }
    case NodeType.PREPARED_SCAN: {
      return getSortOrderFromNode((node as PreparedScanNode).scan);
    }
    case NodeType.FILTER:
      return getSortOrderFromNode((node as FilterNode).source);
    case NodeType.PROJECT:
      return getSortOrderFromNode((node as ProjectNode).source);
    case NodeType.LIMIT:
      return getSortOrderFromNode((node as LimitNode).source);
    case NodeType.SORT: {
      const sort = node as SortNode;
      const primary = sort.orderBy[0];
      if (!primary) return undefined;
      if (primary.field.kind !== 'Field') return undefined;
      return {
        field: `${primary.field.source}.${primary.field.path.join('.')}`,
        direction: primary.direction,
      };
    }
    case NodeType.JOIN: {
      const join = node as JoinNode;
      // Merge join outputs rows sorted by the join key (ASC), regardless of input ordering.
      if (join.joinType === JoinStrategy.Merge) {
        if (join.condition.type !== 'COMPARISON') return undefined;
        const leftField = asField(join.condition.left);
        if (!leftField) return undefined;
        return { field: `${leftField.source}.${leftField.path.join('.')}`, direction: 'asc' };
      }
      // Hash and nested-loop joins preserve the order of the LEFT input stream.
      return getSortOrderFromNode(join.left);
    }
    case NodeType.UNION: {
      const union = node as UnionNode;
      if (union.inputs.length !== 1) return undefined;
      return getSortOrderFromNode(union.inputs[0]);
    }
    case NodeType.AGGREGATE:
      return undefined;
    default:
      node.type satisfies never;
      throw new Error(`Unexpected node type: ${node.type}`);
  }
}

export function isSortedBy(
  currentSort: SortOrder | undefined,
  field: any,
  direction?: OrderByDirection
): boolean {
  if (!currentSort) return false;
  const f = asField(field);
  if (!f) return false;
  const expectedKey = `${f.source}.${f.path.join('.')}`;
  return currentSort.field === expectedKey && currentSort.direction === direction;
}
