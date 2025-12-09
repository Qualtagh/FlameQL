import { OrderByDirection, WhereFilterOp } from '@google-cloud/firestore';
import { type } from 'arktype';

const ORDER_BY_DIRECTION = {
  asc: 'asc',
  desc: 'desc',
} as const satisfies Record<OrderByDirection, OrderByDirection>;

export const orderByDirectionType = type.valueOf(ORDER_BY_DIRECTION);

const WHERE_FILTER_OP = {
  '<': '<',
  '<=': '<=',
  '==': '==',
  '!=': '!=',
  '>=': '>=',
  '>': '>',
  'array-contains': 'array-contains',
  in: 'in',
  'not-in': 'not-in',
  'array-contains-any': 'array-contains-any',
} as const satisfies Record<WhereFilterOp, WhereFilterOp>;

export const whereFilterOpType = type.valueOf(WHERE_FILTER_OP);
