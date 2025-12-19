import { OrderByDirection } from '@google-cloud/firestore';
import { type } from 'arktype';
import { orderByDirectionType } from './external-types';
import { Expression, expressionType } from './predicate';

export const { orderByEntry, orderBy } = type.module({
  orderByDirection: orderByDirectionType,
  orderByEntry: type.or(
    expressionType,
    {
      field: expressionType,
      direction: orderByDirectionType,
    }
  ),
  orderBy: 'orderByEntry[]',
});

export type OrderByEntry = typeof orderByEntry.infer;
export type OrderByInput = typeof orderBy.infer;

export interface OrderBySpec {
  field: Expression;
  direction: OrderByDirection;
}
