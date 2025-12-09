import { OrderByDirection } from '@google-cloud/firestore';
import { type } from 'arktype';
import { orderByDirectionType } from './external-types';
import { Field } from './field';

export const { orderByEntry, orderBy } = type.module({
  orderByDirection: orderByDirectionType,
  orderByEntry: type.or(
    'string',
    {
      field: 'string',
      'direction?': orderByDirectionType,
    }
  ),
  orderBy: 'orderByEntry[]',
});

export type OrderByEntry = typeof orderByEntry.infer;
export type OrderByInput = typeof orderBy.infer;

export interface OrderBySpec {
  field: Field;
  direction: OrderByDirection;
}
