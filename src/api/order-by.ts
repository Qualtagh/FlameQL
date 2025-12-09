import { OrderByDirection } from '@google-cloud/firestore';
import { z } from 'zod';
import { Field } from './field';

const orderByEntrySchema = z.union([
  z.string(),
  z.object({
    field: z.string(),
    direction: z.custom<OrderByDirection>(dir => typeof dir === 'string').optional(),
  }),
]);

export const orderBySchema = z.array(orderByEntrySchema);

export interface OrderBySpec {
  field: Field;
  direction: OrderByDirection;
}
