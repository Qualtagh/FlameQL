import { OrderByDirection } from '@google-cloud/firestore';

export interface SortOrder {
  field: string;
  direction: OrderByDirection;
}

export interface Operator extends AsyncIterable<any> {
  [Symbol.asyncIterator](): AsyncIterator<any>;

  /**
   * Returns the current sort order of the data stream, if any.
   */
  getSortOrder(): SortOrder | undefined;
}
