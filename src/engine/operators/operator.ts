import { OrderByDirection } from '@google-cloud/firestore';

export interface SortOrder {
  field: string;
  direction: OrderByDirection;
}

export abstract class Operator {
  private iterator: AsyncIterator<any> | null = null;

  async next(): Promise<any | null> {
    if (!this.iterator) {
      this.iterator = this[Symbol.asyncIterator]();
    }
    const result = await this.iterator.next();
    return result.done ? null : result.value;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<any> {
    let row;
    while ((row = await this.next()) !== null) {
      yield row;
    }
  }

  /**
   * Returns the current sort order of the data stream, if any.
   */
  abstract getSortOrder(): SortOrder | undefined;
}
