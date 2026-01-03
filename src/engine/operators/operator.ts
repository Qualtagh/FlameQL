export interface Operator extends AsyncIterable<any> {
  [Symbol.asyncIterator](): AsyncIterator<any>;
}
