export interface Operator {
  next(): Promise<any | null>;
}
