import { type } from 'arktype';

export const { param: paramType } = type.module({
  param: {
    kind: "'Param'",
    name: 'string > 0',
  },
});

type ParamInput = typeof paramType.infer;

export interface Param extends ParamInput { }

export class Param {
  constructor(name: string) {
    const parsed = paramType.assert({ kind: 'Param', name });
    this.kind = parsed.kind;
    this.name = parsed.name;
  }
}

export function param(name: string): Param {
  return new Param(name);
}
