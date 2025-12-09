import { type } from 'arktype';

export enum LiteralType {
  String = 'String',
  Number = 'Number',
  Boolean = 'Boolean',
  Null = 'Null',
}

export const { literal: literalType } = type.module({
  literal: {
    kind: "'Literal'",
    value: 'string | number | boolean | null',
    type: type.valueOf(LiteralType),
  },
});

export class Literal {
  kind: 'Literal';
  value: string | number | boolean | null;
  type: LiteralType;

  constructor(value: unknown, type: LiteralType) {
    const parsed = literalType.assert({ kind: 'Literal', value, type });
    this.kind = parsed.kind;
    this.value = parsed.value;
    this.type = parsed.type;
  }
}

export function literal(value: unknown): Literal {
  if (value === null) {
    return new Literal(value, LiteralType.Null);
  }

  if (typeof value === 'number') {
    return new Literal(value, LiteralType.Number);
  }

  if (typeof value === 'boolean') {
    return new Literal(value, LiteralType.Boolean);
  }

  return new Literal(value, LiteralType.String);
}
