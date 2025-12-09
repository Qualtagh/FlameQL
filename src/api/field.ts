import { type } from 'arktype';

export const { field: fieldType } = type.module({
  field: {
    kind: "'Field'",
    source: 'string',
    path: 'string[] > 0',
  },
});

type FieldInput = typeof fieldType.infer;

export interface Field extends FieldInput { }

export class Field {
  constructor(source: string, path: string[]) {
    const parsed = fieldType.assert({ kind: 'Field', source, path });
    this.kind = parsed.kind;
    this.source = parsed.source;
    this.path = parsed.path;
  }
}

export function field(ref: string): Field {
  if (typeof ref !== 'string') {
    throw new Error('field() expects a string in the form "alias.path".');
  }

  const segments = ref.split('.');
  if (segments.length < 2) {
    throw new Error(`Field reference "${ref}" must include alias and path.`);
  }

  const [source, ...path] = segments;
  return new Field(source, path);
}
