import { z } from 'zod';

export const paramSchema = z.object({
  kind: z.literal('Param').default('Param'),
  name: z.string().min(1, 'Parameter name must be a non-empty string.'),
});

type ParamInput = z.infer<typeof paramSchema>;

export interface Param extends ParamInput { }

export class Param {
  constructor(name: string) {
    Object.assign(this, paramSchema.parse({ name }));
  }
}

export function param(name: string): Param {
  return new Param(name);
}
