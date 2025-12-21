import * as ts from 'typescript';
import { Projection } from '../../src/api/projection';
import { runQueryAll, RunQueryOptions } from '../../src/api/run-query';
import { planToCode } from '../../src/engine/codegen/plan-to-code';
import * as runtime from '../../src/engine/codegen/runtime';
import { Planner } from '../../src/engine/planner';

export async function runQueryTest(projection: Projection, options: RunQueryOptions = {}): Promise<any[]> {
  const dynamicResults = await runQueryAll(projection, options);
  const planner = new Planner();
  const plan = planner.plan(projection);
  const code = planToCode(plan, projection.id, {
    functionName: 'runQueryTest',
    includeImports: false,
    includeSignature: false,
  });

  // Transpile TypeScript to JavaScript to remove type annotations
  // Code now contains only the body and helper definitions
  const bodyJs = ts.transpile(code, {
    target: ts.ScriptTarget.ES2020,
    module: ts.ModuleKind.CommonJS,
  });

  // Inject runtime helpers
  const helperKeys = Object.keys(runtime).filter(k => k !== '__esModule');
  const helperValues = helperKeys.map(k => (runtime as any)[k]);

  // Create AsyncFunction
  const AsyncFunction = Object.getPrototypeOf(async function () { }).constructor;
  const fn = new AsyncFunction('db', 'params', ...helperKeys, bodyJs);

  const preparedResults = await fn(options.db, options.parameters, ...helperValues);
  expect(preparedResults).toEqual(dynamicResults);
  return preparedResults;
}
