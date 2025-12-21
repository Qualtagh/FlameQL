import * as ts from 'typescript';
import { Projection } from '../../src/api/projection';
import { runQueryAll, RunQueryOptions } from '../../src/api/run-query';
import { ExecutionNode } from '../../src/engine/ast';
import { planToCode } from '../../src/engine/codegen/plan-to-code';
import * as runtime from '../../src/engine/codegen/runtime';
import { Executor } from '../../src/engine/executor';
import { Planner } from '../../src/engine/planner';

// Helper function to execute a plan using the code generation pipeline
async function executeCompiled(
  plan: ExecutionNode,
  db: any,
  parameters: Record<string, any> | undefined,
  projectionId: string = 'query'
): Promise<any[]> {
  const code = planToCode(plan, projectionId, {
    functionName: 'executeCompiled',
    includeImports: false,
    includeSignature: false,
  });

  // Transpile TypeScript to JavaScript to remove type annotations
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

  return await fn(db, parameters ?? {}, ...helperValues);
}

export async function runQueryTest(projection: Projection, options: RunQueryOptions = {}): Promise<any[]> {
  const dynamicResults = await runQueryAll(projection, options);
  const planner = new Planner();
  const plan = planner.plan(projection);
  const preparedResults = await executeCompiled(plan, options.db, options.parameters, projection.id);
  expect(preparedResults).toEqual(dynamicResults);
  return preparedResults;
}

export async function executeTest(executor: Executor, plan: ExecutionNode, parameters: Record<string, any>): Promise<any[]> {
  const results = await executor.executeAll(plan, parameters);

  try {
    // Access private db using cast
    const db = (executor as any).db;
    const preparedResults = await executeCompiled(plan, db, parameters, 'execute_test');
    expect(preparedResults).toEqual(results);
  } catch (error: any) {
    if (error.message && error.message.includes('code generation not yet implemented')) {
      // Skip if codegen is not supported for this plan (e.g. Aggregate)
    } else {
      throw error;
    }
  }

  return results;
}
