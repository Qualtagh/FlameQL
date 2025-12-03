import * as admin from 'firebase-admin';
import { Executor } from '../engine/executor';
import { IndexManager } from '../engine/indexes/index-manager';
import { Planner } from '../engine/planner';
import { Projection } from './projection';

export interface RunQueryOptions {
  /**
   * Firestore database instance
   */
  db: admin.firestore.Firestore;

  /**
   * Parameters to pass to the projection (for parameterized queries)
   */
  parameters?: Record<string, any>;

  /**
   * Optional transaction to run the query within
   */
  transaction?: admin.firestore.Transaction;
}

/**
 * High-level API to execute a FlameQL projection.
 * Combines planning and execution in one convenient call.
 *
 * @example
 * ```typescript
 * const results = await runQuery(myProjection, {
 *   db: firestore,
 *   parameters: { userId: '123' }
 * });
 * ```
 */
export async function runQuery(
  projection: Projection,
  options: RunQueryOptions
): Promise<any[]> {
  const planner = new Planner();
  const plan = planner.plan(projection);
  const indexManager = new IndexManager();
  const executor = new Executor(options.db, indexManager);
  return executor.execute(plan);
}
