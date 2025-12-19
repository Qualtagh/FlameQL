import * as admin from 'firebase-admin';
import { Executor } from '../engine/executor';
import { IndexManager } from '../engine/indexes/index-manager';
import { Planner } from '../engine/planner';
import { FlameConfig } from './config';
import { Projection } from './projection';

export interface RunQueryOptions {
  /**
   * Firestore database instance.
   * If omitted, uses FlameConfig.db.
   */
  db?: admin.firestore.Firestore;

  /**
   * Parameters to pass to the projection (for parameterized queries)
   */
  parameters?: Record<string, any>;

  /**
   * Optional transaction to run the query within
   * TODO: Implement transaction support
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
  options: RunQueryOptions = {}
): Promise<any[]> {
  const db = options.db ?? FlameConfig.db;
  const indexManager = FlameConfig.indexManager ?? new IndexManager();
  const planner = new Planner(indexManager);
  const plan = planner.plan(projection);
  const executor = new Executor(db, indexManager);
  return executor.execute(plan, options.parameters ?? {});
}
