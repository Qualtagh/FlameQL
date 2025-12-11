import { runQuery } from '../../src/api/api';
import { TranslationResult, normalizeFlameRow, normalizeSqlRow, translateSqlToFlame } from './sql-to-flameql';
import { applyFixture, createDatabase, getSchemaMap, queryRaw, readAllTables } from './sqlite-runner';
import { seedFirestore } from './firestore-seed';
import { db as firestoreDb } from '../setup';

export interface SqlCase {
  name: string;
  fixture: string;
  query: string;
  expectFailure?: boolean;
}

export interface CompatResult {
  translation: TranslationResult;
  sqlRows: Record<string, any>[];
  flameRows: Record<string, any>[];
}

export async function runSqlCompatCase(testCase: SqlCase): Promise<CompatResult> {
  const sqlite = await createDatabase();
  applyFixture(sqlite, testCase.fixture);
  const tables = readAllTables(sqlite);
  const schema = getSchemaMap(tables);

  await seedFirestore(firestoreDb, tables);

  const translation = translateSqlToFlame(testCase.query, schema);

  const sqlResult = queryRaw(sqlite, testCase.query);
  const sqlRows = sqlResult.rows.map(row => normalizeSqlRow(row, translation.select));

  const flameRaw = await runQuery(translation.projection, { db: firestoreDb });
  const flameRows = flameRaw.map(row => normalizeFlameRow(row, translation.select));

  const normalizeList = (rows: Record<string, any>[]) =>
    translation.ordered ? rows : [...rows].sort(stableStringifyCompare);

  return {
    translation,
    sqlRows: normalizeList(sqlRows),
    flameRows: normalizeList(flameRows),
  };
}

function stableStringifyCompare(a: Record<string, any>, b: Record<string, any>): number {
  return JSON.stringify(a).localeCompare(JSON.stringify(b));
}
