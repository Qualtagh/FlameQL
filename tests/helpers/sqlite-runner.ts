import initSqlJs, { Database, SqlJsStatic } from 'sql.js';

export interface TableData {
  name: string;
  columns: string[];
  rows: Record<string, any>[];
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, any>[];
}

export interface RawQueryResult {
  columns: string[];
  rows: any[][];
}

let sqlJsInstance: SqlJsStatic | null = null;

export async function createDatabase(): Promise<Database> {
  if (!sqlJsInstance) {
    sqlJsInstance = await initSqlJs({
      locateFile: (file: string) => require.resolve(`sql.js/dist/${file}`),
    });
  }
  return new sqlJsInstance.Database();
}

export function applyFixture(db: Database, sql: string) {
  if (sql && sql.trim()) {
    db.run(sql);
  }
}

export function query(db: Database, sql: string): Record<string, any>[] {
  return queryWithColumns(db, sql).rows;
}

export function queryWithColumns(db: Database, sql: string): QueryResult {
  const result = db.exec(sql);
  const rows = rowsFromExec(result);
  const columns = result.length ? result[0].columns : [];
  return { columns, rows };
}

export function queryRaw(db: Database, sql: string): RawQueryResult {
  const result = db.exec(sql);
  if (!result.length) return { columns: [], rows: [] };
  const { columns, values } = result[0];
  return { columns, rows: values };
}

export function listTables(db: Database): string[] {
  const result = db.exec("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
  return rowsFromExec(result).map(r => String(r.name));
}

export function readTable(db: Database, name: string): TableData {
  const columns = getTableColumns(db, name);
  const rows = query(db, `SELECT rowid as __rowid__, * FROM ${name}`);
  return { name, columns, rows };
}

export function readAllTables(db: Database): TableData[] {
  return listTables(db).map(t => readTable(db, t));
}

export function getSchemaMap(tables: TableData[]): Record<string, string[]> {
  return Object.fromEntries(tables.map(t => [t.name, t.columns]));
}

function getTableColumns(db: Database, name: string): string[] {
  const result = db.exec(`PRAGMA table_info(${name});`);
  if (!result.length) return [];
  const columns = rowsFromExec(result).map(r => String(r.name));
  return columns;
}

function rowsFromExec(result: ReturnType<Database['exec']>): Record<string, any>[] {
  if (!result.length) return [];
  const { columns, values } = result[0];
  return values.map((row: any[]) => {
    const obj: Record<string, any> = {};
    row.forEach((value: any, idx: number) => {
      obj[columns[idx]] = value;
    });
    return obj;
  });
}
