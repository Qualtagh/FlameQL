declare module 'sql.js' {
  export interface SqlJsConfig {
    locateFile?: (file: string) => string;
  }
  export interface Database {
    run(sql: string): void;
    exec(sql: string): Array<{ columns: string[]; values: any[][] }>;
    close(): void;
  }
  export interface SqlJsStatic {
    Database: new () => Database;
  }
  const init: (config?: SqlJsConfig) => Promise<SqlJsStatic>;
  export default init;
}
