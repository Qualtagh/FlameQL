declare module 'sqlite-parser' {
  function parse(sql: string): any;
  export = parse;
}
