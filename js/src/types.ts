export type SqlValue = null | number | string | Uint8Array;

export type BindParams = SqlValue[] | Record<string, SqlValue>;

export interface Row {
  [column: string]: SqlValue;
}

export interface DatabaseOptions {
  backend?: "memory" | "opfs" | "indexeddb";
}
