// Test-only WASM loader. The published package uses the `--target web`
// build (loaded via fetch + async init). Tests run under Node and load
// the `--target nodejs` build directly via createRequire — this avoids
// shimming fetch/URL just to satisfy the browser entry point.

import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

interface NodeWasmModule {
  WasmDatabase: WasmDatabaseConstructor;
}

interface WasmDatabaseInstance {
  exec(sql: string): bigint;
  execParams(sql: string, params: unknown[]): bigint;
  query(sql: string): unknown[];
  queryParams(sql: string, params: unknown[]): unknown[];
  queryOne(sql: string): unknown | null;
  execMany(sql: string): void;
  toBuffer(): Uint8Array;
  flush(): void;
  free(): void;
}

interface WasmDatabaseConstructor {
  new (): WasmDatabaseInstance;
  openInMemory(): WasmDatabaseInstance;
  fromBuffer(data: Uint8Array): WasmDatabaseInstance;
}

let cached: NodeWasmModule | null = null;

export function loadWasmForTests(): NodeWasmModule {
  if (cached) return cached;
  // The build:wasm-node script writes to dist/wasm-node/. Tests assume it
  // has been run beforehand (npm test wraps it in pretest).
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  cached = require("../dist/wasm-node/rsqlite_wasm.js") as NodeWasmModule;
  return cached;
}

export type { WasmDatabaseInstance, WasmDatabaseConstructor };
