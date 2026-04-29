export type { SqlValue, BindParams, Row, DatabaseOptions } from "./types.js";
export { WorkerDatabase } from "./worker-proxy.js";

interface WasmModule {
  default: (input?: RequestInfo | URL) => Promise<unknown>;
  WasmDatabase: WasmDatabaseConstructor;
}

interface WasmDatabaseInstance {
  exec(sql: string): bigint;
  execParams(sql: string, params: SqlValue[]): bigint;
  query(sql: string): unknown[];
  queryParams(sql: string, params: SqlValue[]): unknown[];
  queryOne(sql: string): unknown | null;
  execMany(sql: string): void;
  toBuffer(): Uint8Array;
  flush(): void;
  close(): void;
  free(): void;
}

interface WasmDatabaseConstructor {
  new (): WasmDatabaseInstance;
  openInMemory(): WasmDatabaseInstance;
  openWithOpfs(name: string): Promise<WasmDatabaseInstance>;
  openWithIdb(name: string): Promise<WasmDatabaseInstance>;
  openPersisted(name: string): Promise<WasmDatabaseInstance>;
  fromBuffer(data: Uint8Array): WasmDatabaseInstance;
}

import type { SqlValue, Row, DatabaseOptions } from "./types.js";

let wasmModule: WasmModule | null = null;
let wasmInitPromise: Promise<WasmModule> | null = null;

async function loadWasm(wasmUrl?: string | URL): Promise<WasmModule> {
  if (wasmModule) return wasmModule;
  if (wasmInitPromise) return wasmInitPromise;

  wasmInitPromise = (async () => {
    const mod: WasmModule = await import(
      /* webpackIgnore: true */
      wasmUrl?.toString() ?? new URL("../pkg/rsqlite_wasm.js", import.meta.url).href
    );
    await mod.default();
    wasmModule = mod;
    return mod;
  })();

  return wasmInitPromise;
}

export class Database {
  private inner: WasmDatabaseInstance;
  private closed = false;

  private constructor(inner: WasmDatabaseInstance) {
    this.inner = inner;
  }

  static async open(
    name?: string,
    options?: DatabaseOptions
  ): Promise<Database> {
    const mod = await loadWasm();
    const backend = options?.backend ?? "memory";

    if (backend === "opfs") {
      const inner = await mod.WasmDatabase.openWithOpfs(name ?? "rsqlite");
      return new Database(inner);
    }

    if (backend === "indexeddb") {
      const inner = await mod.WasmDatabase.openWithIdb(name ?? "rsqlite");
      return new Database(inner);
    }

    if (backend === "memory") {
      const inner = new mod.WasmDatabase();
      return new Database(inner);
    }

    // Default: auto-detect best persistent backend
    const inner = await mod.WasmDatabase.openPersisted(name ?? "rsqlite");
    return new Database(inner);
  }

  static async openInMemory(): Promise<Database> {
    const mod = await loadWasm();
    const inner = mod.WasmDatabase.openInMemory();
    return new Database(inner);
  }

  static async fromBuffer(buffer: Uint8Array | ArrayBuffer): Promise<Database> {
    const mod = await loadWasm();
    const data =
      buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
    const inner = mod.WasmDatabase.fromBuffer(data);
    return new Database(inner);
  }

  exec(sql: string, params?: SqlValue[]): number {
    this.ensureOpen();
    if (params && params.length > 0) {
      return Number(this.inner.execParams(sql, params));
    }
    return Number(this.inner.exec(sql));
  }

  query<T extends Row = Row>(sql: string, params?: SqlValue[]): T[] {
    this.ensureOpen();
    if (params && params.length > 0) {
      return this.inner.queryParams(sql, params) as T[];
    }
    return this.inner.query(sql) as T[];
  }

  queryOne<T extends Row = Row>(sql: string): T | null {
    this.ensureOpen();
    return this.inner.queryOne(sql) as T | null;
  }

  execMany(sql: string): void {
    this.ensureOpen();
    this.inner.execMany(sql);
  }

  toBuffer(): Uint8Array {
    this.ensureOpen();
    return this.inner.toBuffer();
  }

  flush(): void {
    this.ensureOpen();
    this.inner.flush();
  }

  transaction<T>(fn: () => T): T {
    this.ensureOpen();
    this.inner.exec("BEGIN");
    try {
      const result = fn();
      this.inner.exec("COMMIT");
      return result;
    } catch (e) {
      this.inner.exec("ROLLBACK");
      throw e;
    }
  }

  close(): void {
    if (!this.closed) {
      this.inner.free();
      this.closed = true;
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new Error("Database is closed");
    }
  }
}

export async function initWasm(wasmUrl?: string | URL): Promise<void> {
  await loadWasm(wasmUrl);
}
