export type { SqlValue, BindParams, Row, DatabaseOptions } from "./types.js";
export { WorkerDatabase } from "./worker-proxy.js";
export { exposeForDevtools, type ExposeForDevtoolsOptions } from "./devtools.js";

interface WasmModule {
  // The `--target web` build exports an async init that fetches the `.wasm`.
  // The `--target nodejs` build auto-instantiates at import time and has no
  // `default`, so this is optional and guarded at the call site.
  default?: (input?: RequestInfo | URL) => Promise<unknown>;
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
  createFunction(name: string, nArgs: number, fn: (...args: unknown[]) => unknown): void;
  deleteFunction(name: string): boolean;
}

/** Options for [`Database.createFunction`]. */
export interface UdfOptions {
  /** Number of arguments the function accepts. Omit or pass `-1` for
   *  variadic. Calls with the wrong arity error at query time. */
  nArgs?: number;
}

interface WasmDatabaseConstructor {
  new (): WasmDatabaseInstance;
  openInMemory(): WasmDatabaseInstance;
  openWithOpfs(
    name: string,
    chunkSize?: bigint,
    maxShards?: number
  ): Promise<WasmDatabaseInstance>;
  openWithIdb(
    name: string,
    chunkSize?: bigint
  ): Promise<WasmDatabaseInstance>;
  openPersisted(
    name: string,
    chunkSize?: bigint,
    maxShards?: number
  ): Promise<WasmDatabaseInstance>;
  fromBuffer(data: Uint8Array): WasmDatabaseInstance;
  /** Present only in the Node/Deno build (the `nodefs` feature). */
  openWithFile?(path: string): WasmDatabaseInstance;
}

import type { SqlValue, Row, DatabaseOptions } from "./types.js";

/** True under Deno (which needs createRequire to load the CJS nodejs build). */
function isDeno(): boolean {
  return typeof (globalThis as { Deno?: unknown }).Deno !== "undefined";
}

/** True under a server runtime (Node.js or Deno) where the `node:fs`-backed
 *  build applies. The browser build is used everywhere else. */
function isServerRuntime(): boolean {
  const g = globalThis as { process?: { versions?: { node?: string } } };
  return isDeno() || !!g.process?.versions?.node;
}

let wasmModule: WasmModule | null = null;
let wasmInitPromise: Promise<WasmModule> | null = null;

async function loadWasm(wasmUrl?: string | URL): Promise<WasmModule> {
  if (wasmModule) return wasmModule;
  if (wasmInitPromise) return wasmInitPromise;

  wasmInitPromise = (async () => {
    // Deno cannot `import()` the CommonJS `--target nodejs` build from a file
    // URL (it sees `require` as undefined); load it through createRequire,
    // which resolves CJS under Deno for both local and `npm:` consumption.
    // This build synchronously self-instantiates the wasm (no fetch, no init)
    // and carries the `node:fs`-backed file VFS.
    if (!wasmUrl && isDeno()) {
      // Variable specifier so tsc (browser lib, no @types/node) doesn't try to
      // resolve node:module; it exists at runtime under Deno and Node.
      const nodeModule = "node:module";
      const { createRequire } = (await import(nodeModule)) as {
        createRequire: (url: string) => (id: string) => WasmModule;
      };
      const require = createRequire(import.meta.url);
      const mod = require("./wasm-node/rsqlite_wasm.js");
      wasmModule = mod;
      return mod;
    }

    // Node uses dynamic import of the same nodejs build (also mockable under
    // vitest). The browser uses the `--target web` build, whose `default()`
    // fetches the `.wasm`. An explicit `wasmUrl` always wins (web target).
    const url =
      wasmUrl?.toString() ??
      new URL(
        isServerRuntime()
          ? "./wasm-node/rsqlite_wasm.js"
          : "./wasm/rsqlite_wasm.js",
        import.meta.url
      ).href;
    const mod: WasmModule = await import(/* webpackIgnore: true */ url);
    // Web target only: initialize by fetching the wasm. The nodejs target has
    // no `default` and is already live.
    if (typeof mod.default === "function") {
      await mod.default();
    }
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
    const chunkSize =
      options?.chunkSize !== undefined
        ? BigInt(options.chunkSize)
        : undefined;
    const maxShards = options?.maxShards;

    if (backend === "opfs") {
      const inner = await mod.WasmDatabase.openWithOpfs(
        name ?? "rsqlite",
        chunkSize,
        maxShards
      );
      return new Database(inner);
    }

    if (backend === "indexeddb") {
      const inner = await mod.WasmDatabase.openWithIdb(
        name ?? "rsqlite",
        chunkSize
      );
      return new Database(inner);
    }

    if (backend === "file") {
      if (typeof mod.WasmDatabase.openWithFile !== "function") {
        throw new Error(
          "rsqlite-wasm: the 'file' backend is only available in the Node.js/Deno build"
        );
      }
      const inner = mod.WasmDatabase.openWithFile(name ?? "rsqlite.db");
      return new Database(inner);
    }

    if (backend === "memory") {
      const inner = new mod.WasmDatabase();
      return new Database(inner);
    }

    // Default: auto-detect best persistent backend
    const inner = await mod.WasmDatabase.openPersisted(
      name ?? "rsqlite",
      chunkSize,
      maxShards
    );
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

  queryOne<T extends Row = Row>(sql: string, params?: SqlValue[]): T | null {
    this.ensureOpen();
    if (params && params.length > 0) {
      const rows = this.inner.queryParams(sql, params) as T[];
      return rows[0] ?? null;
    }
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

  /** Register a JavaScript callback as a SQL scalar function.
   *
   * The callback runs synchronously inside the engine's evaluation loop —
   * async functions and Promises are not awaited. Throwing inside the
   * callback surfaces as a query error. UDFs cannot shadow built-ins.
   */
  createFunction(
    name: string,
    fn: (...args: SqlValue[]) => SqlValue,
    options?: UdfOptions
  ): void {
    this.ensureOpen();
    const nArgs = options?.nArgs ?? -1;
    this.inner.createFunction(name, nArgs, fn as (...args: unknown[]) => unknown);
  }

  /** Remove a previously-registered UDF. Returns true if it existed. */
  deleteFunction(name: string): boolean {
    this.ensureOpen();
    return this.inner.deleteFunction(name);
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
