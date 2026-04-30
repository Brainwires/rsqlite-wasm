import type { SqlValue, Row } from "./types.js";

interface WasmModule {
  default: (input?: RequestInfo | URL) => Promise<unknown>;
  WasmDatabase: {
    new (): WasmDatabaseInstance;
    openInMemory(): WasmDatabaseInstance;
    openWithOpfs(name: string): Promise<WasmDatabaseInstance>;
    openWithIdb(name: string): Promise<WasmDatabaseInstance>;
    fromBuffer(data: Uint8Array): WasmDatabaseInstance;
  };
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

type WorkerRequest =
  | { id: number; type: "open"; name?: string; backend?: string }
  | { id: number; type: "openInMemory" }
  | { id: number; type: "fromBuffer"; data: Uint8Array }
  | { id: number; type: "exec"; sql: string; params?: SqlValue[] }
  | { id: number; type: "query"; sql: string; params?: SqlValue[] }
  | { id: number; type: "queryOne"; sql: string; params?: SqlValue[] }
  | { id: number; type: "execMany"; sql: string }
  | { id: number; type: "toBuffer" }
  | { id: number; type: "flush" }
  | { id: number; type: "close" };

type WorkerResponse =
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

let wasmModule: WasmModule | null = null;
let db: WasmDatabaseInstance | null = null;

async function loadWasm(): Promise<WasmModule> {
  if (wasmModule) return wasmModule;
  const mod: WasmModule = await import(
    /* webpackIgnore: true */
    new URL("../pkg/rsqlite_wasm.js", import.meta.url).href
  );
  await mod.default();
  wasmModule = mod;
  return mod;
}

async function handleMessage(msg: WorkerRequest): Promise<WorkerResponse> {
  try {
    switch (msg.type) {
      case "open": {
        const mod = await loadWasm();
        if (msg.backend === "opfs") {
          db = await mod.WasmDatabase.openWithOpfs(msg.name ?? "rsqlite");
        } else if (msg.backend === "indexeddb") {
          db = await mod.WasmDatabase.openWithIdb(msg.name ?? "rsqlite");
        } else {
          db = new mod.WasmDatabase();
        }
        return { id: msg.id, ok: true };
      }
      case "openInMemory": {
        const mod = await loadWasm();
        db = mod.WasmDatabase.openInMemory();
        return { id: msg.id, ok: true };
      }
      case "fromBuffer": {
        const mod = await loadWasm();
        db = mod.WasmDatabase.fromBuffer(msg.data);
        return { id: msg.id, ok: true };
      }
      case "exec": {
        if (!db) throw new Error("Database not open");
        const result = msg.params?.length
          ? Number(db.execParams(msg.sql, msg.params))
          : Number(db.exec(msg.sql));
        return { id: msg.id, ok: true, result };
      }
      case "query": {
        if (!db) throw new Error("Database not open");
        const rows = msg.params?.length
          ? db.queryParams(msg.sql, msg.params)
          : db.query(msg.sql);
        return { id: msg.id, ok: true, result: rows };
      }
      case "queryOne": {
        if (!db) throw new Error("Database not open");
        const allRows = msg.params?.length
          ? db.queryParams(msg.sql, msg.params)
          : db.query(msg.sql);
        const row = (allRows as unknown[])[0] ?? null;
        return { id: msg.id, ok: true, result: row };
      }
      case "execMany": {
        if (!db) throw new Error("Database not open");
        db.execMany(msg.sql);
        return { id: msg.id, ok: true };
      }
      case "toBuffer": {
        if (!db) throw new Error("Database not open");
        const buf = db.toBuffer();
        return { id: msg.id, ok: true, result: buf };
      }
      case "flush": {
        if (!db) throw new Error("Database not open");
        db.flush();
        return { id: msg.id, ok: true };
      }
      case "close": {
        if (db) {
          db.free();
          db = null;
        }
        return { id: msg.id, ok: true };
      }
      default:
        throw new Error(`Unknown message type: ${(msg as { type: string }).type}`);
    }
  } catch (e) {
    const error = e instanceof Error ? e.message : String(e);
    return { id: msg.id, ok: false, error };
  }
}

self.onmessage = async (event: MessageEvent<WorkerRequest>) => {
  const response = await handleMessage(event.data);
  self.postMessage(response);
};
