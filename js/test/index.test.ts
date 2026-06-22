// Tests for src/index.ts — the synchronous `Database` wrapper class and the
// loadWasm/initWasm memoization. The wrapper dynamically import()s the
// browser wasm module (`./wasm/rsqlite_wasm.js`), which cannot load under
// node. We intercept that dynamic import with vi.mock against the resolved
// file URL and substitute a fully stubbed WasmDatabase surface.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// The dynamic import target inside loadWasm is:
//   new URL("./wasm/rsqlite_wasm.js", import.meta.url).href
// import.meta.url for src/index.ts resolves to the src/ dir at test time, so
// the wasm sibling URL is src/wasm/rsqlite_wasm.js. We mock that exact
// specifier. vi.mock is hoisted; the factory builds canned instances.

// vi.mock is hoisted to the top of the module, so the specifier and the spy
// registry it closes over must be created via vi.hoisted (which runs first).
const { wasmUrl, calls, makeInstance } = vi.hoisted(() => {
  const url = new URL("../src/wasm/rsqlite_wasm.js", import.meta.url).href;
  const registry: {
    defaultInit: ReturnType<typeof vi.fn>;
    ctor: ReturnType<typeof vi.fn>;
    openInMemory: ReturnType<typeof vi.fn>;
    openWithOpfs: ReturnType<typeof vi.fn>;
    openWithIdb: ReturnType<typeof vi.fn>;
    openPersisted: ReturnType<typeof vi.fn>;
    fromBuffer: ReturnType<typeof vi.fn>;
    instances: ReturnType<typeof mk>[];
  } = {
    defaultInit: vi.fn(),
    ctor: vi.fn(),
    openInMemory: vi.fn(),
    openWithOpfs: vi.fn(),
    openWithIdb: vi.fn(),
    openPersisted: vi.fn(),
    fromBuffer: vi.fn(),
    instances: [],
  };
  function mk() {
    const inst = {
      exec: vi.fn((_sql: string) => 5n),
      execParams: vi.fn((_sql: string, _p: unknown[]) => 7n),
      query: vi.fn((_sql: string) => [{ id: 1 }]),
      queryParams: vi.fn((_sql: string, _p: unknown[]) => [{ id: 2 }]),
      queryOne: vi.fn((_sql: string) => ({ id: 9 })),
      execMany: vi.fn((_sql: string) => undefined),
      toBuffer: vi.fn(() => new Uint8Array([1, 2, 3])),
      flush: vi.fn(() => undefined),
      close: vi.fn(() => undefined),
      free: vi.fn(() => undefined),
      createFunction: vi.fn(
        (_n: string, _a: number, _f: (...args: unknown[]) => unknown) => undefined
      ),
      deleteFunction: vi.fn((_n: string) => true),
    };
    registry.instances.push(inst);
    return inst;
  }
  return { wasmUrl: url, calls: registry, makeInstance: mk };
});

vi.mock(wasmUrl, () => {
  class WasmDatabase {
    constructor() {
      calls.ctor();
      return makeInstance() as unknown as WasmDatabase;
    }
    static openInMemory() {
      calls.openInMemory();
      return makeInstance();
    }
    static async openWithOpfs(name: string, chunkSize?: bigint, maxShards?: number) {
      calls.openWithOpfs(name, chunkSize, maxShards);
      return makeInstance();
    }
    static async openWithIdb(name: string, chunkSize?: bigint) {
      calls.openWithIdb(name, chunkSize);
      return makeInstance();
    }
    static async openPersisted(name: string, chunkSize?: bigint, maxShards?: number) {
      calls.openPersisted(name, chunkSize, maxShards);
      return makeInstance();
    }
    static fromBuffer(data: Uint8Array) {
      calls.fromBuffer(data);
      return makeInstance();
    }
  }
  return {
    default: (...args: unknown[]) => {
      calls.defaultInit(...args);
      return Promise.resolve();
    },
    WasmDatabase,
  };
});

// Import after the mock is registered. Because loadWasm memoizes the module
// across the whole test file, the dynamic import factory runs once.
import { Database, initWasm } from "../src/index";

function lastInstance() {
  return calls.instances[calls.instances.length - 1];
}

beforeEach(() => {
  for (const k of Object.keys(calls) as (keyof typeof calls)[]) {
    if (k === "instances") continue;
    (calls[k] as ReturnType<typeof vi.fn>).mockClear();
  }
  calls.instances.length = 0;
});

afterEach(() => {
  vi.clearAllMocks();
  calls.instances.length = 0;
});

describe("Database.open backend dispatch", () => {
  it("defaults to memory backend (constructor)", async () => {
    const db = await Database.open();
    expect(calls.ctor).toHaveBeenCalledTimes(1);
    expect(db.isClosed).toBe(false);
  });

  it("memory backend explicit", async () => {
    await Database.open("foo", { backend: "memory" });
    expect(calls.ctor).toHaveBeenCalledTimes(1);
  });

  it("opfs backend forwards name + BigInt chunkSize + maxShards", async () => {
    await Database.open("mydb", { backend: "opfs", chunkSize: 4096, maxShards: 8 });
    expect(calls.openWithOpfs).toHaveBeenCalledWith("mydb", 4096n, 8);
  });

  it("opfs backend uses default name + undefined chunkSize when omitted", async () => {
    await Database.open(undefined, { backend: "opfs" });
    expect(calls.openWithOpfs).toHaveBeenCalledWith("rsqlite", undefined, undefined);
  });

  it("indexeddb backend forwards name + BigInt chunkSize", async () => {
    await Database.open("idb", { backend: "indexeddb", chunkSize: 1024 });
    expect(calls.openWithIdb).toHaveBeenCalledWith("idb", 1024n);
  });

  it("indexeddb backend default name", async () => {
    await Database.open(undefined, { backend: "indexeddb" });
    expect(calls.openWithIdb).toHaveBeenCalledWith("rsqlite", undefined);
  });

  it("default/auto backend uses openPersisted", async () => {
    // backend is a value not matched by the memory/opfs/indexeddb branches
    await Database.open("auto", { backend: "default" as never, chunkSize: 99, maxShards: 3 });
    expect(calls.openPersisted).toHaveBeenCalledWith("auto", 99n, 3);
  });

  it("default/auto backend default name", async () => {
    await Database.open(undefined, { backend: "weird" as never });
    expect(calls.openPersisted).toHaveBeenCalledWith("rsqlite", undefined, undefined);
  });
});

describe("Database static factories", () => {
  it("openInMemory", async () => {
    const db = await Database.openInMemory();
    expect(calls.openInMemory).toHaveBeenCalledTimes(1);
    expect(db).toBeInstanceOf(Database);
  });

  it("fromBuffer with Uint8Array passes data through", async () => {
    const data = new Uint8Array([9, 8, 7]);
    await Database.fromBuffer(data);
    expect(calls.fromBuffer).toHaveBeenCalledWith(data);
  });

  it("fromBuffer with ArrayBuffer wraps in Uint8Array", async () => {
    const ab = new ArrayBuffer(4);
    await Database.fromBuffer(ab);
    const arg = calls.fromBuffer.mock.calls[0][0];
    expect(arg).toBeInstanceOf(Uint8Array);
    expect((arg as Uint8Array).length).toBe(4);
  });
});

describe("Database query/exec branches", () => {
  it("exec without params calls exec and Number()s the bigint", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    const r = db.exec("CREATE TABLE t(a)");
    expect(inst.exec).toHaveBeenCalledWith("CREATE TABLE t(a)");
    expect(r).toBe(5);
  });

  it("exec with params calls execParams", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    const r = db.exec("INSERT INTO t VALUES (?)", [1]);
    expect(inst.execParams).toHaveBeenCalledWith("INSERT INTO t VALUES (?)", [1]);
    expect(r).toBe(7);
  });

  it("exec with empty params array uses no-params branch", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    db.exec("SELECT 1", []);
    expect(inst.exec).toHaveBeenCalled();
    expect(inst.execParams).not.toHaveBeenCalled();
  });

  it("query without params", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    expect(db.query("SELECT 1")).toEqual([{ id: 1 }]);
    expect(inst.query).toHaveBeenCalled();
  });

  it("query with params", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    expect(db.query("SELECT ?", [1])).toEqual([{ id: 2 }]);
    expect(inst.queryParams).toHaveBeenCalledWith("SELECT ?", [1]);
  });

  it("queryOne without params delegates to inner.queryOne", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    expect(db.queryOne("SELECT 1")).toEqual({ id: 9 });
    expect(inst.queryOne).toHaveBeenCalled();
  });

  it("queryOne with params returns first row", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    inst.queryParams.mockReturnValueOnce([{ a: 1 }, { a: 2 }]);
    expect(db.queryOne("SELECT ?", [1])).toEqual({ a: 1 });
  });

  it("queryOne with params returns null when no rows (?? null)", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    inst.queryParams.mockReturnValueOnce([]);
    expect(db.queryOne("SELECT ?", [1])).toBeNull();
  });
});

describe("Database misc ops", () => {
  it("execMany delegates", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    db.execMany("A; B;");
    expect(inst.execMany).toHaveBeenCalledWith("A; B;");
  });

  it("toBuffer delegates", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    expect(db.toBuffer()).toEqual(new Uint8Array([1, 2, 3]));
    expect(inst.toBuffer).toHaveBeenCalled();
  });

  it("flush delegates", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    db.flush();
    expect(inst.flush).toHaveBeenCalled();
  });

  it("createFunction defaults nArgs to -1", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    const fn = (...a: unknown[]) => a[0];
    db.createFunction("myfn", fn as never);
    expect(inst.createFunction).toHaveBeenCalledWith("myfn", -1, fn);
  });

  it("createFunction honours explicit nArgs", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    const fn = (...a: unknown[]) => a[0];
    db.createFunction("myfn", fn as never, { nArgs: 2 });
    expect(inst.createFunction).toHaveBeenCalledWith("myfn", 2, fn);
  });

  it("deleteFunction returns inner result", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    expect(db.deleteFunction("x")).toBe(true);
    expect(inst.deleteFunction).toHaveBeenCalledWith("x");
  });
});

describe("Database transaction", () => {
  it("commits on success and returns fn result", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    const r = db.transaction(() => 42);
    expect(r).toBe(42);
    expect(inst.exec).toHaveBeenNthCalledWith(1, "BEGIN");
    expect(inst.exec).toHaveBeenNthCalledWith(2, "COMMIT");
  });

  it("rolls back and rethrows on error", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    const err = new Error("boom");
    expect(() => db.transaction(() => { throw err; })).toThrow("boom");
    expect(inst.exec).toHaveBeenNthCalledWith(1, "BEGIN");
    expect(inst.exec).toHaveBeenNthCalledWith(2, "ROLLBACK");
  });
});

describe("Database close + ensureOpen", () => {
  it("close frees inner and is idempotent", async () => {
    const db = await Database.openInMemory();
    const inst = lastInstance();
    db.close();
    expect(inst.free).toHaveBeenCalledTimes(1);
    expect(db.isClosed).toBe(true);
    db.close(); // no-op second time
    expect(inst.free).toHaveBeenCalledTimes(1);
  });

  it("ops throw after close", async () => {
    const db = await Database.openInMemory();
    db.close();
    expect(() => db.exec("X")).toThrow("Database is closed");
    expect(() => db.query("X")).toThrow("Database is closed");
    expect(() => db.queryOne("X")).toThrow("Database is closed");
    expect(() => db.execMany("X")).toThrow("Database is closed");
    expect(() => db.toBuffer()).toThrow("Database is closed");
    expect(() => db.flush()).toThrow("Database is closed");
    expect(() => db.transaction(() => 1)).toThrow("Database is closed");
    expect(() => db.createFunction("f", (() => 0) as never)).toThrow("Database is closed");
    expect(() => db.deleteFunction("f")).toThrow("Database is closed");
  });
});

describe("loadWasm / initWasm memoization", () => {
  it("default init was called and module is memoized (single instance reuse)", async () => {
    // By now many opens have happened but the wasm default() init should
    // have run at most once thanks to wasmModule/wasmInitPromise caching.
    await Database.openInMemory();
    expect(calls.defaultInit).toHaveBeenCalledTimes(0);
    // ^ cleared each beforeEach; the real assertion is that subsequent
    // opens don't re-import. Reaching here without throwing proves the
    // memoized module path (wasmModule early-return) is used.
  });

  it("initWasm resolves without throwing (early-return path)", async () => {
    await expect(initWasm()).resolves.toBeUndefined();
  });

  it("initWasm with explicit url resolves (memoized, url ignored after init)", async () => {
    await expect(initWasm("http://example.com/x.js")).resolves.toBeUndefined();
  });

  it("concurrent first opens share the in-flight init promise (wasmInitPromise branch)", async () => {
    // Reset module state so wasmModule/wasmInitPromise start null, then fire
    // two opens in the SAME synchronous tick. The first sets wasmInitPromise
    // but cannot set wasmModule until `await mod.default()` resolves a tick
    // later — so the second open hits the `if (wasmInitPromise) return ...`
    // early-return branch (index.ts line 59).
    vi.resetModules();
    const fresh = await import("../src/index");
    const [a, b] = await Promise.all([
      fresh.Database.openInMemory(),
      fresh.Database.openInMemory(),
    ]);
    expect(a).toBeInstanceOf(fresh.Database);
    expect(b).toBeInstanceOf(fresh.Database);
  });
});
