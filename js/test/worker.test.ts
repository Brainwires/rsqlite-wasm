// Tests for src/worker.ts — the Web Worker entry point. handleMessage is not
// exported, so we drive it through the module-level `self.onmessage` handler
// (which worker.ts installs at import time) and capture replies via a stubbed
// `self.postMessage`. The browser wasm import is replaced with vi.mock just
// like index.test.ts (vitest config aliases the file:// URL so it resolves).

import { describe, it, expect, beforeEach, afterEach, beforeAll, vi } from "vitest";

const { wasmUrl, calls, makeInstance } = vi.hoisted(() => {
  const url = new URL("../src/wasm/rsqlite_wasm.js", import.meta.url).href;
  const registry: {
    defaultInit: ReturnType<typeof vi.fn>;
    ctor: ReturnType<typeof vi.fn>;
    openInMemory: ReturnType<typeof vi.fn>;
    openWithOpfs: ReturnType<typeof vi.fn>;
    openWithIdb: ReturnType<typeof vi.fn>;
    fromBuffer: ReturnType<typeof vi.fn>;
    instances: ReturnType<typeof mk>[];
  } = {
    defaultInit: vi.fn(),
    ctor: vi.fn(),
    openInMemory: vi.fn(),
    openWithOpfs: vi.fn(),
    openWithIdb: vi.fn(),
    fromBuffer: vi.fn(),
    instances: [],
  };
  function mk() {
    const inst = {
      exec: vi.fn((_sql: string) => 3n),
      execParams: vi.fn((_sql: string, _p: unknown[]) => 4n),
      query: vi.fn((_sql: string) => [{ a: 1 }, { a: 2 }]),
      queryParams: vi.fn((_sql: string, _p: unknown[]) => [{ b: 1 }]),
      execMany: vi.fn(() => undefined),
      toBuffer: vi.fn(() => new Uint8Array([7, 7])),
      flush: vi.fn(() => undefined),
      close: vi.fn(() => undefined),
      free: vi.fn(() => undefined),
      createFunction: vi.fn(
        (_n: string, _a: number, _f: (...args: unknown[]) => unknown) => undefined
      ),
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

// Stub `self` BEFORE importing worker.ts so the module-level
// `self.onmessage = ...` assignment lands on our stub. postMessage records
// the worker's replies.
const posted: unknown[] = [];
let onmessage: ((event: { data: unknown }) => void | Promise<void>) | null = null;

const selfStub = {
  set onmessage(fn: (event: { data: unknown }) => void | Promise<void>) {
    onmessage = fn;
  },
  get onmessage() {
    return onmessage as (event: { data: unknown }) => void;
  },
  postMessage(msg: unknown) {
    posted.push(msg);
  },
};

beforeAll(() => {
  (globalThis as unknown as { self: unknown }).self = selfStub;
});

// Import after `self` is stubbed and the wasm mock is registered.
beforeAll(async () => {
  await import("../src/worker");
});

type Resp = { id: number; ok: boolean; result?: unknown; error?: string };

// Post a message to the worker's onmessage handler and return its reply.
// The handler is async (awaits handleMessage then postMessage), so we await
// the returned promise from invoking it.
async function send(msg: Record<string, unknown>): Promise<Resp> {
  posted.length = 0;
  await onmessage!({ data: msg });
  // postMessage is called synchronously after the await chain resolves.
  expect(posted.length).toBe(1);
  return posted[0] as Resp;
}

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

afterEach(async () => {
  // Reset module-level `db` to null between tests so the !db guard branch is
  // reachable. close sets db = null.
  await send({ id: 9999, type: "close" });
});

describe("worker handleMessage — open variants", () => {
  it("open default backend uses constructor", async () => {
    const r = await send({ id: 1, type: "open" });
    expect(r).toEqual({ id: 1, ok: true });
    expect(calls.ctor).toHaveBeenCalledTimes(1);
  });

  it("open opfs backend forwards name + BigInt chunkSize + maxShards", async () => {
    const r = await send({
      id: 2,
      type: "open",
      backend: "opfs",
      name: "db1",
      chunkSize: 2048,
      maxShards: 5,
    });
    expect(r.ok).toBe(true);
    expect(calls.openWithOpfs).toHaveBeenCalledWith("db1", 2048n, 5);
  });

  it("open opfs default name + undefined chunkSize", async () => {
    await send({ id: 3, type: "open", backend: "opfs" });
    expect(calls.openWithOpfs).toHaveBeenCalledWith("rsqlite", undefined, undefined);
  });

  it("open indexeddb backend forwards name + BigInt chunkSize", async () => {
    await send({ id: 4, type: "open", backend: "indexeddb", name: "ix", chunkSize: 64 });
    expect(calls.openWithIdb).toHaveBeenCalledWith("ix", 64n);
  });

  it("open indexeddb default name", async () => {
    await send({ id: 5, type: "open", backend: "indexeddb" });
    expect(calls.openWithIdb).toHaveBeenCalledWith("rsqlite", undefined);
  });

  it("openInMemory", async () => {
    const r = await send({ id: 6, type: "openInMemory" });
    expect(r).toEqual({ id: 6, ok: true });
    expect(calls.openInMemory).toHaveBeenCalledTimes(1);
  });

  it("fromBuffer passes data", async () => {
    const data = new Uint8Array([1, 2, 3]);
    const r = await send({ id: 7, type: "fromBuffer", data });
    expect(r.ok).toBe(true);
    expect(calls.fromBuffer).toHaveBeenCalledWith(data);
  });
});

describe("worker handleMessage — ops with open db", () => {
  beforeEach(async () => {
    await send({ id: 100, type: "openInMemory" });
  });

  it("exec without params", async () => {
    const inst = lastInstance();
    const r = await send({ id: 11, type: "exec", sql: "X" });
    expect(r.result).toBe(3);
    expect(inst.exec).toHaveBeenCalledWith("X");
  });

  it("exec with params", async () => {
    const inst = lastInstance();
    const r = await send({ id: 12, type: "exec", sql: "X", params: [1] });
    expect(r.result).toBe(4);
    expect(inst.execParams).toHaveBeenCalledWith("X", [1]);
  });

  it("query without params", async () => {
    const inst = lastInstance();
    const r = await send({ id: 13, type: "query", sql: "S" });
    expect(r.result).toEqual([{ a: 1 }, { a: 2 }]);
    expect(inst.query).toHaveBeenCalled();
  });

  it("query with params", async () => {
    const inst = lastInstance();
    const r = await send({ id: 14, type: "query", sql: "S", params: [9] });
    expect(r.result).toEqual([{ b: 1 }]);
    expect(inst.queryParams).toHaveBeenCalledWith("S", [9]);
  });

  it("queryOne without params returns first row", async () => {
    const r = await send({ id: 15, type: "queryOne", sql: "S" });
    expect(r.result).toEqual({ a: 1 });
  });

  it("queryOne with params returns first row", async () => {
    const r = await send({ id: 16, type: "queryOne", sql: "S", params: [1] });
    expect(r.result).toEqual({ b: 1 });
  });

  it("queryOne returns null when no rows", async () => {
    const inst = lastInstance();
    inst.query.mockReturnValueOnce([]);
    const r = await send({ id: 17, type: "queryOne", sql: "S" });
    expect(r.result).toBeNull();
  });

  it("execMany", async () => {
    const inst = lastInstance();
    const r = await send({ id: 18, type: "execMany", sql: "A;B;" });
    expect(r.ok).toBe(true);
    expect(inst.execMany).toHaveBeenCalledWith("A;B;");
  });

  it("toBuffer", async () => {
    const r = await send({ id: 19, type: "toBuffer" });
    expect(r.result).toEqual(new Uint8Array([7, 7]));
  });

  it("flush", async () => {
    const inst = lastInstance();
    const r = await send({ id: 20, type: "flush" });
    expect(r.ok).toBe(true);
    expect(inst.flush).toHaveBeenCalled();
  });

  it("createFunction rehydrates fnSource and calls inner", async () => {
    const inst = lastInstance();
    const r = await send({
      id: 21,
      type: "createFunction",
      name: "addone",
      nArgs: 1,
      fnSource: "(x) => x + 1",
    });
    expect(r.ok).toBe(true);
    expect(inst.createFunction).toHaveBeenCalledWith("addone", 1, expect.any(Function));
    // Exercise the rehydrated wrapper: it should apply fnSource to its args.
    const wrapper = inst.createFunction.mock.calls[0][2] as (...a: unknown[]) => unknown;
    expect(wrapper(41)).toBe(42);
  });

  it("close frees db and returns ok", async () => {
    const inst = lastInstance();
    const r = await send({ id: 22, type: "close" });
    expect(r.ok).toBe(true);
    expect(inst.free).toHaveBeenCalled();
  });
});

describe("worker handleMessage — error paths (db not open)", () => {
  // afterEach closes the db, so at the start of each test here db is null.
  it.each([
    "exec",
    "query",
    "queryOne",
    "execMany",
    "toBuffer",
    "flush",
    "createFunction",
  ])("%s without an open db returns ok:false 'Database not open'", async (type) => {
    const r = await send({ id: 30, type, sql: "X", name: "f", nArgs: 1, fnSource: "()=>1" });
    expect(r.ok).toBe(false);
    expect(r.error).toBe("Database not open");
  });

  it("close with no db still returns ok (db stays null)", async () => {
    const r = await send({ id: 31, type: "close" });
    expect(r).toEqual({ id: 31, ok: true });
  });

  it("unknown message type returns ok:false with message", async () => {
    const r = await send({ id: 32, type: "frobnicate" });
    expect(r.ok).toBe(false);
    expect(r.error).toMatch(/Unknown message type: frobnicate/);
  });

  it("outer catch stringifies non-Error throws", async () => {
    // Open a db, then make an op throw a non-Error to hit the String(e) branch.
    await send({ id: 33, type: "openInMemory" });
    const inst = lastInstance();
    inst.exec.mockImplementationOnce(() => {
      throw "raw string boom";
    });
    const r = await send({ id: 34, type: "exec", sql: "X" });
    expect(r.ok).toBe(false);
    expect(r.error).toBe("raw string boom");
  });
});
