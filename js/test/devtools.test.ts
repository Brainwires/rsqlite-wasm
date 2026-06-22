// Tests for src/devtools.ts. The bridge logic is engine-independent — it
// just multiplexes calls onto whatever Database-shaped object you give it —
// so we mock the Database surface here instead of building a real WASM db.
//
// Real-engine integration (the bridge actually executing SQL on a live
// rsqlite-wasm Database) is exercised by the Brainwires OPFS extension's
// Playwright suite, which loads the wasm and exposes a real db.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { exposeForDevtools, type ExposeForDevtoolsOptions } from "../src/devtools";

const GLOBAL_KEY = "__BRAINWIRES_RSQLITE_DEVTOOLS__";

// The bridge is off by default — every test opts in. Wrap once so the
// `enabled: true` is not repeated at each call site.
function expose(db: unknown, opts: Omit<ExposeForDevtoolsOptions, "enabled"> = {}) {
  return exposeForDevtools(db as Parameters<typeof exposeForDevtools>[0], {
    ...opts,
    enabled: true,
  });
}

interface MockDb {
  exec(sql: string, params?: unknown[]): number;
  execMany(sql: string): void;
  query<T = unknown>(sql: string, params?: unknown[]): T[];
  queryOne<T = unknown>(sql: string, params?: unknown[]): T | null;
  isClosed: boolean;
  // Mock metadata used by the test to assert the bridge actually called us
  log: Array<{ op: string; sql: string; params?: unknown[] }>;
}

function mockDb(): MockDb {
  const log: MockDb["log"] = [];
  let closed = false;
  return {
    log,
    get isClosed() {
      return closed;
    },
    set isClosed(v) {
      closed = v;
    },
    exec(sql, params) {
      log.push({ op: "exec", sql, params });
      return 1;
    },
    execMany(sql) {
      log.push({ op: "execMany", sql });
    },
    query<T>(sql: string, params?: unknown[]): T[] {
      log.push({ op: "query", sql, params });
      return [{ id: 1, name: "alice" }, { id: 2, name: "bob" }] as unknown as T[];
    },
    queryOne<T>(sql: string, params?: unknown[]): T | null {
      log.push({ op: "queryOne", sql, params });
      return ({ count: 42 } as unknown) as T;
    },
  } as unknown as MockDb;
}

beforeEach(() => {
  // ensure clean global between tests
  delete (globalThis as Record<string, unknown>)[GLOBAL_KEY];
  // silence the security warning the bridge emits on enable
  vi.spyOn(console, "warn").mockImplementation(() => {});
});
afterEach(() => {
  delete (globalThis as Record<string, unknown>)[GLOBAL_KEY];
  vi.restoreAllMocks();
});

function bridge() {
  return (globalThis as Record<string, unknown>)[GLOBAL_KEY] as {
    v: number;
    listDbs(): string[];
    info(name: string): { name: string; changeCounter: number; closed: boolean } | null;
    invoke(name: string, op: string, sql: string, params?: unknown[]): number;
    poll(id: number): { pending: true } | { pending: false; ok: boolean; value?: unknown; error?: { message: string } };
  };
}

async function callOp(name: string, op: string, sql: string, params?: unknown[]) {
  const id = bridge().invoke(name, op, sql, params);
  // Bridge dispatches via Promise.resolve().then(...) so we wait one tick.
  await Promise.resolve();
  await Promise.resolve();
  const r = bridge().poll(id);
  if (r.pending) throw new Error("still pending after microtask drain");
  return r;
}

describe("exposeForDevtools", () => {
  it("installs the bridge global on first call", () => {
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeUndefined();
    const db = mockDb();
    expose(db);
    const root = bridge();
    expect(root).toBeDefined();
    expect(root.v).toBe(1);
    expect(root.listDbs()).toEqual(["main"]);
  });

  it("warns when the bridge is enabled", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expose(mockDb());
    expect(warn).toHaveBeenCalled();
    expect(String(warn.mock.calls[0]?.[0])).toMatch(/same-origin|production/i);
  });

  it("registers under custom names and lists them all", () => {
    expose(mockDb(), { name: "users" });
    expose(mockDb(), { name: "logs" });
    expect(bridge().listDbs().sort()).toEqual(["logs", "users"]);
  });

  it("is a no-op unless enabled", () => {
    // No options at all → off by default.
    exposeForDevtools(mockDb() as never);
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeUndefined();
    // Explicit enabled: false → still off.
    exposeForDevtools(mockDb() as never, { enabled: false });
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeUndefined();
  });

  it("info() reports name + changeCounter + closed", () => {
    const db = mockDb();
    expose(db);
    expect(bridge().info("main")).toEqual({
      name: "main",
      changeCounter: 0,
      closed: false,
    });
    expect(bridge().info("missing")).toBeNull();
  });

  it("query round-trips through invoke/poll", async () => {
    const db = mockDb();
    expose(db);
    const r = await callOp("main", "query", "SELECT * FROM t", undefined);
    expect(r.pending).toBe(false);
    expect(r.ok).toBe(true);
    expect(r.value).toEqual([{ id: 1, name: "alice" }, { id: 2, name: "bob" }]);
    expect(db.log).toEqual([{ op: "query", sql: "SELECT * FROM t", params: undefined }]);
  });

  it("queryOne returns first row from mock", async () => {
    const db = mockDb();
    expose(db);
    const r = await callOp("main", "queryOne", "SELECT COUNT(*) FROM t");
    expect(r.ok).toBe(true);
    expect(r.value).toEqual({ count: 42 });
  });

  it("exec bumps changeCounter", async () => {
    const db = mockDb();
    expose(db);
    expect(bridge().info("main")?.changeCounter).toBe(0);
    await callOp("main", "exec", "UPDATE t SET name = 'x'", undefined);
    expect(bridge().info("main")?.changeCounter).toBe(1);
    await callOp("main", "exec", "INSERT INTO t VALUES (3, 'c')", undefined);
    expect(bridge().info("main")?.changeCounter).toBe(2);
    // Reads do not bump
    await callOp("main", "query", "SELECT * FROM t", undefined);
    expect(bridge().info("main")?.changeCounter).toBe(2);
  });

  it("execMany also bumps changeCounter", async () => {
    const db = mockDb();
    expose(db);
    await callOp("main", "execMany", "CREATE TABLE x(a); CREATE INDEX y ON x(a);");
    expect(bridge().info("main")?.changeCounter).toBe(1);
  });

  it("page-side direct call to db.exec ALSO bumps changeCounter", () => {
    // exposeForDevtools wraps db.exec/execMany so the user's own writes
    // are observable via the bridge's changeCounter.
    const db = mockDb();
    expose(db);
    db.exec("INSERT INTO t VALUES (10)");
    expect(bridge().info("main")?.changeCounter).toBe(1);
    db.execMany("UPDATE t SET a=1; UPDATE t SET a=2;");
    expect(bridge().info("main")?.changeCounter).toBe(2);
  });

  it("invoke for unknown db returns NotRegistered error", async () => {
    expose(mockDb());
    const r = await callOp("nope", "query", "SELECT 1");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toMatch(/not registered/);
  });

  it("invoke with an unknown op returns an error", async () => {
    expose(mockDb());
    const r = await callOp("main", "frobnicate", "SELECT 1");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toMatch(/unknown op/);
  });

  it("invoke when db.isClosed returns Closed error", async () => {
    const db = mockDb();
    expose(db);
    db.isClosed = true;
    const r = await callOp("main", "query", "SELECT 1");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toMatch(/closed/i);
  });

  it("propagates engine errors as poll() error", async () => {
    const db = mockDb();
    db.query = () => {
      throw new Error("syntax error near 'XYZ'");
    };
    expose(db);
    const r = await callOp("main", "query", "BAD SQL");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toBe("syntax error near 'XYZ'");
  });

  it("stringifies non-Error throws", async () => {
    const db = mockDb();
    db.query = () => {
      // eslint-disable-next-line @typescript-eslint/no-throw-literal
      throw "plain string failure";
    };
    expose(db);
    const r = await callOp("main", "query", "BAD SQL");
    expect(r.ok).toBe(false);
    expect(r.error?.message).toBe("plain string failure");
  });

  it("re-exposing the same name swaps in the new db (HMR-friendly)", async () => {
    const db1 = mockDb();
    const db2 = mockDb();
    expose(db1, { name: "live" });
    expose(db2, { name: "live" });
    await callOp("live", "query", "SELECT 1");
    expect(db1.log).toEqual([]);
    expect(db2.log).toHaveLength(1);
  });

  it("two exposed dbs share one bridge + invocation-id space", () => {
    expose(mockDb(), { name: "a" });
    expose(mockDb(), { name: "b" });
    const id1 = bridge().invoke("a", "query", "SELECT 1");
    const id2 = bridge().invoke("b", "query", "SELECT 1");
    expect(id2).toBe(id1 + 1); // shared nextId counter
  });

  it("release() removes only its own db when others remain", () => {
    expose(mockDb(), { name: "keep" });
    const release = expose(mockDb(), { name: "removable" });
    expect(bridge().listDbs().sort()).toEqual(["keep", "removable"]);
    release();
    // Other db still registered → bridge stays installed.
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeDefined();
    expect(bridge().listDbs()).toEqual(["keep"]);
  });

  it("release() tears down the bridge when the last db is removed", () => {
    const release = expose(mockDb(), { name: "removable" });
    expect(bridge().listDbs()).toContain("removable");
    release();
    expect((globalThis as Record<string, unknown>)[GLOBAL_KEY]).toBeUndefined();
  });

  it("poll on an unknown id returns Expired error", () => {
    expose(mockDb());
    const r = bridge().poll(99999);
    expect(r.pending).toBe(false);
    expect((r as { ok: boolean }).ok).toBe(false);
  });
});

// ── WorkerDatabase-shaped (Promise-returning) mocks ───────────────────────

interface AsyncMockDb {
  exec(sql: string, params?: unknown[]): Promise<number>;
  execMany(sql: string): Promise<void>;
  query<T = unknown>(sql: string, params?: unknown[]): Promise<T[]>;
  queryOne<T = unknown>(sql: string, params?: unknown[]): Promise<T | null>;
  isClosed: boolean;
  log: Array<{ op: string; sql: string }>;
}

function asyncMockDb(): AsyncMockDb {
  const log: AsyncMockDb["log"] = [];
  let closed = false;
  return {
    log,
    get isClosed() {
      return closed;
    },
    set isClosed(v) {
      closed = v;
    },
    async exec(sql) {
      log.push({ op: "exec", sql });
      return 1;
    },
    async execMany(sql) {
      log.push({ op: "execMany", sql });
    },
    async query(sql) {
      log.push({ op: "query", sql });
      return [{ via: "worker" }] as never;
    },
    async queryOne(sql) {
      log.push({ op: "queryOne", sql });
      return { one: 1 } as never;
    },
  } as unknown as AsyncMockDb;
}

async function callOpAsync(name: string, op: string, sql: string) {
  const id = bridge().invoke(name, op, sql);
  // Drain a few microtasks because the dispatcher awaits the async db
  for (let i = 0; i < 4; i++) await Promise.resolve();
  return bridge().poll(id);
}

describe("exposeForDevtools — async (WorkerDatabase-style)", () => {
  it("awaits Promise-returning query", async () => {
    const db = asyncMockDb();
    expose(db);
    const r = await callOpAsync("main", "query", "SELECT 1");
    expect(r.pending).toBe(false);
    expect((r as { ok: boolean }).ok).toBe(true);
    expect((r as { value: unknown }).value).toEqual([{ via: "worker" }]);
  });

  it("bumps changeCounter only AFTER the async exec resolves", async () => {
    const db = asyncMockDb();
    expose(db);
    const promise = db.exec("UPDATE t SET a=1");
    // Counter should still be 0 — exec hasn't resolved yet
    expect(bridge().info("main")?.changeCounter).toBe(0);
    await promise;
    expect(bridge().info("main")?.changeCounter).toBe(1);
  });

  it("propagates rejected Promises as error", async () => {
    const db = asyncMockDb();
    db.query = async () => {
      throw new Error("worker exploded");
    };
    expose(db);
    const r = await callOpAsync("main", "query", "SELECT 1");
    expect(r.pending).toBe(false);
    expect((r as { ok: boolean }).ok).toBe(false);
    expect((r as { error: { message: string } }).error.message).toBe("worker exploded");
  });
});
