// Tests for src/worker-proxy.ts — the WorkerDatabase main-thread async proxy.
// It talks to a real Web Worker via postMessage; we stub globalThis.Worker
// with a fake that records outgoing messages and lets the test inject
// responses by calling the proxy's installed `onmessage`. No real worker or
// wasm is loaded.

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { WorkerDatabase } from "../src/worker-proxy";

interface SentMessage {
  id: number;
  type: string;
  [k: string]: unknown;
}

// Tracks every FakeWorker created during a test so helpers can reach the most
// recent one and drive its onmessage / inspect postMessage / terminate.
let workers: FakeWorker[] = [];

class FakeWorker {
  static lastUrl: string | URL | undefined;
  static lastOptions: unknown;
  onmessage: ((event: { data: unknown }) => void) | null = null;
  sent: SentMessage[] = [];
  terminated = false;

  constructor(url: string | URL, options?: unknown) {
    FakeWorker.lastUrl = url;
    FakeWorker.lastOptions = options;
    workers.push(this);
  }

  postMessage(msg: SentMessage) {
    this.sent.push(msg);
  }

  terminate() {
    this.terminated = true;
  }

  // Test helper: deliver a worker response back to the proxy.
  reply(data: unknown) {
    this.onmessage?.({ data });
  }

  // Reply OK to the last message sent (mirrors how the real worker echoes id).
  replyOkLast(result?: unknown) {
    const last = this.sent[this.sent.length - 1];
    this.reply({ id: last.id, ok: true, result });
  }
}

function lastWorker() {
  return workers[workers.length - 1];
}

beforeEach(() => {
  workers = [];
  (globalThis as unknown as { Worker: unknown }).Worker = FakeWorker;
});

afterEach(() => {
  delete (globalThis as unknown as { Worker?: unknown }).Worker;
  vi.restoreAllMocks();
});

// Open a WorkerDatabase by kicking off the static factory and auto-replying
// OK to the initial open message it sends.
async function openProxy(
  factory: () => Promise<WorkerDatabase>
): Promise<WorkerDatabase> {
  const p = factory();
  // The constructor + send happen synchronously; the worker now has one
  // pending "open"-style message. Reply to it so the factory resolves.
  await Promise.resolve();
  lastWorker().replyOkLast();
  return p;
}

describe("WorkerDatabase.open factory", () => {
  it("creates a module worker and sends an open request with defaults", async () => {
    const p = WorkerDatabase.open();
    await Promise.resolve();
    const w = lastWorker();
    expect(w.sent[0]).toMatchObject({
      type: "open",
      name: undefined,
      backend: "opfs",
      chunkSize: undefined,
      maxShards: undefined,
    });
    expect(FakeWorker.lastOptions).toEqual({ type: "module" });
    w.replyOkLast();
    const db = await p;
    expect(db.isClosed).toBe(false);
  });

  it("forwards name/backend/chunkSize/maxShards and a custom workerUrl", async () => {
    const p = WorkerDatabase.open("mydb", {
      backend: "indexeddb",
      chunkSize: 123,
      maxShards: 4,
      workerUrl: "https://example.com/w.js",
    });
    await Promise.resolve();
    const w = lastWorker();
    expect(w.sent[0]).toMatchObject({
      type: "open",
      name: "mydb",
      backend: "indexeddb",
      chunkSize: 123,
      maxShards: 4,
    });
    expect(FakeWorker.lastUrl).toBe("https://example.com/w.js");
    w.replyOkLast();
    await p;
  });

  it("openInMemory sends openInMemory", async () => {
    const p = WorkerDatabase.openInMemory();
    await Promise.resolve();
    const w = lastWorker();
    expect(w.sent[0]).toMatchObject({ type: "openInMemory" });
    w.replyOkLast();
    await p;
  });

  it("fromBuffer with Uint8Array sends data as-is", async () => {
    const data = new Uint8Array([1, 2, 3]);
    const p = WorkerDatabase.fromBuffer(data);
    await Promise.resolve();
    const w = lastWorker();
    expect(w.sent[0]).toMatchObject({ type: "fromBuffer" });
    expect(w.sent[0].data).toBe(data);
    w.replyOkLast();
    await p;
  });

  it("fromBuffer with ArrayBuffer wraps in Uint8Array", async () => {
    const ab = new ArrayBuffer(4);
    const p = WorkerDatabase.fromBuffer(ab);
    await Promise.resolve();
    const w = lastWorker();
    expect(w.sent[0].data).toBeInstanceOf(Uint8Array);
    expect((w.sent[0].data as Uint8Array).length).toBe(4);
    w.replyOkLast();
    await p;
  });
});

describe("WorkerDatabase op methods", () => {
  it("exec resolves with the numeric result", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.exec("INSERT", [1]);
    await Promise.resolve();
    expect(w.sent[w.sent.length - 1]).toMatchObject({
      type: "exec",
      sql: "INSERT",
      params: [1],
    });
    w.replyOkLast(5);
    expect(await p).toBe(5);
  });

  it("query resolves with rows", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.query("SELECT 1");
    await Promise.resolve();
    w.replyOkLast([{ a: 1 }]);
    expect(await p).toEqual([{ a: 1 }]);
  });

  it("queryOne resolves with a single row", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.queryOne("SELECT 1");
    await Promise.resolve();
    w.replyOkLast({ a: 9 });
    expect(await p).toEqual({ a: 9 });
  });

  it("execMany resolves void", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.execMany("A;B;");
    await Promise.resolve();
    expect(w.sent[w.sent.length - 1]).toMatchObject({ type: "execMany", sql: "A;B;" });
    w.replyOkLast();
    expect(await p).toBeUndefined();
  });

  it("toBuffer resolves the buffer", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.toBuffer();
    await Promise.resolve();
    const buf = new Uint8Array([4, 5]);
    w.replyOkLast(buf);
    expect(await p).toBe(buf);
  });

  it("flush resolves void", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.flush();
    await Promise.resolve();
    w.replyOkLast();
    expect(await p).toBeUndefined();
  });

  it("createFunction serializes fn via toString", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const fn = (x: number) => x * 2;
    const p = db.createFunction("dbl", 1, fn as never);
    await Promise.resolve();
    expect(w.sent[w.sent.length - 1]).toMatchObject({
      type: "createFunction",
      name: "dbl",
      nArgs: 1,
    });
    expect(w.sent[w.sent.length - 1].fnSource).toBe(fn.toString());
    w.replyOkLast();
    await p;
  });
});

describe("WorkerDatabase response routing", () => {
  it("rejects with an Error when the worker replies ok:false", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.exec("BAD");
    await Promise.resolve();
    const last = w.sent[w.sent.length - 1];
    w.reply({ id: last.id, ok: false, error: "boom" });
    await expect(p).rejects.toThrow("boom");
  });

  it("ignores responses with an unknown id", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.exec("X");
    await Promise.resolve();
    const last = w.sent[w.sent.length - 1];
    // Unknown id: must be silently ignored (no resolve/reject).
    w.reply({ id: 999999, ok: true, result: "wrong" });
    // Now deliver the correct one.
    w.reply({ id: last.id, ok: true, result: 1 });
    expect(await p).toBe(1);
  });

  it("assigns incrementing ids", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const idsBefore = w.sent.map((m) => m.id);
    db.exec("A");
    db.exec("B");
    await Promise.resolve();
    const newIds = w.sent.map((m) => m.id).slice(idsBefore.length);
    expect(newIds[1]).toBe(newIds[0] + 1);
  });
});

describe("WorkerDatabase close", () => {
  it("close sends close, terminates the worker, and is idempotent", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const p = db.close();
    await Promise.resolve();
    expect(w.sent[w.sent.length - 1]).toMatchObject({ type: "close" });
    w.replyOkLast();
    await p;
    expect(w.terminated).toBe(true);
    expect(db.isClosed).toBe(true);

    // Second close is a no-op: no new message, no throw.
    const sentCount = w.sent.length;
    await db.close();
    expect(w.sent.length).toBe(sentCount);
  });

  it("send after close throws synchronously 'Database is closed'", async () => {
    const db = await openProxy(() => WorkerDatabase.openInMemory());
    const w = lastWorker();
    const cp = db.close();
    await Promise.resolve();
    w.replyOkLast();
    await cp;
    // exec() awaits send(); send throws synchronously, surfacing as rejection.
    await expect(db.exec("X")).rejects.toThrow("Database is closed");
  });
});
