import type { SqlValue, Row, DatabaseOptions } from "./types.js";

type WorkerResponse =
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

export class WorkerDatabase {
  private worker: Worker;
  private nextId = 1;
  private pending = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>();
  private closed = false;

  private constructor(worker: Worker) {
    this.worker = worker;
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const { id, ...rest } = event.data;
      const entry = this.pending.get(id);
      if (!entry) return;
      this.pending.delete(id);
      if (rest.ok) {
        entry.resolve(rest.result);
      } else {
        entry.reject(new Error(rest.error));
      }
    };
  }

  private send(msg: Record<string, unknown>): Promise<unknown> {
    if (this.closed) throw new Error("Database is closed");
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, ...msg });
    });
  }

  static async open(name?: string, options?: DatabaseOptions): Promise<WorkerDatabase> {
    const workerUrl = new URL("./worker.js", import.meta.url);
    const worker = new Worker(workerUrl, { type: "module" });
    const db = new WorkerDatabase(worker);
    await db.send({
      type: "open",
      name,
      backend: options?.backend ?? "opfs",
    });
    return db;
  }

  static async openInMemory(): Promise<WorkerDatabase> {
    const workerUrl = new URL("./worker.js", import.meta.url);
    const worker = new Worker(workerUrl, { type: "module" });
    const db = new WorkerDatabase(worker);
    await db.send({ type: "openInMemory" });
    return db;
  }

  static async fromBuffer(buffer: Uint8Array | ArrayBuffer): Promise<WorkerDatabase> {
    const workerUrl = new URL("./worker.js", import.meta.url);
    const worker = new Worker(workerUrl, { type: "module" });
    const db = new WorkerDatabase(worker);
    const data = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
    await db.send({ type: "fromBuffer", data });
    return db;
  }

  async exec(sql: string, params?: SqlValue[]): Promise<number> {
    return (await this.send({ type: "exec", sql, params })) as number;
  }

  async query<T extends Row = Row>(sql: string, params?: SqlValue[]): Promise<T[]> {
    return (await this.send({ type: "query", sql, params })) as T[];
  }

  async queryOne<T extends Row = Row>(sql: string, params?: SqlValue[]): Promise<T | null> {
    return (await this.send({ type: "queryOne", sql, params })) as T | null;
  }

  async execMany(sql: string): Promise<void> {
    await this.send({ type: "execMany", sql });
  }

  async toBuffer(): Promise<Uint8Array> {
    return (await this.send({ type: "toBuffer" })) as Uint8Array;
  }

  async flush(): Promise<void> {
    await this.send({ type: "flush" });
  }

  async close(): Promise<void> {
    if (!this.closed) {
      await this.send({ type: "close" });
      this.worker.terminate();
      this.closed = true;
    }
  }

  get isClosed(): boolean {
    return this.closed;
  }
}
