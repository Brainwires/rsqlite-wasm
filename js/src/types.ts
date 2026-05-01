export type SqlValue = null | number | string | Uint8Array;

export type BindParams = SqlValue[] | Record<string, SqlValue>;

export interface Row {
  [column: string]: SqlValue;
}

export interface DatabaseOptions {
  /** Storage backend. Defaults to auto-detection (OPFS > IndexedDB). */
  backend?: "memory" | "opfs" | "indexeddb";
  /** Explicit URL for the worker script. When omitted, resolved via
   *  `new URL("./worker.js", import.meta.url)`. Bundlers that inline
   *  worker-proxy.js break `import.meta.url` resolution — pass the
   *  correct URL to work around that. */
  workerUrl?: string | URL;
  /** Per-shard cap in bytes for the multi-file VFS. Logical databases are
   *  spread across files of this size to escape browser per-file caps.
   *  Defaults to 1 GB (1 << 30). */
  chunkSize?: number;
  /** Maximum number of shards to pre-register on OPFS. SyncAccessHandle
   *  creation is async, but engine writes are sync, so every shard handle
   *  must exist before any query runs. Each unused shard is a zero-byte
   *  file. Defaults to 16 (= 16 GB at the default chunk size). Ignored
   *  for the IndexedDB backend, whose shards are created on demand. */
  maxShards?: number;
}
