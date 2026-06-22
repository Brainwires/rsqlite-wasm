# rsqlite-wasm

A pure Rust SQLite-compatible database engine that compiles to WebAssembly for use in browser Progressive Web Apps.

Databases created by rsqlite-wasm are **file-format compatible** with SQLite — you can open them with the `sqlite3` CLI and vice versa. This enables importing and exporting real `.sqlite` files in the browser.

## Features

- **Pure Rust** — zero C dependencies, builds cleanly for `wasm32-unknown-unknown`
- **SQLite file format** — binary-compatible with SQLite 3 databases
- **Browser persistence** — OPFS (primary) and IndexedDB (fallback) backends
- **Multi-file sharding** — logical databases shard transparently across 1 GB files to escape browser per-file size caps (see [Database size & sharding](#database-size--sharding))
- **Web Worker architecture** — all I/O runs off the main thread
- **Vector search** — built-in `vec_distance_cosine`, `vec_distance_l2`, and `vec_distance_dot` functions for embedding similarity search
- **JavaScript UDFs** — register synchronous JS callbacks as SQL scalar functions via `db.createFunction(name, fn)`
- **Small binary** — ~2 MB WASM with LTO + `opt-level=z`
- **720+ tests** — comprehensive coverage across all crates
- **Virtual-table foundation** — `CREATE VIRTUAL TABLE … USING module(...)`, writeable via INSERT, with built-in `series`, `kvstore`, `vec_index` (typed vector storage), `rtree` (multi-dim bounding boxes), and `fts5` (basic full-text search) modules; extensible via `vtab::register_module`

## SQL Support

The full deferred-feature inventory lives in [LIMITATIONS.md](./LIMITATIONS.md).
The list below is the headline-feature surface; LIMITATIONS is the truth-source for what doesn't work.



### Fully supported

- **DML:** SELECT, INSERT, UPDATE, DELETE with full WHERE/ORDER BY/LIMIT/OFFSET
- **Joins:** INNER JOIN, LEFT JOIN, CROSS JOIN
- **Aggregates:** COUNT, SUM, AVG, MIN, MAX, TOTAL, GROUP_CONCAT (with DISTINCT, custom separator)
- **Subqueries:** IN, EXISTS, scalar subqueries
- **Set operations:** UNION, UNION ALL
- **CTEs:** WITH ... AS (multiple, column renaming); WITH RECURSIVE
- **Views:** CREATE VIEW, DROP VIEW, SELECT from views
- **Expressions:** CASE, CAST, LIKE, GLOB, BETWEEN, IN, string concatenation (`||`)
- **DDL:** CREATE TABLE, CREATE INDEX, DROP TABLE/INDEX/VIEW, ALTER TABLE (ADD COLUMN, RENAME)
- **Transactions:** BEGIN, COMMIT, ROLLBACK with rollback journal; SAVEPOINT, RELEASE, ROLLBACK TO
- **Indexes:** B-tree indexes with equality and range scan optimization
- **Constraints:** NOT NULL, UNIQUE, CHECK, FOREIGN KEY enforcement; AUTOINCREMENT via sqlite_sequence
- **UPSERT:** INSERT ... ON CONFLICT DO UPDATE/NOTHING, INSERT OR REPLACE/IGNORE
- **PRAGMA:** table_info, table_list, index_list, index_info, page_size, page_count, integrity_check, foreign_keys, database_list, journal_mode
- **EXPLAIN QUERY PLAN:** human-readable query plan output
- **Triggers:** CREATE/DROP TRIGGER with BEFORE/AFTER timing, OLD/NEW row references, WHEN conditions
- **VACUUM:** rebuild database to reclaim unused space
- **ATTACH DATABASE / DETACH:** open and query multiple database files
- **WAL mode stub:** accepts `PRAGMA journal_mode = WAL` gracefully (operates in rollback journal mode)
- **Window functions:** ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER
- **Vector search:** `vec_distance_cosine`, `vec_distance_l2`, `vec_distance_dot`, `vec_from_json`, `vec_to_json`, `vec_normalize`, `vec_length`
- **JSON functions:** `json`, `json_extract`, `json_type`, `json_valid`, `json_array`, `json_object`, `json_array_length`, `json_insert`, `json_replace`, `json_set`, `json_remove`, `json_patch`, `json_quote`
- **Collation:** COLLATE NOCASE for case-insensitive comparisons and ordering
- **50+ scalar functions:** LENGTH, SUBSTR, UPPER, LOWER, TRIM, REPLACE, COALESCE, IFNULL, TYPEOF, HEX, ROUND, ABS, RANDOM, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, IIF, PRINTF, and more
- **Parameter binding:** `?` placeholders with bound values
- **Prepared statement cache:** LRU cache (64 entries) with DDL-triggered invalidation

## Quick Start — Rust

```rust
use rsqlite::vfs::memory::MemoryVfs;
use rsqlite::core::database::Database;

let vfs = MemoryVfs::new();
let mut db = Database::create(&vfs, "test.db").unwrap();

db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").unwrap();
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')").unwrap();

let result = db.query("SELECT * FROM users WHERE name = 'Alice'").unwrap();
for row in &result.rows {
    println!("{:?}", row);
}
```

## Quick Start — JavaScript (Browser)

```bash
npm install rsqlite-wasm
```

```typescript
import { WorkerDatabase } from 'rsqlite-wasm';

const db = await WorkerDatabase.open('myapp.db');

await db.exec(`
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    done INTEGER DEFAULT 0
  )
`);

await db.exec("INSERT INTO todos (title) VALUES ('Buy groceries')");

const rows = await db.query("SELECT * FROM todos WHERE done = 0");
console.log(rows);

db.close();
```

The WASM module runs inside a Web Worker. The `WorkerDatabase` class is a main-thread proxy that communicates via `postMessage`. OPFS is used for persistence when available, with IndexedDB as a fallback.

## DevTools (Brainwires OPFS extension)

The [Brainwires OPFS](https://github.com/Brainwires/opfs-extension) Chrome
DevTools extension auto-detects rsqlite-wasm databases stored in OPFS and gives
you a full SQLite IDE — schema browser, paginated table data, FK click-through,
saved queries, EXPLAIN — inside DevTools.

By default the panel loads a *snapshot* of the shards from disk (read-only
when the page holds the live `SyncAccessHandle` — OPFS sync handles are
exclusive). To get **fully live editing** through the panel — UPDATEs, DDL,
auto-refresh when your app writes — opt in with one line at startup.

> ⚠️ **Security:** the bridge is off by default and you must pass
> `enabled: true` to install it. Once enabled, **any script in the same origin
> can read and write the entire database** through the bridge global. Gate it on
> your dev build and never enable it in production. See
> [Security](#security).

```typescript
import { Database, exposeForDevtools } from "rsqlite-wasm";

const db = await Database.open("chat", { backend: "opfs" });

// Enable in development only — gate on your bundler's dev flag so the bridge
// tree-shakes out of production builds:
exposeForDevtools(db, { name: "chat", enabled: import.meta.env.DEV });
```

How it works:

- When `enabled`, installs `window.__BRAINWIRES_RSQLITE_DEVTOOLS__`, a tiny
  `globalThis` bridge that lets the panel route SQL through *your* `Database`
  instance — no second handle, no lock conflict. A `console.warn` is emitted
  when the bridge is installed, as a reminder it is active.
- Wraps `db.exec` / `db.execMany` so writes from your own code bump a
  `changeCounter`. The panel polls that counter to auto-refresh when your app
  changes data behind its back.
- Returns a `release()` function to unregister (handy for HMR / teardown).
- Omitting `enabled` (or `enabled: false`) is a no-op — the same source
  compiles out of production builds.
- Multiple databases can be exposed under different names. The panel shows
  one **● live (name)** badge per registered db.

When the bridge isn't installed, the panel still works — it reads the file
bytes directly and shows the schema + data. Edits attempted in that mode
surface a friendly "page is using this file, expose it for live editing"
message.

## Database size & sharding

Browsers cap individual storage files well below SQLite-class workloads (OPFS and IndexedDB both have per-file size limits, often ≤ 4 GB). To escape that cap, rsqlite-wasm transparently shards each logical database across multiple backing files via a `MultiplexVfs` layer.

A logical database `myapp.db` is stored on disk as `myapp.db.000`, `myapp.db.001`, `myapp.db.002`, … Each shard is capped at 1 GB by default. With the default 16-shard ceiling, a single database can grow to 16 GB without any application changes.

```typescript
// Default: 1 GB shards, 16 shards max → 16 GB ceiling.
const db = await WorkerDatabase.open('myapp.db');

// For larger databases, raise the ceiling at open time:
const big = await WorkerDatabase.open('huge.db', {
  chunkSize: 1024 * 1024 * 1024,  // 1 GB per shard
  maxShards: 64,                  // 64 GB total
});
```

Notes:

- **OPFS pre-registration.** OPFS only exposes asynchronous handle creation, but the engine reads and writes synchronously. To bridge the gap, rsqlite-wasm registers all `maxShards` handles at open time. Unused shards are zero-byte files and cost only a directory entry.
- **IndexedDB has no shard ceiling.** The IDB backend creates shards lazily, so `maxShards` is ignored there.
- **Backward compatibility.** A legacy non-sharded file (e.g. a database created by an older single-file VFS) is detected on open and treated as shard 0; growth past 1 GB writes new shards alongside it (`myapp.db`, `myapp.db.001`, `myapp.db.002`, …).
- **Exporting to vanilla `sqlite3`.** A sharded database is logically one file. To open it with the `sqlite3` CLI, concatenate the shards: `cat myapp.db.* > myapp.db && sqlite3 myapp.db`.

## Building from Source

### Prerequisites

- Rust 1.85+ (edition 2024)
- [wasm-pack](https://rustwasm.github.io/wasm-pack/) (for WASM builds)
- Node.js 18+ (for the JS wrapper)

### Native build

```bash
cargo build --release
```

### WASM build

```bash
wasm-pack build --target web --out-dir pkg crates/rsqlite-wasm
```

### JS wrapper

```bash
cd js
npm install
npm run build
```

## Running Tests

```bash
cargo test --workspace        # Rust workspace
cd js && npm test             # JS package (rebuilds the WASM glue first)
```

### Coverage

CI runs `cargo-llvm-cov` against the Rust workspace and fails if line
coverage drops below 75%. The wasm-bindgen-only `rsqlite-wasm` crate
is excluded from that gate because its tests run under
`wasm-pack test` (Node + headless Chrome) instead of native
`cargo test`. CI runs those separately in the `wasm-tests` job, so
both surfaces are exercised.

Reproduce locally with:

```bash
scripts/coverage.sh           # native Rust + JS + wasm-bindgen-test
scripts/coverage.sh rust      # gate-checked Rust llvm-cov
scripts/coverage.sh js        # JS coverage report (informational)
scripts/coverage.sh wasm      # wasm-pack test --node + --chrome
```

Requires:
- `cargo install cargo-llvm-cov`
- `rustup component add llvm-tools-preview`
- `wasm-pack` (and Chrome on PATH for `wasm` mode)

## Architecture

```
rsqlite-wasm/
  crates/
    rsqlite-parser/     SQL parsing (sqlparser-rs wrapper)
    rsqlite-vfs/        VFS trait + native file + memory backends
    rsqlite-storage/    B-tree, pager, SQLite file format codec
    rsqlite-core/       Query planner, executor, catalog, transactions
    rsqlite-wasm/       wasm-bindgen API, OPFS + IndexedDB backends
    rsqlite/            Public Rust facade (re-exports core + native VFS)
  js/                   TypeScript wrapper + Web Worker glue
  demo/                 Demo PWA (contacts CRUD + SQL console)
```

**Dependency graph:**

```
rsqlite (facade) --> rsqlite-core --> rsqlite-parser
                                  --> rsqlite-storage --> rsqlite-vfs

rsqlite-wasm ------> rsqlite-core
                  --> rsqlite-vfs (OPFS + IndexedDB backends, MultiplexVfs)
```

The `MultiplexVfs` layer sits between the engine and any concrete VFS backend; it presents one logical file backed by N capped-size physical files, so OPFS and IDB databases can scale past per-file size limits.

The core engine uses a **tree-walking interpreter** with a Volcano/iterator execution model. The query planner produces logical plans that the executor evaluates directly — no bytecode VM. This keeps the WASM binary small and the code easy to debug.

## Vector Search (Non-Standard Extension)

rsqlite-wasm includes built-in vector similarity search functions. These are **not part of the SQL standard or SQLite** — they are custom extensions inspired by [sqlite-vec](https://github.com/asg017/sqlite-vec) and similar projects.

### Storage format

Vectors are stored as plain BLOBs: `N` float32 values in **little-endian** byte order, giving `4 * N` bytes per vector. A 384-dimension embedding (typical for models like all-MiniLM-L6-v2) is a 1,536-byte BLOB.

```sql
CREATE TABLE embeddings (
  id INTEGER PRIMARY KEY,
  text TEXT,
  vector BLOB
);
```

### Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `vec_distance_cosine(a, b)` | `(BLOB, BLOB) -> REAL` | Cosine distance (1 - cosine similarity). 0 = identical. |
| `vec_distance_l2(a, b)` | `(BLOB, BLOB) -> REAL` | Euclidean (L2) distance. 0 = identical. |
| `vec_distance_dot(a, b)` | `(BLOB, BLOB) -> REAL` | Negative dot product. Lower = more similar. |
| `vec_length(a)` | `(BLOB) -> INTEGER` | Number of dimensions (`byte_length / 4`). |
| `vec_normalize(a)` | `(BLOB) -> BLOB` | Returns L2-normalized copy of the vector. |
| `vec_from_json(text)` | `(TEXT) -> BLOB` | Parses a JSON array like `[0.1, 0.2, ...]` into a vector BLOB. |
| `vec_to_json(blob)` | `(BLOB) -> TEXT` | Serializes a vector BLOB back to a JSON array. |

### Brute-force KNN

The simplest setup stores vectors in an ordinary column and scans them with the
distance functions above. This is exact (no recall loss) and suitable for
PWA-scale workloads — thousands to low tens-of-thousands of rows search in tens
of milliseconds, and any `WHERE` filter is applied before the distance is
evaluated.

```sql
-- Insert via JSON (or bind a BLOB parameter directly)
INSERT INTO embeddings VALUES (1, 'hello world', vec_from_json('[0.1, 0.2, 0.3, ...]'));

-- K-nearest-neighbor search
SELECT id, text, vec_distance_cosine(vector, vec_from_json(?)) AS distance
FROM embeddings
ORDER BY distance
LIMIT 10;
```

### Approximate search (`vec_index`)

For larger collections rsqlite-wasm ships an HNSW (Hierarchical Navigable Small
World) approximate-nearest-neighbor index as a virtual table. It gives
`O(log N)` expected query time instead of a full scan:

```sql
CREATE VIRTUAL TABLE embeds USING vec_index(dim=384, metric=cosine);
INSERT INTO embeds VALUES (vec_from_json('[0.1, 0.2, ...]'));   -- one BLOB column

-- The planner pushes `ORDER BY vec_distance_<metric>(col, ?) LIMIT k`
-- straight into the graph traversal, skipping the outer sort.
SELECT rowid FROM embeds
ORDER BY vec_distance_cosine(vector, ?)
LIMIT 10;
```

Arguments (passed at `CREATE` time):

| Arg | Default | Meaning |
|-----|---------|---------|
| `dim=N` | *(required)* | Vector dimension; inserts must decode to exactly `N` floats. |
| `metric=cosine\|l2\|dot` | `cosine` | Distance metric; must match the `vec_distance_*` used in `ORDER BY` for pushdown. |
| `m=N` | `16` | Max graph degree per layer above 0 (layer 0 caps at `2 * m`). |
| `ef_construction=N` | `200` | Candidate-pool size during inserts. Higher → better recall, slower build. |
| `ef=N` | `50` | Candidate-pool size at query time. Higher → better recall, slower query. |

### Persistence

- **Plain BLOB column → persistent.** Vectors stored in an ordinary table (the
  brute-force pattern above) are regular row data: they are written atomically
  with the rest of the database and survive reopen, export, and import like any
  other BLOB.
- **`vec_index` HNSW graph → in-memory (today).** The HNSW graph is currently
  held in memory and is not yet written to the backing file, so it must be
  repopulated by re-inserting rows after each open. On-disk persistence for the
  graph (mirroring the FTS5 shadow-table approach) is planned — see the
  [CHANGELOG](./CHANGELOG.md). Until it lands, **prefer the plain-BLOB-column +
  brute-force pattern when your vectors must survive reload** (e.g. an
  OPFS-backed store), and reserve `vec_index` for in-session acceleration.

### Embeddings / RAG example

A typical retrieval-augmented-generation store keeps chunks in a plain table so
the vectors persist, scoped to a conversation (or shared globally when
`conversation_id IS NULL`):

```sql
CREATE TABLE documents (
  id              INTEGER PRIMARY KEY,
  name            TEXT NOT NULL,
  conversation_id TEXT NULL              -- NULL = global, visible to every search
);
CREATE TABLE chunks (
  id          INTEGER PRIMARY KEY,
  document_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
  text        TEXT,
  vector      BLOB,                       -- f32 little-endian, 4 * dim bytes
  vector_dim  INTEGER
);

-- Top-K search scoped to one conversation plus any global docs:
SELECT chunks.id, chunks.text, documents.name,
       vec_distance_cosine(chunks.vector, vec_from_json(?)) AS distance
FROM chunks JOIN documents ON documents.id = chunks.document_id
WHERE chunks.vector_dim = ?
  AND ( ? IS NULL OR documents.conversation_id = ? OR documents.conversation_id IS NULL )
ORDER BY distance ASC
LIMIT ?;
```

From JavaScript, bind the query vector as a `Uint8Array` BLOB (it survives the
Web Worker boundary via structured clone) instead of round-tripping JSON:

```typescript
// f32 query vector → little-endian bytes
const queryBlob = new Uint8Array(new Float32Array(queryVec).buffer);
const rows = await db.query(
  "SELECT id, text, vec_distance_cosine(vector, ?) AS distance " +
  "FROM chunks ORDER BY distance LIMIT ?",
  [queryBlob, 5],
);
```

### Portability note

Vector BLOBs are ordinary SQLite BLOB values — they will survive export/import with the `sqlite3` CLI. However, the `vec_*` functions only exist in rsqlite-wasm, so queries that use them will not work in standard SQLite.

## Security

rsqlite-wasm runs entirely client-side — there is no server component and no
network I/O. A few things are worth knowing before you ship it:

- **DevTools bridge (`exposeForDevtools`).** Off by default. When you enable it
  (`enabled: true`), it installs a global on `window`/`globalThis` that lets the
  Brainwires OPFS panel run arbitrary SQL against your database. Because it is a
  same-realm global, **any script running in the same origin** — including
  third-party scripts and injected content — can read and write the whole
  database through it. Enable it only in development builds; never in production.
  A `console.warn` fires whenever the bridge is installed.
- **`toBuffer()` exports raw, unencrypted bytes.** The export is a plain SQLite
  file image with no encryption or integrity tag. If you transmit or persist it
  outside the browser's storage, securing that channel is the caller's
  responsibility.
- **Parameter binding.** Use `?` placeholders with bound values (`query`/`exec`
  take a params array) rather than string-concatenating user input into SQL.
- **SQL comes from your application.** The engine parses and executes whatever
  SQL you hand it; treat SQL text the same way you would treat code.

Found a security issue? Please report it via the
[issue tracker](https://github.com/Brainwires/rsqlite-wasm/issues).

## License

[MIT](LICENSE)
