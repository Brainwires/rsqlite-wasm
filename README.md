# rsqlite-wasm

A pure Rust SQLite-compatible database engine that compiles to WebAssembly for use in browser Progressive Web Apps.

Databases created by rsqlite-wasm are **file-format compatible** with SQLite — you can open them with the `sqlite3` CLI and vice versa. This enables importing and exporting real `.sqlite` files in the browser.

## Features

- **Pure Rust** — zero C dependencies, builds cleanly for `wasm32-unknown-unknown`
- **SQLite file format** — binary-compatible with SQLite 3 databases
- **Browser persistence** — OPFS (primary) and IndexedDB (fallback) backends
- **Web Worker architecture** — all I/O runs off the main thread
- **Vector search** — built-in `vec_distance_cosine`, `vec_distance_l2`, and `vec_distance_dot` functions for embedding similarity search
- **Small binary** — ~1.6 MB WASM with LTO + `opt-level=z`
- **324+ tests** — comprehensive coverage across all crates

## SQL Support

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
cargo test --workspace
```

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
                  --> rsqlite-vfs (OPFS + IndexedDB backends)
```

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

### KNN query pattern

Search is brute-force (no approximate nearest neighbor index). This is suitable for PWA-scale workloads — thousands to low tens-of-thousands of rows.

```sql
-- Insert via JSON (or bind a BLOB parameter directly)
INSERT INTO embeddings VALUES (1, 'hello world', vec_from_json('[0.1, 0.2, 0.3, ...]'));

-- K-nearest-neighbor search
SELECT id, text, vec_distance_cosine(vector, vec_from_json(?)) AS distance
FROM embeddings
ORDER BY distance
LIMIT 10;
```

### Portability note

Vector BLOBs are ordinary SQLite BLOB values — they will survive export/import with the `sqlite3` CLI. However, the `vec_*` functions only exist in rsqlite-wasm, so queries that use them will not work in standard SQLite.

## License

[MIT](LICENSE)
