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
- **315+ tests** — comprehensive coverage across all crates

## SQL Support

### Fully supported

- **DML:** SELECT, INSERT, UPDATE, DELETE with full WHERE/ORDER BY/LIMIT/OFFSET
- **Joins:** INNER JOIN, LEFT JOIN, CROSS JOIN
- **Aggregates:** COUNT, SUM, AVG, MIN, MAX, TOTAL, GROUP_CONCAT (with DISTINCT, custom separator)
- **Subqueries:** IN, EXISTS, scalar subqueries
- **Set operations:** UNION, UNION ALL
- **CTEs:** WITH ... AS (multiple, column renaming)
- **Views:** CREATE VIEW, DROP VIEW, SELECT from views
- **Expressions:** CASE, CAST, LIKE, GLOB, BETWEEN, IN, string concatenation (`||`)
- **DDL:** CREATE TABLE, CREATE INDEX, DROP TABLE/INDEX/VIEW, ALTER TABLE (ADD COLUMN, RENAME)
- **Transactions:** BEGIN, COMMIT, ROLLBACK with rollback journal; SAVEPOINT, RELEASE, ROLLBACK TO
- **Indexes:** B-tree indexes with equality and range scan optimization
- **Constraints:** NOT NULL, UNIQUE, CHECK enforcement; AUTOINCREMENT via sqlite_sequence
- **UPSERT:** INSERT ... ON CONFLICT DO UPDATE/NOTHING, INSERT OR REPLACE/IGNORE
- **PRAGMA:** table_info, table_list, index_list, index_info, page_size, page_count, integrity_check
- **EXPLAIN QUERY PLAN:** human-readable query plan output
- **Window functions:** ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER
- **Vector search:** `vec_distance_cosine`, `vec_distance_l2`, `vec_distance_dot`, `vec_from_json`, `vec_to_json`, `vec_normalize`, `vec_length`
- **JSON functions:** `json`, `json_extract`, `json_type`, `json_valid`, `json_array`, `json_object`, `json_array_length`, `json_insert`, `json_replace`, `json_set`, `json_remove`, `json_patch`, `json_quote`
- **Collation:** COLLATE NOCASE for case-insensitive comparisons and ordering
- **50+ scalar functions:** LENGTH, SUBSTR, UPPER, LOWER, TRIM, REPLACE, COALESCE, IFNULL, TYPEOF, HEX, ROUND, ABS, RANDOM, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, IIF, PRINTF, and more
- **Parameter binding:** `?` placeholders with bound values
- **Prepared statement cache:** LRU cache (64 entries) with DDL-triggered invalidation

### Not yet supported

- Recursive CTEs (WITH RECURSIVE)
- Triggers
- FOREIGN KEY constraint enforcement
- WAL mode
- VACUUM, ATTACH DATABASE

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

## License

[MIT](LICENSE)
