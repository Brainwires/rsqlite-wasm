# Changelog

## 0.1.4

Real B-tree index seeks and in-place row deletion — turning PRIMARY KEY / UNIQUE
lookups and single-row deletes from O(rows) into O(log n). Before this, every
read materialized the whole b-tree and linear-filtered, and every delete rebuilt
the entire tree; a point lookup on a 5 000-row table took ~8 ms and scaled
linearly. Now both are flat and competitive with Deno KV. The whole family
(`rsqlite-parser`, `rsqlite-wasm-vfs`, `rsqlite-storage`, `rsqlite-core`,
`rsqlite-wasm`) moves to 0.1.4 together.

### New features

- **Index-cursor seeks** (`rsqlite-storage`): `IndexCursor::seek_at_or_after` /
  `seek_first_with_prefix` now descend the b-tree instead of scanning, and a new
  `btree_read_row_by_rowid` fetches a single table row by a rowid descent.
- **Implicit PK/UNIQUE indexes** (`rsqlite-core`): `CREATE TABLE` now emits
  SQLite-style `sqlite_autoindex_*` indexes for non-integer PRIMARY KEY and
  UNIQUE columns, so the planner turns `WHERE key = ?` into an index `SEARCH`.
  The SQL-less autoindex rows are re-derived from the table on load (matching
  SQLite's on-disk format — real `sqlite3` reads the files unchanged).
- **Seek-backed reads and constraint checks**: index equality/range scans, the
  UNIQUE-constraint check, and index-narrowed `DELETE` / `UPDATE` `WHERE col = ?`
  now seek rather than scan.
- **In-place deletes** (`rsqlite-storage`): `btree_delete` / `btree_delete_many`
  and the index deletes remove a single cell by rewriting only its leaf page
  (O(log n)) instead of rebuilding the whole tree. Trees may grow sparse
  (deletes don't rebalance), which real `sqlite3` `PRAGMA integrity_check`
  accepts as `ok`; `btree_max_rowid` is hardened against an emptied rightmost
  leaf so re-inserted rowids never collide.

## 0.1.3

Adds `PRAGMA synchronous`, letting callers trade fsync-per-commit durability for
write throughput. `rsqlite-wasm` bumps to 0.1.3; the supporting changes land in
`rsqlite-core` (pragma dispatch) and `rsqlite-storage` (the pager gate).

### New features

- **`PRAGMA synchronous = OFF | NORMAL | FULL`** (and the read form
  `PRAGMA synchronous`). `FULL` (default) fsyncs on every commit — unchanged
  behavior. `NORMAL` skips the per-commit fsync (keeping structural syncs); `OFF`
  never syncs. On the `node:fs` file backend this is a large write speedup (~50×
  for `NORMAL` in a commit-per-write loop) at the cost of losing the last few
  commits on a crash — appropriate for regenerable data such as a cache.

## 0.1.2

Adds a first-class server-side (Node.js / Deno) file-persistence backend. Only
the `rsqlite-wasm` crate changes in this release; the rest of the family stays
at 0.1.1.

### New features

- **Server-side file persistence (Node.js / Deno).** A new `node:fs`-backed VFS
  compiles into the `--target nodejs` build (`nodefs` cargo feature) and is
  exposed as `WasmDatabase.openWithFile(path)` / `Database.open(path, { backend:
  "file" })`. Databases persist to a real file on disk — no OPFS, no Web Worker,
  no `fetch` — and the file is a standard SQLite-3 database readable by the
  `sqlite3` CLI. The JS wrapper auto-detects Node/Deno and loads the nodejs
  build (which synchronously self-instantiates). Single-writer only for now (see
  follow-ups).

### Known follow-ups

- **Cross-process file locking for the `file` backend.** The `node:fs` VFS tracks
  locks advisorily only (no `flock`/`fcntl`), so the file backend is
  single-writer; multi-process access to one file is unsafe.

## 0.1.1

Correctness, security, and cleanup release: fixes data-loss/corruption bugs in
the storage engine and a parameter-binding bug, hardens the DevTools bridge, and
tidies packaging. Includes the LIMIT/OFFSET `?` placeholder feature.

### Bug fixes (correctness)

- **B-tree page splits no longer lose rows.** Table/index roots are now immutable
  (the tree deepens in place), so a split's new pages are reachable. Previously,
  inserting enough rows to split a page silently orphaned half the tree (e.g.
  400 rows of 300-byte text returned 8 rows).
- **Large values now use overflow pages.** Cell payloads larger than fit inline
  spill to a chained overflow-page list (SQLite format); reads reassemble them.
  Previously, big TEXT/BLOB values corrupted on read or panicked. Overflow pages
  are reclaimed via a new page freelist.
- **DELETE/UPDATE on large multi-level trees no longer corrupt** the database.
- **Anonymous `?` parameters are bound by SQL text order**, not planner
  traversal order. Previously a query with `?` spanning the SELECT list, WHERE,
  and LIMIT could bind parameters to the wrong positions.
- Round-trip integrity verified against the `sqlite3` CLI (`PRAGMA
  integrity_check`) after bulk insert/delete and overflow writes.

### Security

- **DevTools bridge is now off by default.** `exposeForDevtools(db, …)` no longer
  installs the bridge unless you pass `enabled: true`. Previously it was on
  whenever called and only suppressed by `disabled: true`. The bridge exposes a
  same-origin global that can read and write the whole database, so it must be
  opt-in and dev-only. **Breaking:** the `disabled` option is replaced by
  `enabled` (default `false`); update calls to
  `exposeForDevtools(db, { enabled: import.meta.env.DEV })`.
- A `console.warn` is now emitted whenever the bridge is installed.
- Bounded the bridge's in-memory results map so an un-polled caller can't grow
  it without limit.

### Documentation & packaging

- Added a **Security** section to the README and rewrote the DevTools section
  for the new `enabled` flag.
- Crates now ship the README to crates.io (`readme` metadata), and internal
  workspace dependencies carry explicit versions so `cargo publish` works.

### Internal

- Deduplicated the query→JS row-conversion paths in the wasm bindings.
- Substantially expanded test coverage across the engine (WITHOUT ROWID writes,
  virtual-table DML, datetime functions, planner/parser branches, B-tree page
  splits) and the JS wrapper/worker layer.

### Known follow-ups (deferred to a later release)

- Refactors flagged in design review: splitting `planner.rs` / `eval_helpers.rs`,
  scoping the executor's thread-local state to a query context, and adding
  explicit input-size limits (SQL length, parameter count).

## 0.1.0

Initial public release.

### SQL surface

- DML: SELECT, INSERT, UPDATE, DELETE with WHERE / ORDER BY / LIMIT / OFFSET
- Joins: INNER, LEFT, CROSS
- Aggregates: COUNT, SUM, AVG, MIN, MAX, TOTAL, GROUP_CONCAT (with DISTINCT and custom separator)
- Subqueries: IN, EXISTS, scalar
- Set operations: UNION, UNION ALL
- CTEs (`WITH`) including `WITH RECURSIVE`
- Views (CREATE / DROP / SELECT FROM)
- Expressions: CASE, CAST, LIKE, GLOB, BETWEEN, IN, `||` concat
- DDL: CREATE TABLE, CREATE INDEX, DROP TABLE/INDEX/VIEW, ALTER TABLE (ADD COLUMN, RENAME)
- Transactions: BEGIN, COMMIT, ROLLBACK; SAVEPOINT, RELEASE, ROLLBACK TO
- Constraints: NOT NULL, UNIQUE, CHECK, FOREIGN KEY (`ON DELETE` actions); AUTOINCREMENT
- UPSERT: `INSERT ... ON CONFLICT`, `INSERT OR REPLACE/IGNORE`
- PRAGMAs: table_info, table_list, index_list, index_info, page_size, page_count, integrity_check, foreign_keys, database_list, journal_mode (WAL accepted as no-op)
- EXPLAIN QUERY PLAN
- Triggers (BEFORE/AFTER, OLD/NEW, WHEN)
- VACUUM
- ATTACH / DETACH DATABASE
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, SUM/COUNT/AVG/MIN/MAX OVER
- JSON: `json`, `json_extract`, `json_type`, `json_valid`, `json_array`, `json_object`, `json_array_length`, `json_insert`, `json_replace`, `json_set`, `json_remove`, `json_patch`, `json_quote`
- Vector search: scalar functions `vec_distance_cosine`, `vec_distance_l2`, `vec_distance_dot`, `vec_from_json`, `vec_to_json`, `vec_normalize`, `vec_length` (little-endian f32 BLOBs) — usable as a brute-force KNN over a plain BLOB column; plus a `vec_index` virtual table backed by an HNSW approximate-nearest-neighbor graph (`dim`/`metric`/`m`/`ef`/`ef_construction` tunables) with planner pushdown of `ORDER BY vec_distance_<metric>(col, ?) LIMIT k`. The HNSW graph is currently in-memory only (rebuilt by re-inserting after open); on-disk persistence is planned.
- Collation: `COLLATE NOCASE`
- 50+ scalar functions (LENGTH, SUBSTR, UPPER, LOWER, TRIM, REPLACE, COALESCE, IFNULL, TYPEOF, HEX, ROUND, ABS, RANDOM, DATE, TIME, DATETIME, STRFTIME, JULIANDAY, UNIXEPOCH, IIF, PRINTF, …)
- Parameter binding via `?` placeholders
- LRU prepared statement cache (64 entries)

### Storage and persistence

- File-format compatible with SQLite 3 (open with `sqlite3` CLI)
- B-tree pager with rollback journal
- OPFS backend (primary) and IndexedDB fallback in browsers
- **Multi-file sharding (`MultiplexVfs`).** Logical databases are transparently spread across capped-size shard files (default 1 GB, configurable per-open). Default 16-shard ceiling for OPFS gives 16 GB per logical database; IDB has no ceiling. Existing single-file databases stay readable and grow into multi-shard form on first overflow write.
- LRU prepared statement cache, DDL-triggered invalidation
- ~2 MB WASM binary (LTO + `opt-level=z`)

### JavaScript wrapper

- `WorkerDatabase` main-thread proxy talking to a Web Worker
- `Database` synchronous API (in-worker only)
- `chunkSize` and `maxShards` open options for the multi-file VFS

### Known limitations

See [LIMITATIONS.md](./LIMITATIONS.md) for the full list of deferred features.
