# Changelog

## 0.1.0 — unreleased

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
- Vector search: `vec_distance_cosine`, `vec_distance_l2`, `vec_distance_dot`, `vec_from_json`, `vec_to_json`, `vec_normalize`, `vec_length`
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
