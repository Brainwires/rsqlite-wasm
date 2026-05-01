# rsqlite-wasm Limitations

rsqlite-wasm aims for SQLite compatibility but isn't a 1:1 port. This
document is the truth-source for what's deliberately deferred. Each entry
explains the gap, the workaround if any, and where to follow progress.

## Parser / dialect

Inherited from `sqlparser-rs` 0.55's `SQLiteDialect`:

- **Bitwise shift `<<` and `>>`, complement `~`.** Parse error at the
  operator syntax level. Use the function equivalents instead:
  - `__shl(a, b)` → `a << b`
  - `__shr(a, b)` → `a >> b`
  - `__bnot(a)` → `~a`

  Both AND (`&`) and OR (`|`) work as native operators since SQLiteDialect
  accepts them. A future release will add a custom dialect with
  `parse_infix` to accept the missing operator tokens directly.

- **`x IS TRUE` / `x IS FALSE` / `x IS NOT TRUE` / `x IS NOT FALSE`.**
  Not parseable as syntax. Use the function equivalents:
  - `is_true(x)` — 1 when `x` is non-null and truthy, else 0
  - `is_false(x)` — 1 when `x` is non-null and falsy, else 0
  - `is_not_true(x)` — 1 when `x` is null or falsy
  - `is_not_false(x)` — 1 when `x` is null or truthy

  Truthiness follows SQLite semantics: NULL is unknown; integer/real != 0
  is true; text is true if it parses as a non-zero number.

## Schema

- **VIRTUAL generated columns** are computed at write time and stored
  on disk, identical to `STORED`. SQLite's read-time semantics for
  VIRTUAL aren't yet implemented. Functionally correct, slightly
  larger storage footprint.
- **Bare `rowid` references on tables without `INTEGER PRIMARY KEY`.**
  Now supported — the executor threads each row's btree rowid through
  to `eval_expr`, so `SELECT rowid FROM t` and `WHERE rowid = ?` work
  even when the table has no rowid alias. Computed result rows
  (aggregates, projections, joins) leave `rowid` unset, so referencing
  `rowid` on those still errors.
- **`sqlite_schema` root-page split.** Schemas with very many CREATE
  TABLE / INDEX statements (large enough that the schema metadata
  itself outgrows page 1) error during DDL. Workaround: keep the
  schema modest, or split across multiple attached databases.

## Indexes

- **Partial indexes** (`CREATE INDEX ... WHERE ...`) build correctly,
  are maintained on INSERT/UPDATE, and are picked at query lookup time
  when the query's WHERE clause has the index's WHERE predicate as a
  top-level conjunct (the conservative case). More elaborate
  predicate-implication shapes still fall back to full scan; correctness
  is preserved either way.
- **Expression indexes** (`CREATE INDEX ... ON t(lower(name))`) now
  build with the expression evaluated against each row, and INSERT /
  UPDATE / DELETE keep the index in sync. The planner doesn't yet
  recognize `expr(col) = literal` patterns at lookup time, so reads
  still go through a full table scan — the index exists, it just
  isn't picked yet. Correctness is preserved.

## DML

- **`UPDATE ... LIMIT` / `UPDATE ... ORDER BY`** is not supported.
  `DELETE` supports both. SQLite gates these behind a compile-time
  flag (`SQLITE_ENABLE_UPDATE_DELETE_LIMIT`) and `sqlparser` doesn't
  expose them under SQLiteDialect. Workaround: rewrite as
  `UPDATE ... WHERE rowid IN (SELECT rowid FROM ... LIMIT N)`.

## Maintenance

- **`REINDEX`** is accepted as a no-op. Our btree implementation
  doesn't suffer from the corruption modes (collation changes, etc.)
  that real SQLite uses REINDEX to recover from. Tools that issue
  REINDEX won't error, but no work is done.
- **`ANALYZE`** populates `sqlite_stat1` with one row per table (row
  count) and one row per index (`<row_count> 1` placeholder for the
  per-distinct-prefix average). The schema matches SQLite's, so external
  tools that read `sqlite_stat1` work. The planner itself is still
  rule-based and doesn't yet consume the stats — that's tracked under
  cost-aware planning.

## Not implemented at all

- **Virtual tables / FTS / R-Tree** — out of scope.
- **`LOAD_EXTENSION`** — not safe in WASM.
- **User-defined functions** from JavaScript — supported in the
  in-worker `Database` API via `db.createFunction(name, fn, opts)`. Not
  yet supported through the cross-thread `WorkerDatabase` proxy because
  callbacks can't be `postMessage`-serialized. Async UDFs are deferred
  to a future release.
- **`WITHOUT ROWID` tables** — storage layer assumes rowid keys.
- **Covering / index-only scans** — performance optimization, not
  spec compliance.

## Known follow-ups

These are tracked as v0.2 candidates:

1. Expression-index *lookup-time* use (planner pattern matching of
   `expr(col)` against indexed expressions; build side already works)
2. UPDATE LIMIT / ORDER BY (needs custom parser path)
3. Bare `rowid` on tables without an alias (synthetic column or Row.rowid)
4. sqlite_schema root-page split (btree restructure)
5. Native bitwise operator syntax (`<<`, `>>`, `~`) — currently only the
   `__shl`, `__shr`, `__bnot` function forms work.
6. `IS TRUE` / `IS FALSE` as syntax — currently only the `is_true()`,
   `is_false()`, `is_not_true()`, `is_not_false()` function forms work.
7. Partial-index implication beyond the verbatim-conjunct case
   (e.g. tighter range proves looser range).
8. Cost-aware planner consuming the `sqlite_stat1` rows ANALYZE writes.
9. Virtual tables / FTS5 / R-Tree / HNSW vector index — major
   subsystems deferred to v0.2.
10. WITHOUT ROWID tables — storage layer rewrite deferred to v0.2.
