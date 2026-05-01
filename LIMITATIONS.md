# rsqlite-wasm Limitations

rsqlite-wasm aims for SQLite compatibility but isn't a 1:1 port. This
document is the truth-source for what's deliberately deferred. Each entry
explains the gap, the workaround if any, and where to follow progress.

## Parser / dialect

Inherited from `sqlparser-rs` 0.55's `SQLiteDialect`:

- **Bitwise complement `~`.** Supported as syntax for simple operands
  via a parser pre-pass: `~col` and `~(expr)` rewrite to
  `__bnot(col)` / `__bnot((expr))` before sqlparser sees them. For
  numeric literals or complex prefixes that the rewriter doesn't
  match, use `__bnot(...)` directly.

- **Bitwise shift `<<` and `>>`.** Still parse errors at the operator
  syntax level — the `__shl(a, b)` / `__shr(a, b)` function forms
  cover the operations. AND (`&`) and OR (`|`) work as native
  operators since SQLiteDialect accepts them.

- **`x IS TRUE` / `x IS FALSE` / `x IS NOT TRUE` / `x IS NOT FALSE`.**
  The syntax form is supported when `x` is a single identifier
  (`col`, `t.col`, `"quoted col"`); the parser pre-pass rewrites it
  into `(x IS NOT NULL AND x <> 0)` etc. before handing it to
  SQLiteDialect. For arbitrary LHS expressions (`(a + b) IS TRUE`),
  use the function form `is_true(...)` / `is_false(...)` /
  `is_not_true(...)` / `is_not_false(...)` — semantics match SQLite's
  truthiness rule (NULL is unknown; integer/real != 0 is true; text
  is true if it parses as a non-zero number).

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
- **Expression indexes** (`CREATE INDEX ... ON t(lower(name))`) build
  with the expression evaluated against each row, and INSERT /
  UPDATE / DELETE keep the index in sync. The planner picks the
  index when a `<idx_expr> = <literal>` conjunct appears in the WHERE
  (e.g. `WHERE lower(name) = 'bob'`). Single-column expression
  indexes only for now; multi-column expression indexes still fall
  back to a full table scan.

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

## Known follow-ups

These are tracked as v0.2 candidates:

1. Multi-column expression-index lookup (single-column already works).
2. UPDATE LIMIT / ORDER BY (needs custom parser path).
3. sqlite_schema root-page split (btree restructure).
4. Native bitwise shift syntax (`<<`, `>>`) — currently only the
   `__shl`, `__shr` function forms work. Prefix `~` is already
   supported for simple operands.
5. `IS TRUE` / `IS FALSE` syntax with arbitrary expression LHS
   (single-identifier form already works via parser pre-pass).
6. Partial-index implication beyond the verbatim-conjunct case
   (e.g. tighter range proves looser range).
7. Cost-aware planner consuming the `sqlite_stat1` rows ANALYZE writes.
8. Virtual tables / FTS5 / R-Tree / HNSW vector index — major
   subsystems deferred to v0.2.
9. WITHOUT ROWID tables — storage layer rewrite deferred to v0.2.
