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

- **Bitwise shift `<<` and `>>`.** Supported as syntax for "safe"
  operands via a parser pre-pass: identifier (possibly qualified),
  parenthesized expression, integer literal, or function call all
  rewrite to `__shl(a, b)` / `__shr(a, b)`. Chains like
  `1 << 2 << 3` resolve left-to-right via repeated rewriting. The
  rewriter is deliberately narrow — it won't touch `a + b << c`
  (where SQL precedence intends `(a+b) << c`) because that would
  silently produce the wrong answer. For unsafe-shape cases, use
  `__shl(...)` / `__shr(...)` directly. AND (`&`) and OR (`|`)
  work as native operators since SQLiteDialect accepts them.

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
  index when each indexed column has a matching `<idx_expr> =
  <literal>` conjunct in the WHERE — single-column and multi-column
  forms both work (e.g. `WHERE lower(name) = 'bob' AND
  lower(email) = 'b@y.com'`).

## DML

- **`UPDATE ... LIMIT` / `UPDATE ... ORDER BY`.** Supported via a
  parser pre-pass that rewrites the statement into the rowid-IN
  form SQLite documents. Single-table UPDATE only —
  `UPDATE ... FROM` falls through and the user keeps writing the
  IN-form by hand. Works in tandem with the planner's
  Sort-before-Project shape so the ORDER BY column doesn't have to
  be in the SELECT projection.

## Maintenance

- **`REINDEX`** is accepted as a no-op. Our btree implementation
  doesn't suffer from the corruption modes (collation changes, etc.)
  that real SQLite uses REINDEX to recover from. Tools that issue
  REINDEX won't error, but no work is done.
- **`ANALYZE`** populates `sqlite_stat1` with one row per table (row
  count) and one row per index. Index stats are now real
  (`<row_count> <avg_per_first_col> <avg_per_first_two_cols> …`)
  computed from the actual btree contents, not placeholder `1`s.
  Schema matches SQLite's, so external tools that read
  `sqlite_stat1` work. The catalog loads the stats on open, and
  `try_index_scan` consults them to pick the most-selective
  candidate when multiple indexes match the same equality query.

## Not implemented at all

- **FTS5 / R-Tree / HNSW** — the virtual-table foundation
  (`crate::vtab`) is in place, including a built-in `series` /
  `generate_series` module and CREATE VIRTUAL TABLE wiring. The
  specific search modules are deferred to v0.2; third parties can
  register modules today via `vtab::register_module`.
- **`LOAD_EXTENSION`** — not safe in WASM.
- **User-defined functions** from JavaScript — supported in the
  in-worker `Database` API via `db.createFunction(name, fn, opts)`. Not
  yet supported through the cross-thread `WorkerDatabase` proxy because
  callbacks can't be `postMessage`-serialized. Async UDFs are deferred
  to a future release.
- **`WITHOUT ROWID` tables** — storage layer assumes rowid keys.

## Known follow-ups

These are tracked as v0.2 candidates:

1. sqlite_schema root-page split (btree restructure).
3. `IS TRUE` / `IS FALSE` syntax with arbitrary expression LHS
   (single-identifier form already works via parser pre-pass).
4. Partial-index implication beyond the verbatim-conjunct case
   (e.g. tighter range proves looser range).
5. FTS5 / R-Tree / HNSW vector index modules — the vtab foundation
   exists, but these specific modules are deferred to v0.2.
6. WITHOUT ROWID tables — storage layer rewrite deferred to v0.2.
