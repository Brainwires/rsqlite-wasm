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
  Supported as syntax when the LHS is a single identifier
  (`col`, `t.col`, `"quoted col"`) or a parenthesized expression
  (`(a + b) IS TRUE`); the parser pre-pass rewrites it into
  `(x IS NOT NULL AND x <> 0)` etc. before handing it to
  SQLiteDialect. The function-form fallbacks `is_true(...)` /
  `is_false(...)` / `is_not_true(...)` / `is_not_false(...)`
  remain available for shapes the rewriter can't match.
  Truthiness follows SQLite semantics: NULL is unknown;
  integer/real != 0 is true; text is true if it parses as a
  non-zero number.

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

- **Vector index `vec_index`** — typed vector storage shipped as a
  built-in virtual-table module (`CREATE VIRTUAL TABLE e USING
  vec_index(dim=N, metric=cosine|l2|dot)`). Inserts validate the
  vector dimension. Lookup is brute-force for v0.1 — the user
  composes a nearest-neighbor query as
  `SELECT rowid FROM e ORDER BY vec_distance_cosine(vector, ?)
  LIMIT k`. Swapping in a real HNSW graph behind the same API is
  a v0.2 perf optimization.
- **R-Tree `rtree`** — multi-dimensional bounding-box storage
  shipped as a built-in virtual-table module
  (`CREATE VIRTUAL TABLE r USING rtree(N)` for 1 ≤ N ≤ 5). Inserts
  validate `min ≤ max` per dimension. Overlap queries are
  brute-force in v0.1; a real R*-Tree (split heuristic, MBR
  cascade) is a v0.2 swap behind the same module API.
- **FTS5 `fts5`** — basic full-text search shipped as a built-in
  module (`CREATE VIRTUAL TABLE docs USING fts5(content)`). v0.1
  ships single-column tables, an ASCII-aware whitespace +
  punctuation tokenizer, and the scalar functions `fts5_match(col,
  'query')` (boolean: every query token present) and
  `fts5_rank(col, 'query')` (`matched_terms / total_query_terms`,
  use in `ORDER BY`). Lookup is brute force; the native `MATCH`
  operator and SQLite's `unicode61` Unicode tokenizer + inverted
  index + BM25 ranking are v0.2 follow-ups.
- **`LOAD_EXTENSION`** — not safe in WASM.
- **User-defined functions** from JavaScript — supported in the
  in-worker `Database` API via `db.createFunction(name, fn, opts)`. Not
  yet supported through the cross-thread `WorkerDatabase` proxy because
  callbacks can't be `postMessage`-serialized. Async UDFs are deferred
  to a future release.
- **`WITHOUT ROWID` tables** — syntax accepted; the catalog tracks
  the flag and the planner enforces that a PRIMARY KEY is declared.
  Query semantics match a regular rowid table because PK uniqueness
  is checked the same way (composite-PK as a tuple). The on-disk
  btree shape is still rowid-keyed, so a database created here with
  `WITHOUT ROWID` won't load that specific table in vanilla
  `sqlite3` — the rest of the file still does. Real
  composite-PK-as-btree-key storage is a v0.2 follow-up.

## Known follow-ups

These are tracked as v0.2 candidates:

1. sqlite_schema root-page split (btree restructure).
2. Partial-index implication beyond the verbatim-conjunct + literal
   range cases (e.g. semantic implication of two `IN` lists).
3. Real HNSW graph + R*-Tree split heuristic + FTS5 inverted index +
   BM25 (the brute-force `vec_index`, `rtree`, and `fts5` shipped
   today are API-shaped for the swap; multi-column FTS5 with
   per-column weights and the native `MATCH` operator also belong
   here).
4. WITHOUT ROWID storage rewrite — the syntax is accepted and PK
   uniqueness enforced today, but real composite-PK-as-btree-key
   storage (for SQLite file-format compat on those tables) is
   deferred to v0.2.
