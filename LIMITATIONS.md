# rsqlite-wasm Limitations

This file lists the small set of features that don't behave identically
to vanilla SQLite. Most are dialect-level caveats with documented
workarounds; the only architecturally-blocked feature is
`LOAD_EXTENSION`.

## Parser / dialect

These are inherited from `sqlparser-rs`'s `SQLiteDialect` and worked
around with parser pre-passes. The pre-passes cover the common shapes
of each operator; the function-form fallback (`__shl`, `__bnot`,
`is_true`, etc.) is always available for any shape the rewriter
doesn't recognize.

- **Bitwise shift `<<` / `>>`.** Identifier, parenthesized expression,
  integer literal, and function-call operands work as expected. For
  shapes where SQL precedence with surrounding `+` / `-` / `*` matters
  (e.g. `a + b << c`), wrap the intended operand in parens
  (`(a + b) << c`) or use `__shl(a + b, c)` directly.
- **`x IS TRUE` / `x IS FALSE` / `x IS NOT TRUE` / `x IS NOT FALSE`.**
  Identifier and parenthesized-expression LHS forms work. For more
  elaborate LHS shapes, the function-form fallbacks `is_true(...)`,
  `is_false(...)`, etc. remain available. Truthiness follows SQLite
  semantics: NULL is unknown; integer/real != 0 is true; text is true
  if it parses as a non-zero number.
- **`UPDATE … FROM` with `LIMIT` / `ORDER BY`.** Plain UPDATE with
  LIMIT/ORDER BY works via a parser pre-pass that rewrites to the
  `WHERE rowid IN (SELECT rowid …)` shape SQLite documents. The same
  rewrite for the multi-table `UPDATE … FROM …` form isn't supplied;
  users wanting LIMIT/ORDER BY with FROM write the IN-form by hand.

## Not safe in WASM

- **`LOAD_EXTENSION`.** Loads native shared objects (`.so` / `.dll`),
  which can't run in a WebAssembly sandbox. Errors with
  `"LOAD_EXTENSION not supported on WASM"`. There is no workaround
  short of re-implementing the extension natively in Rust and
  registering it as a [user-defined function][udf] or a
  [virtual-table module][vtab].

[udf]: https://docs.rs/rsqlite-wasm/latest/rsqlite_wasm/struct.WasmDatabase.html#method.create_function
[vtab]: https://docs.rs/rsqlite-core/latest/rsqlite_core/vtab/

## Storage / engine

- **`WITHOUT ROWID` table: PRIMARY KEY projected through a secondary-index
  lookup.** On a `WITHOUT ROWID` table that also has a secondary index, a query
  resolved via that index returns correct row counts and correct values for the
  *indexed* column, but projecting the table's **primary-key column** through the
  index-backed lookup yields `0` instead of the real key — the secondary-index
  entry does not carry the `WITHOUT ROWID` primary key. A full table scan (or a
  query that doesn't resolve through the secondary index) returns correct data.
  Workaround: select the PK via a plan that scans the table, or add the PK
  columns to the index. (Tracked for a fix in a later release.)

## JS bindings

- **Async user-defined functions.** `db.createFunction(name, nArgs, fn)`
  registers a synchronous JS function. Async UDFs would need a
  `SharedArrayBuffer` + `Atomics.wait` round-trip (only available in
  `crossOriginIsolated` contexts), so they're not exposed as a default
  API. Workaround: pre-compute async work and bind the results as
  query parameters.
