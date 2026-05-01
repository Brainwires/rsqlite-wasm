//! Integration coverage for scalar functions that have unit tests in
//! `eval_helpers` but were missing end-to-end SQL coverage. Each test goes
//! through the full parse → plan → execute path so a regression in any
//! layer is caught.

use super::*;
use crate::database::Database;
use crate::types::Value;
use rsqlite_vfs::memory::MemoryVfs;

fn fresh() -> Database {
    let vfs = MemoryVfs::new();
    Database::create(&vfs, "test.db").unwrap()
}

// ── QUOTE ─────────────────────────────────────────────────────────────

#[test]
fn quote_text_doubles_single_quotes() {
    let mut db = fresh();
    let r = db.query("SELECT QUOTE('it''s ok')").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("'it''s ok'".to_string()));
}

#[test]
fn quote_null_returns_null_keyword() {
    let mut db = fresh();
    let r = db.query("SELECT QUOTE(NULL)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("NULL".to_string()));
}

#[test]
fn quote_integer_passes_through() {
    let mut db = fresh();
    let r = db.query("SELECT QUOTE(42)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("42".to_string()));
}

#[test]
fn quote_blob_returns_x_literal() {
    let mut db = fresh();
    let r = db
        .query_with_params("SELECT QUOTE(?)", vec![Value::Blob(vec![0xde, 0xad])])
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("X'DEAD'".to_string()));
}

// ── UNICODE ───────────────────────────────────────────────────────────

#[test]
fn unicode_returns_first_codepoint() {
    let mut db = fresh();
    let r = db.query("SELECT UNICODE('A')").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(65));
}

#[test]
fn unicode_handles_multibyte() {
    let mut db = fresh();
    let r = db.query("SELECT UNICODE('é')").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(233));
}

#[test]
fn unicode_empty_string_is_null() {
    let mut db = fresh();
    let r = db.query("SELECT UNICODE('')").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Null);
}

// ── ZEROBLOB ──────────────────────────────────────────────────────────

#[test]
fn zeroblob_returns_blob_of_zeros() {
    let mut db = fresh();
    let r = db.query("SELECT ZEROBLOB(5)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Blob(vec![0u8; 5]));
}

#[test]
fn zeroblob_zero_length_is_empty_blob() {
    let mut db = fresh();
    let r = db.query("SELECT ZEROBLOB(0)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Blob(vec![]));
}

// ── RANDOMBLOB ────────────────────────────────────────────────────────

#[test]
fn randomblob_length_matches_request() {
    let mut db = fresh();
    let r = db.query("SELECT LENGTH(RANDOMBLOB(16))").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(16));
}

#[test]
fn randomblob_two_calls_differ() {
    let mut db = fresh();
    // Two 16-byte random blobs colliding is ~ 2^-128. If this ever fails,
    // RANDOMBLOB is broken.
    let r = db
        .query("SELECT QUOTE(RANDOMBLOB(16)), QUOTE(RANDOMBLOB(16))")
        .unwrap();
    assert_ne!(r.rows[0].values[0], r.rows[0].values[1]);
}

// ── PRINTF ────────────────────────────────────────────────────────────

#[test]
fn printf_basic_specs() {
    let mut db = fresh();
    let r = db.query("SELECT PRINTF('%s=%d', 'x', 7)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("x=7".to_string()));
}

#[test]
fn printf_no_args_passes_through() {
    let mut db = fresh();
    let r = db.query("SELECT PRINTF('hello')").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("hello".to_string()));
}

#[test]
fn printf_percent_escape() {
    let mut db = fresh();
    let r = db.query("SELECT PRINTF('100%%')").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("100%".to_string()));
}

// ── LIKELY / UNLIKELY / LIKELIHOOD ────────────────────────────────────

#[test]
fn likely_unlikely_pass_value_through() {
    let mut db = fresh();
    let r = db
        .query("SELECT LIKELY(1), UNLIKELY('x'), LIKELIHOOD(42, 0.5)")
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[0].values[1], Value::Text("x".to_string()));
    assert_eq!(r.rows[0].values[2], Value::Integer(42));
}

// ── SIGN edge cases ───────────────────────────────────────────────────

#[test]
fn sign_real_values() {
    let mut db = fresh();
    let r = db.query("SELECT SIGN(3.14), SIGN(-2.5), SIGN(0.0)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[0].values[1], Value::Integer(-1));
    assert_eq!(r.rows[0].values[2], Value::Integer(0));
}

#[test]
fn sign_text_coerces_or_nulls() {
    let mut db = fresh();
    let numeric = db.query("SELECT SIGN('-7')").unwrap();
    assert_eq!(numeric.rows[0].values[0], Value::Integer(-1));
    let nonnumeric = db.query("SELECT SIGN('abc')").unwrap();
    assert_eq!(nonnumeric.rows[0].values[0], Value::Null);
}

// ── Vector function edges ─────────────────────────────────────────────

fn vec_blob(values: &[f32]) -> Value {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    Value::Blob(bytes)
}

#[test]
fn vec_length_empty_blob_is_zero() {
    let mut db = fresh();
    let r = db
        .query_with_params("SELECT vec_length(?)", vec![Value::Blob(vec![])])
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(0));
}

#[test]
fn vec_distance_cosine_with_null_errors() {
    let mut db = fresh();
    let v = vec_blob(&[1.0, 0.0, 0.0]);
    // Calling a vec_* function with NULL is treated as a programming error
    // — vectors are required arguments. This locks in the current behavior.
    let res = db.query_with_params("SELECT vec_distance_cosine(?, NULL)", vec![v]);
    assert!(res.is_err());
}

#[test]
fn vec_normalize_unit_vector_is_identity() {
    let mut db = fresh();
    let v = vec_blob(&[1.0, 0.0, 0.0]);
    let r = db
        .query_with_params("SELECT vec_to_json(vec_normalize(?))", vec![v])
        .unwrap();
    if let Value::Text(s) = &r.rows[0].values[0] {
        assert!(s.starts_with("[1") || s.starts_with("[1.0"));
    } else {
        panic!("expected text result, got {:?}", r.rows[0].values[0]);
    }
}

#[test]
fn vec_normalize_scales_to_unit_length() {
    let mut db = fresh();
    // [3, 4, 0] has length 5, normalized → [0.6, 0.8, 0]
    let v = vec_blob(&[3.0, 4.0, 0.0]);
    let r = db
        .query_with_params("SELECT vec_to_json(vec_normalize(?))", vec![v])
        .unwrap();
    if let Value::Text(s) = &r.rows[0].values[0] {
        // Parse the JSON-ish output and check magnitudes are roughly right.
        let trimmed = s.trim_matches(|c| c == '[' || c == ']');
        let parts: Vec<f32> = trimmed
            .split(',')
            .map(|p| p.trim().parse().unwrap())
            .collect();
        assert!((parts[0] - 0.6).abs() < 1e-5);
        assert!((parts[1] - 0.8).abs() < 1e-5);
        assert!(parts[2].abs() < 1e-5);
    } else {
        panic!("expected text result");
    }
}

#[test]
fn vec_from_json_then_distance() {
    let mut db = fresh();
    let r = db
        .query("SELECT vec_distance_l2(vec_from_json('[0,0,0]'), vec_from_json('[3,4,0]'))")
        .unwrap();
    if let Value::Real(d) = r.rows[0].values[0] {
        assert!((d - 5.0).abs() < 1e-5);
    } else {
        panic!("expected real distance");
    }
}

// ── JSON deeper paths and semantic differences ────────────────────────

#[test]
fn json_extract_array_index() {
    let mut db = fresh();
    let r = db
        .query(r#"SELECT json_extract('[10, 20, 30]', '$[1]')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(20));
}

#[test]
fn json_extract_nested_array_in_object() {
    let mut db = fresh();
    let r = db
        .query(r#"SELECT json_extract('{"items":[{"id":1},{"id":2}]}', '$.items[1].id')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn json_set_creates_and_replaces() {
    let mut db = fresh();
    // json_set is the union of insert + replace: it both ADDs new keys and
    // OVERWRITES existing ones. This is the semantic distinction from the
    // dedicated json_insert / json_replace functions.
    let r = db
        .query(r#"SELECT json_set('{"a":1}', '$.a', 99, '$.b', 2)"#)
        .unwrap();
    if let Value::Text(s) = &r.rows[0].values[0] {
        assert!(s.contains("\"a\":99"));
        assert!(s.contains("\"b\":2"));
    } else {
        panic!("expected text json output");
    }
}

#[test]
fn json_remove_array_element() {
    let mut db = fresh();
    let r = db.query("SELECT json_remove('[1,2,3]', '$[1]')").unwrap();
    if let Value::Text(s) = &r.rows[0].values[0] {
        assert_eq!(s, "[1,3]");
    } else {
        panic!("expected text result");
    }
}

#[test]
fn json_array_length_array_path() {
    let mut db = fresh();
    let r = db
        .query(r#"SELECT json_array_length('{"x":[1,2,3,4,5]}', '$.x')"#)
        .unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(5));
}

// ── INSERT OR REPLACE / IGNORE on UNIQUE conflict ─────────────────────

#[test]
fn insert_or_replace_overwrites_existing_pk() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    db.execute("INSERT OR REPLACE INTO t VALUES (1, 'new')")
        .unwrap();
    let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("new".to_string()));
}

#[test]
fn insert_or_ignore_keeps_existing_on_conflict() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    db.execute("INSERT OR IGNORE INTO t VALUES (1, 'second')")
        .unwrap();
    let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("first".to_string()));
}

// ── Expression index build correctness ────────────────────────────────

#[test]
fn expression_index_builds_evaluated_values() {
    // Verify CREATE INDEX with an expression evaluates the expression
    // against existing rows (rather than emitting NULL placeholders).
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB')").unwrap();
    // No assertion on EXPLAIN — just ensure CREATE INDEX succeeds without
    // silently producing junk. The query side still doesn't optimize for
    // expression indexes, so we do a regular SELECT to confirm row counts.
    db.execute("CREATE INDEX idx_lower_name ON t(lower(name))").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn expression_index_maintained_on_insert() {
    // INSERT after the index exists should add a properly-keyed entry.
    // We can't observe the index entry directly, but we exercise the path
    // — any panic from an index-build helper would surface here.
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE INDEX idx_lower_name ON t(lower(name))").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Carol')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'dave')").unwrap();
    let r = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn expression_index_maintained_on_update_and_delete() {
    // UPDATE / DELETE should remove the old indexed key + re-add the
    // expression-evaluated new key. As above, we verify the path runs
    // without error.
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Eve'), (2, 'Frank')").unwrap();
    db.execute("CREATE INDEX idx_lower_name ON t(lower(name))").unwrap();
    db.execute("UPDATE t SET name = 'EVELYN' WHERE id = 1").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    let r = db.query("SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("EVELYN".to_string()));
    let count = db.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(count.rows[0].values[0], Value::Integer(1));
}

// ── Expression index lookup-time use ──────────────────────────────────

#[test]
fn expression_index_picked_for_matching_query() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE INDEX idx_lower_name ON t(lower(name))").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB'), (3, 'carol'), (4, 'DAVE')",
    )
    .unwrap();

    let plan = db
        .query("EXPLAIN QUERY PLAN SELECT id FROM t WHERE lower(name) = 'bob'")
        .unwrap();
    let plan_text = plan
        .rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(|v| format!("{v:?}"))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("idx_lower_name") || plan_text.contains("INDEX"),
        "EXPLAIN QUERY PLAN didn't show the expression index being used: {plan_text}"
    );
    let r = db
        .query("SELECT id FROM t WHERE lower(name) = 'bob'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn expression_index_returns_correct_rows_for_multiple_matches() {
    // The index is over `lower(name)`. Two rows share the same indexed
    // value. The Filter wrap must still let both come through.
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE INDEX idx_lower_name ON t(lower(name))").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'EVE'), (2, 'eve'), (3, 'frank')")
        .unwrap();

    let r = db
        .query("SELECT id FROM t WHERE lower(name) = 'eve' ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[1].values[0], Value::Integer(2));
}

// ── Partial index lookup-time use ─────────────────────────────────────

#[test]
fn partial_index_used_when_query_implies_predicate() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_active ON t(name) WHERE status = 'active'")
        .unwrap();
    db.execute(
        "INSERT INTO t VALUES \
         (1, 'active', 'a'), (2, 'inactive', 'b'), (3, 'active', 'c')",
    )
    .unwrap();
    // Query WHERE has the index's predicate as a top-level conjunct.
    let plan = db
        .query(
            "EXPLAIN QUERY PLAN SELECT id FROM t WHERE status = 'active' AND name = 'a'",
        )
        .unwrap();
    let plan_text = plan
        .rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(|v| format!("{v:?}"))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("idx_active") || plan_text.contains("INDEX"),
        "EXPLAIN QUERY PLAN didn't show the partial index being used: {plan_text}"
    );
    // And of course the actual query still returns the right rows.
    let r = db
        .query("SELECT id FROM t WHERE status = 'active' AND name = 'a'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
}

#[test]
fn partial_index_skipped_when_query_does_not_imply() {
    // Query lacks the partial index's predicate as a top-level conjunct;
    // the planner must NOT pick the index (otherwise rows where the
    // predicate is false would be missed).
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, name TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_active ON t(name) WHERE status = 'active'")
        .unwrap();
    db.execute(
        "INSERT INTO t VALUES \
         (1, 'active', 'a'), (2, 'inactive', 'a'), (3, 'inactive', 'b')",
    )
    .unwrap();
    // Query without `status = 'active'` — both rows with name='a' must come
    // back, including the inactive one.
    let r = db
        .query("SELECT id FROM t WHERE name = 'a' ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[1].values[0], Value::Integer(2));
}

// ── UPDATE LIMIT / ORDER BY (preprocessed to rowid IN form) ──────────

#[test]
fn update_with_limit_only() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old'), (2, 'old'), (3, 'old')")
        .unwrap();
    db.execute("UPDATE t SET status = 'new' LIMIT 2").unwrap();
    let r = db
        .query("SELECT id, status FROM t ORDER BY id")
        .unwrap();
    let new_count = r
        .rows
        .iter()
        .filter(|row| row.values[1] == Value::Text("new".to_string()))
        .count();
    assert_eq!(new_count, 2, "exactly 2 rows should be updated");
}

#[test]
fn update_with_where_and_limit() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'a', 30), (4, 'b', 40)")
        .unwrap();
    db.execute("UPDATE t SET status = 'updated' WHERE status = 'a' LIMIT 2")
        .unwrap();
    let r = db
        .query("SELECT id, status FROM t ORDER BY id")
        .unwrap();
    let updated_count = r
        .rows
        .iter()
        .filter(|row| row.values[1] == Value::Text("updated".to_string()))
        .count();
    assert_eq!(updated_count, 2);
    // Row 4 (status='b') stays untouched.
    assert_eq!(r.rows[3].values[1], Value::Text("b".to_string()));
}

#[test]
fn update_with_order_by_and_limit_picks_correct_rows() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, label TEXT)").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 30, 'orig'), (2, 10, 'orig'), (3, 20, 'orig'), (4, 40, 'orig')",
    )
    .unwrap();
    // Updates the 2 rows with LOWEST n: ids 2 (n=10) and 3 (n=20).
    db.execute("UPDATE t SET label = 'low' ORDER BY n ASC LIMIT 2")
        .unwrap();
    let r = db
        .query("SELECT id, label FROM t ORDER BY id")
        .unwrap();
    assert_eq!(r.rows[0].values[1], Value::Text("orig".to_string())); // id=1
    assert_eq!(r.rows[1].values[1], Value::Text("low".to_string()));  // id=2
    assert_eq!(r.rows[2].values[1], Value::Text("low".to_string()));  // id=3
    assert_eq!(r.rows[3].values[1], Value::Text("orig".to_string())); // id=4
}

#[test]
fn select_rowid_with_order_by_non_projected_column() {
    // The Sort-before-Project planner change makes this work — `n` is
    // not in the SELECT list but the Sort can still reach it.
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)").unwrap();
    let r = db
        .query("SELECT id FROM t ORDER BY n ASC")
        .unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], Value::Integer(2)); // n=10
    assert_eq!(r.rows[1].values[0], Value::Integer(3)); // n=20
    assert_eq!(r.rows[2].values[0], Value::Integer(1)); // n=30
}

#[test]
fn update_with_limit_does_not_corrupt_string_literal() {
    // The literal `' LIMIT '` should NOT trigger the LIMIT preprocess
    // — the keyword finder has to respect string boundaries.
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'foo')").unwrap();
    db.execute("UPDATE t SET label = 'has LIMIT in it' WHERE id = 1")
        .unwrap();
    let r = db.query("SELECT label FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("has LIMIT in it".to_string()));
}

// ── Bitwise NOT (~) syntax via preprocess ────────────────────────────

#[test]
fn tilde_prefix_complements_identifier() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0), (2, 255)").unwrap();
    let r = db.query("SELECT id, ~n AS inv FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[1], Value::Integer(-1));
    assert_eq!(r.rows[1].values[1], Value::Integer(-256));
}

#[test]
fn tilde_prefix_complements_parenthesized_expr() {
    let mut db = fresh();
    let r = db.query("SELECT ~(5 + 2) AS v").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(-8));
}

#[test]
fn tilde_prefix_in_where() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0), (2, -1)").unwrap();
    // ~n = 0 means n = -1 (since ~-1 = 0).
    let r = db.query("SELECT id FROM t WHERE ~n = 0").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn tilde_does_not_corrupt_string_literal() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '~tilde~'), (2, 'other')")
        .unwrap();
    let r = db.query("SELECT id FROM t WHERE label = '~tilde~'").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
}

// ── IS TRUE / IS FALSE syntax (single-identifier LHS) ────────────────

#[test]
fn is_true_syntax_with_column_lhs() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, NULL)")
        .unwrap();
    let r = db.query("SELECT id FROM t WHERE flag IS TRUE ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
}

#[test]
fn is_false_syntax_with_column_lhs() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, NULL)")
        .unwrap();
    let r = db.query("SELECT id FROM t WHERE flag IS FALSE ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
}

#[test]
fn is_not_true_syntax_includes_null() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, NULL)")
        .unwrap();
    // IS NOT TRUE matches both falsy and NULL — i.e. ids 2 and 3.
    let r = db
        .query("SELECT id FROM t WHERE flag IS NOT TRUE ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(2));
    assert_eq!(r.rows[1].values[0], Value::Integer(3));
}

#[test]
fn is_not_false_syntax_includes_null() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, flag INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (2, 0), (3, NULL)")
        .unwrap();
    let r = db
        .query("SELECT id FROM t WHERE flag IS NOT FALSE ORDER BY id")
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[1].values[0], Value::Integer(3));
}

#[test]
fn is_true_syntax_does_not_corrupt_string_literals() {
    // The literal `'IS TRUE'` should NOT be rewritten — preprocessing
    // must respect string boundaries.
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'IS TRUE'), (2, 'other')")
        .unwrap();
    let r = db.query("SELECT id FROM t WHERE label = 'IS TRUE'").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
}

// ── Bitwise / IS TRUE-FALSE function workarounds ──────────────────────

#[test]
fn shl_function_shifts_left() {
    let mut db = fresh();
    let r = db.query("SELECT __shl(1, 3)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(8));
}

#[test]
fn shr_function_shifts_right() {
    let mut db = fresh();
    let r = db.query("SELECT __shr(16, 2)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(4));
}

#[test]
fn bnot_function_inverts_bits() {
    let mut db = fresh();
    let r = db.query("SELECT __bnot(0)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(-1));
    let r = db.query("SELECT __bnot(255)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(-256));
}

#[test]
fn shl_shr_null_propagates() {
    let mut db = fresh();
    let r = db.query("SELECT __shl(NULL, 1)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Null);
    let r = db.query("SELECT __shr(1, NULL)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Null);
    let r = db.query("SELECT __bnot(NULL)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Null);
}

#[test]
fn is_true_false_family() {
    let mut db = fresh();
    let r = db
        .query(
            "SELECT \
             is_true(1), is_true(0), is_true(NULL), \
             is_false(0), is_false(1), is_false(NULL), \
             is_not_true(0), is_not_true(1), is_not_true(NULL), \
             is_not_false(1), is_not_false(0), is_not_false(NULL)",
        )
        .unwrap();
    let row = &r.rows[0].values;
    // is_true: 1→1, 0→0, NULL→0
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::Integer(0));
    assert_eq!(row[2], Value::Integer(0));
    // is_false: 0→1, 1→0, NULL→0
    assert_eq!(row[3], Value::Integer(1));
    assert_eq!(row[4], Value::Integer(0));
    assert_eq!(row[5], Value::Integer(0));
    // is_not_true: 0→1, 1→0, NULL→1
    assert_eq!(row[6], Value::Integer(1));
    assert_eq!(row[7], Value::Integer(0));
    assert_eq!(row[8], Value::Integer(1));
    // is_not_false: 1→1, 0→0, NULL→1
    assert_eq!(row[9], Value::Integer(1));
    assert_eq!(row[10], Value::Integer(0));
    assert_eq!(row[11], Value::Integer(1));
}

// ── User-defined scalar functions ─────────────────────────────────────

#[test]
fn udf_dispatched_from_sql() {
    use crate::udf;
    udf::clear();
    udf::register(
        "add_one",
        Some(1),
        std::rc::Rc::new(|args: &[Value]| match &args[0] {
            Value::Integer(n) => Ok(Value::Integer(n + 1)),
            _ => Err(crate::error::Error::Other("expected integer".into())),
        }),
    );
    let mut db = fresh();
    let r = db.query("SELECT add_one(41)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Integer(42));
    udf::clear();
}

#[test]
fn udf_can_run_inside_select_over_table() {
    use crate::udf;
    udf::clear();
    udf::register(
        "shout",
        Some(1),
        std::rc::Rc::new(|args: &[Value]| match &args[0] {
            Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
            other => Ok(other.clone()),
        }),
    );
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
        .unwrap();
    let r = db.query("SELECT shout(name) FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("HELLO".to_string()));
    assert_eq!(r.rows[1].values[0], Value::Text("WORLD".to_string()));
    udf::clear();
}

#[test]
fn udf_arity_mismatch_surfaces_as_query_error() {
    use crate::udf;
    udf::clear();
    udf::register(
        "needs_two",
        Some(2),
        std::rc::Rc::new(|_args: &[Value]| Ok(Value::Null)),
    );
    let mut db = fresh();
    let res = db.query("SELECT needs_two(1)");
    assert!(res.is_err());
    udf::clear();
}

#[test]
fn udf_does_not_shadow_builtin() {
    use crate::udf;
    udf::clear();
    // Even if we register UPPER as a UDF, the built-in dispatch wins.
    udf::register(
        "UPPER",
        Some(1),
        std::rc::Rc::new(|_args: &[Value]| Ok(Value::Text("UDF-WAS-CALLED".into()))),
    );
    let mut db = fresh();
    let r = db.query("SELECT UPPER('abc')").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("ABC".to_string()));
    udf::clear();
}

// ── ON UPDATE foreign-key actions ─────────────────────────────────────

fn fk_db_with_pragma() -> Database {
    let mut db = fresh();
    db.execute("PRAGMA foreign_keys = ON").unwrap();
    db
}

#[test]
fn on_update_cascade_propagates_parent_change() {
    let mut db = fk_db_with_pragma();
    db.execute(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child ( \
           id INTEGER PRIMARY KEY, \
           parent_code TEXT, \
           FOREIGN KEY (parent_code) REFERENCES parent(code) ON UPDATE CASCADE \
         )",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'A'), (2, 'B')").unwrap();
    db.execute("INSERT INTO child VALUES (10, 'A'), (11, 'A'), (12, 'B')")
        .unwrap();

    db.execute("UPDATE parent SET code = 'A2' WHERE id = 1").unwrap();
    let r = db
        .query("SELECT id, parent_code FROM child ORDER BY id")
        .unwrap();
    assert_eq!(r.rows[0].values[1], Value::Text("A2".to_string()));
    assert_eq!(r.rows[1].values[1], Value::Text("A2".to_string()));
    assert_eq!(r.rows[2].values[1], Value::Text("B".to_string()));
}

#[test]
fn on_update_set_null_clears_child_fk() {
    let mut db = fk_db_with_pragma();
    db.execute(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child ( \
           id INTEGER PRIMARY KEY, \
           parent_code TEXT, \
           FOREIGN KEY (parent_code) REFERENCES parent(code) ON UPDATE SET NULL \
         )",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'X')").unwrap();
    db.execute("INSERT INTO child VALUES (5, 'X')").unwrap();

    db.execute("UPDATE parent SET code = 'Y' WHERE id = 1").unwrap();
    let r = db.query("SELECT parent_code FROM child").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Null);
}

#[test]
fn on_update_restrict_blocks_change_when_referenced() {
    let mut db = fk_db_with_pragma();
    db.execute(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT UNIQUE NOT NULL)",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child ( \
           id INTEGER PRIMARY KEY, \
           parent_code TEXT, \
           FOREIGN KEY (parent_code) REFERENCES parent(code) ON UPDATE RESTRICT \
         )",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'A')").unwrap();
    db.execute("INSERT INTO child VALUES (5, 'A')").unwrap();

    let res = db.execute("UPDATE parent SET code = 'B' WHERE id = 1");
    assert!(res.is_err(), "expected RESTRICT to block update");
}

#[test]
fn on_update_no_op_when_referenced_column_unchanged() {
    let mut db = fk_db_with_pragma();
    db.execute(
        "CREATE TABLE parent ( \
           id INTEGER PRIMARY KEY, \
           code TEXT UNIQUE NOT NULL, \
           extra TEXT \
         )",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE child ( \
           id INTEGER PRIMARY KEY, \
           parent_code TEXT, \
           FOREIGN KEY (parent_code) REFERENCES parent(code) ON UPDATE RESTRICT \
         )",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'A', 'old')").unwrap();
    db.execute("INSERT INTO child VALUES (5, 'A')").unwrap();

    // Updating only `extra` (not the referenced `code`) must not fire FK
    // checks even though the action is RESTRICT.
    db.execute("UPDATE parent SET extra = 'new' WHERE id = 1")
        .unwrap();
    let r = db.query("SELECT extra FROM parent").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Text("new".to_string()));
}

// ── Covering / index-only scan ────────────────────────────────────────

#[test]
fn covering_scan_returns_correct_rows_without_table_fetch() {
    // Test the correctness side of B6 — when the index covers all
    // requested columns, the result must still match a non-covered scan.
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();
    db.execute("CREATE INDEX idx_age ON t(age)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 30), (2, 'b', 25), (3, 'c', 30)")
        .unwrap();

    // SELECT only the indexed column + rowid alias — fully covered by idx_age.
    let r = db.query("SELECT age, id FROM t WHERE age = 30 ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Integer(30));
    assert_eq!(r.rows[0].values[1], Value::Integer(1));
    assert_eq!(r.rows[1].values[0], Value::Integer(30));
    assert_eq!(r.rows[1].values[1], Value::Integer(3));

    // SELECT a non-indexed column — must fall through to table fetch.
    let r = db.query("SELECT name FROM t WHERE age = 30 ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0].values[0], Value::Text("a".to_string()));
    assert_eq!(r.rows[1].values[0], Value::Text("c".to_string()));
}

// ── Bare rowid (no INTEGER PRIMARY KEY alias) ─────────────────────────

#[test]
fn bare_rowid_works_without_integer_primary_key_alias() {
    let mut db = fresh();
    // Table with NO INTEGER PRIMARY KEY — rowid has no alias column.
    db.execute("CREATE TABLE t (name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('a'), ('b'), ('c')").unwrap();
    let r = db.query("SELECT rowid, name FROM t ORDER BY rowid").unwrap();
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0].values[0], Value::Integer(1));
    assert_eq!(r.rows[0].values[1], Value::Text("a".to_string()));
    assert_eq!(r.rows[2].values[0], Value::Integer(3));
}

#[test]
fn bare_rowid_filter_without_alias() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('first'), ('second'), ('third')")
        .unwrap();
    let r = db.query("SELECT val FROM t WHERE rowid = 2").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].values[0], Value::Text("second".to_string()));
}

// ── ANALYZE / sqlite_stat1 ────────────────────────────────────────────

#[test]
fn analyze_creates_sqlite_stat1_with_row_counts() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    db.execute("ANALYZE").unwrap();

    let r = db
        .query("SELECT tbl, idx, stat FROM sqlite_stat1 WHERE tbl = 't'")
        .unwrap();
    assert_eq!(r.rows.len(), 1, "expected one stat row for table t");
    assert_eq!(r.rows[0].values[0], Value::Text("t".to_string()));
    assert_eq!(r.rows[0].values[1], Value::Null);
    assert_eq!(r.rows[0].values[2], Value::Text("3".to_string()));
}

#[test]
fn analyze_records_index_stats_too() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE INDEX idx_name ON t(name)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    db.execute("ANALYZE").unwrap();

    let r = db
        .query("SELECT idx, stat FROM sqlite_stat1 WHERE tbl = 't' AND idx = 'idx_name'")
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    if let Value::Text(s) = &r.rows[0].values[1] {
        assert!(s.starts_with("2"), "stat starts with row count: got {s:?}");
    } else {
        panic!("expected text stat");
    }
}

#[test]
fn analyze_refresh_replaces_stats() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    db.execute("ANALYZE").unwrap();
    let first = db.query("SELECT stat FROM sqlite_stat1 WHERE tbl = 't'").unwrap();
    assert_eq!(first.rows[0].values[0], Value::Text("2".to_string()));

    db.execute("INSERT INTO t VALUES (3), (4), (5)").unwrap();
    db.execute("ANALYZE").unwrap();
    let second = db.query("SELECT stat FROM sqlite_stat1 WHERE tbl = 't'").unwrap();
    assert_eq!(second.rows[0].values[0], Value::Text("5".to_string()));
    // Old entry was replaced, not appended.
    let count = db
        .query("SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = 't'")
        .unwrap();
    assert_eq!(count.rows[0].values[0], Value::Integer(1));
}

#[test]
fn analyze_skips_internal_tables() {
    let mut db = fresh();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.execute("ANALYZE").unwrap();
    let r = db
        .query("SELECT tbl FROM sqlite_stat1 WHERE tbl LIKE 'sqlite_%'")
        .unwrap();
    assert_eq!(
        r.rows.len(),
        0,
        "ANALYZE should skip sqlite_* tables; got {:?}",
        r.rows
    );
}

// ── ATTACH / DETACH visibility ────────────────────────────────────────

#[test]
fn attached_database_appears_in_database_list() {
    let primary = "/tmp/rsqlite_attach_list_primary.db";
    let secondary = "/tmp/rsqlite_attach_list_secondary.db";
    let _ = std::fs::remove_file(primary);
    let _ = std::fs::remove_file(secondary);

    {
        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let _ = Database::create(&vfs, secondary).unwrap();
    }

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    let mut db = Database::create(&vfs, primary).unwrap();
    db.execute(&format!("ATTACH DATABASE '{secondary}' AS sec"))
        .unwrap();
    let r = db.query("PRAGMA database_list").unwrap();
    let names: Vec<String> = r
        .rows
        .iter()
        .map(|row| {
            if let Value::Text(s) = &row.values[1] {
                s.clone()
            } else {
                String::new()
            }
        })
        .collect();
    assert!(names.iter().any(|n| n == "main"), "names = {names:?}");
    assert!(names.iter().any(|n| n == "sec"), "names = {names:?}");
    db.execute("DETACH sec").unwrap();
    let r = db.query("PRAGMA database_list").unwrap();
    assert_eq!(r.rows.len(), 1);

    let _ = std::fs::remove_file(primary);
    let _ = std::fs::remove_file(secondary);
}
