//! `wasm-bindgen-test` coverage for the WasmDatabase surface in `lib.rs`.
//!
//! Run with:
//!   wasm-pack test --node crates/rsqlite-wasm
//!
//! These tests target everything that doesn't need a browser (in-memory
//! VFS, value conversion, exec/query/queryParams, createFunction,
//! roundtrip via to_buffer/from_buffer, exec_many statement splitting).
//! The OPFS / IndexedDB paths require browser APIs and are exercised
//! separately via `wasm-pack test --headless --chrome`.

#![cfg(target_arch = "wasm32")]

use js_sys::{Array, Reflect, Uint8Array};
use rsqlite_wasm::WasmDatabase;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_node_experimental);

fn fresh() -> WasmDatabase {
    WasmDatabase::new().expect("fresh in-memory db")
}

fn first_row(rows: JsValue) -> JsValue {
    let arr: Array = rows.dyn_into().unwrap();
    assert!(arr.length() > 0, "expected at least one row");
    arr.get(0)
}

fn col(row: &JsValue, name: &str) -> JsValue {
    Reflect::get(row, &JsValue::from_str(name)).unwrap()
}

#[wasm_bindgen_test]
fn open_in_memory_constructs_a_db() {
    let mut db = WasmDatabase::open_in_memory().unwrap();
    let n = db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    assert_eq!(n, 0);
}

#[wasm_bindgen_test]
fn exec_returns_affected_row_count() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    let n = db.exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    assert_eq!(n, 3);
}

#[wasm_bindgen_test]
fn query_returns_array_of_objects() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    db.exec("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')").unwrap();

    let rows = db.query("SELECT id, name FROM t ORDER BY id").unwrap();
    let arr: Array = rows.dyn_into().unwrap();
    assert_eq!(arr.length(), 2);

    let first = arr.get(0);
    assert_eq!(col(&first, "id").as_f64().unwrap() as i64, 1);
    assert_eq!(col(&first, "name").as_string().unwrap(), "alpha");
}

#[wasm_bindgen_test]
fn query_one_returns_object_or_null() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();

    let none = db.query_one("SELECT * FROM t").unwrap();
    assert!(none.is_null());

    db.exec("INSERT INTO t VALUES (42)").unwrap();
    let some = db.query_one("SELECT id FROM t").unwrap();
    assert!(!some.is_null());
    assert_eq!(col(&some, "id").as_f64().unwrap() as i64, 42);
}

#[wasm_bindgen_test]
fn exec_params_roundtrips_integer_text_real_blob_null() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (i INTEGER, r REAL, s TEXT, b BLOB, n INTEGER)").unwrap();

    let params = Array::new();
    params.push(&JsValue::from_f64(7.0));
    params.push(&JsValue::from_f64(3.14));
    params.push(&JsValue::from_str("hi"));
    let blob = Uint8Array::new_with_length(3);
    blob.copy_from(&[1u8, 2, 3]);
    params.push(&blob.into());
    params.push(&JsValue::NULL);

    let n = db
        .exec_params("INSERT INTO t VALUES (?, ?, ?, ?, ?)", params.into())
        .unwrap();
    assert_eq!(n, 1);

    let row = first_row(db.query("SELECT i, r, s, b, n FROM t").unwrap());
    assert_eq!(col(&row, "i").as_f64().unwrap() as i64, 7);
    assert!((col(&row, "r").as_f64().unwrap() - 3.14).abs() < 1e-9);
    assert_eq!(col(&row, "s").as_string().unwrap(), "hi");
    let b: Uint8Array = col(&row, "b").dyn_into().unwrap();
    assert_eq!(b.to_vec(), vec![1, 2, 3]);
    assert!(col(&row, "n").is_null() || col(&row, "n").is_undefined());
}

#[wasm_bindgen_test]
fn query_params_filters_by_bound_value() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();

    let params = Array::new();
    params.push(&JsValue::from_f64(20.0));
    let rows = db.query_params("SELECT id FROM t WHERE v >= ?", params.into()).unwrap();
    let arr: Array = rows.dyn_into().unwrap();
    assert_eq!(arr.length(), 2);
}

#[wasm_bindgen_test]
fn exec_params_rejects_non_array_params() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (x INTEGER)").unwrap();
    let bad = JsValue::from_str("not an array");
    let result = db.exec_params("INSERT INTO t VALUES (?)", bad);
    assert!(result.is_err());
}

#[wasm_bindgen_test]
fn exec_many_runs_multiple_statements() {
    let mut db = fresh();
    db.exec_many(
        "CREATE TABLE t (id INTEGER PRIMARY KEY); \
         INSERT INTO t VALUES (1); \
         INSERT INTO t VALUES (2);",
    )
    .unwrap();
    let row = db.query_one("SELECT COUNT(*) AS n FROM t").unwrap();
    assert_eq!(col(&row, "n").as_f64().unwrap() as i64, 2);
}

#[wasm_bindgen_test]
fn exec_many_keeps_trigger_begin_end_intact() {
    // Regression: split_statements must NOT split on the `;` inside a
    // BEGIN...END trigger body.
    let mut db = fresh();
    db.exec_many(
        "CREATE TABLE log (msg TEXT); \
         CREATE TABLE src (id INTEGER PRIMARY KEY); \
         CREATE TRIGGER trg AFTER INSERT ON src BEGIN \
           INSERT INTO log VALUES ('inserted'); \
         END; \
         INSERT INTO src VALUES (1);",
    )
    .unwrap();
    let row = db.query_one("SELECT msg FROM log").unwrap();
    assert_eq!(col(&row, "msg").as_string().unwrap(), "inserted");
}

#[wasm_bindgen_test]
fn to_buffer_then_from_buffer_roundtrips_data() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
    db.exec("INSERT INTO t VALUES (1, 'persisted')").unwrap();

    let buf = db.to_buffer().unwrap();
    assert!(buf.len() >= 4096, "expected at least one page in the buffer");

    let mut reopened = WasmDatabase::from_buffer(&buf).unwrap();
    let row = reopened.query_one("SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(col(&row, "v").as_string().unwrap(), "persisted");
}

#[wasm_bindgen_test]
fn from_buffer_rejects_garbage() {
    let bad = vec![0u8; 16];
    assert!(WasmDatabase::from_buffer(&bad).is_err());
}

#[wasm_bindgen_test]
fn flush_is_a_noop_for_in_memory_backend() {
    let db = fresh();
    db.flush().unwrap();
}

#[wasm_bindgen_test]
fn create_function_registers_and_callable() {
    let mut db = fresh();

    // double_it(x) -> 2*x
    let cb = js_sys::Function::new_with_args("x", "return Number(x) * 2;");
    db.create_function("double_it", 1, cb);

    let row = db.query_one("SELECT double_it(21) AS v").unwrap();
    assert_eq!(col(&row, "v").as_f64().unwrap() as i64, 42);

    assert!(db.delete_function("double_it"));
    assert!(!db.delete_function("nonexistent"));
}

#[wasm_bindgen_test]
fn create_function_propagates_thrown_errors() {
    let mut db = fresh();
    let cb = js_sys::Function::new_no_args("throw new Error('boom')");
    db.create_function("boom", 0, cb);

    // The query must surface the throwing UDF as an Err, not crash or
    // return rows. We don't peek at the JsError's message — it varies
    // across wasm-bindgen versions — just assert it's an error.
    assert!(db.query("SELECT boom()").is_err());
}

#[wasm_bindgen_test]
fn variadic_function_accepts_arbitrary_arity() {
    let mut db = fresh();
    let cb = js_sys::Function::new_with_args("...args", "return args.length;");
    db.create_function("nargs", -1, cb);

    let row = db.query_one("SELECT nargs(1, 2, 3, 4, 5) AS n").unwrap();
    assert_eq!(col(&row, "n").as_f64().unwrap() as i64, 5);
}

#[wasm_bindgen_test]
fn integer_passes_through_when_fractional_is_zero() {
    // js_to_value() should keep a whole-number f64 as Integer, not Real,
    // so SQL integer columns roundtrip correctly.
    let mut db = fresh();
    db.exec("CREATE TABLE t (x INTEGER)").unwrap();
    let params = Array::new();
    params.push(&JsValue::from_f64(7.0));
    db.exec_params("INSERT INTO t VALUES (?)", params.into()).unwrap();

    let row = db.query_one("SELECT typeof(x) AS t, x AS v FROM t").unwrap();
    assert_eq!(col(&row, "t").as_string().unwrap(), "integer");
    assert_eq!(col(&row, "v").as_f64().unwrap() as i64, 7);
}

#[wasm_bindgen_test]
fn close_consumes_the_handle() {
    let db = fresh();
    db.close();
    // If we got here without panicking, close() ran cleanly.
}

#[wasm_bindgen_test]
fn null_roundtrips_through_query() {
    let mut db = fresh();
    db.exec("CREATE TABLE t (x INTEGER)").unwrap();
    db.exec("INSERT INTO t VALUES (NULL)").unwrap();
    let row = db.query_one("SELECT x FROM t").unwrap();
    let v = col(&row, "x");
    assert!(v.is_null() || v.is_undefined());
}
