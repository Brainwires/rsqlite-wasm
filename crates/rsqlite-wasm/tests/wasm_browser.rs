//! Browser-only `wasm-bindgen-test` coverage for the IndexedDB VFS.
//! Run with:
//!   wasm-pack test --headless --chrome crates/rsqlite-wasm
//!
//! IndexedDB works on the main thread, so wasm-bindgen-test (which
//! runs in the main thread) can exercise it directly. OPFS's
//! FileSystemSyncAccessHandle is dedicated-worker-only, so OPFS code
//! paths aren't covered here — they're exercised at runtime by the
//! JS package's tests instead.

#![cfg(target_arch = "wasm32")]

use rsqlite_wasm::WasmDatabase;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

fn unique_db_name(suffix: &str) -> String {
    let n = (js_sys::Math::random() * 1e9) as u64;
    format!("rsqlite_test_{n}_{suffix}")
}

fn col_str(row: &wasm_bindgen::JsValue, name: &str) -> String {
    js_sys::Reflect::get(row, &wasm_bindgen::JsValue::from_str(name))
        .unwrap()
        .as_string()
        .unwrap()
}

fn col_int(row: &wasm_bindgen::JsValue, name: &str) -> i64 {
    js_sys::Reflect::get(row, &wasm_bindgen::JsValue::from_str(name))
        .unwrap()
        .as_f64()
        .unwrap() as i64
}

#[wasm_bindgen_test]
async fn open_with_idb_creates_then_reopens() {
    let name = unique_db_name("idb_basic");

    {
        let mut db = WasmDatabase::open_with_idb(&name, None).await.unwrap();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        db.exec("INSERT INTO t VALUES (1, 'persisted')").unwrap();
        db.flush().unwrap();
    }

    {
        let mut db = WasmDatabase::open_with_idb(&name, None).await.unwrap();
        let row = db.query_one("SELECT v FROM t WHERE id = 1").unwrap();
        assert_eq!(col_str(&row, "v"), "persisted");
    }
}

#[wasm_bindgen_test]
async fn open_with_idb_creates_fresh_db_when_missing() {
    let name = unique_db_name("idb_fresh");
    let mut db = WasmDatabase::open_with_idb(&name, None).await.unwrap();
    let n = db.exec("CREATE TABLE t (x INTEGER)").unwrap();
    assert_eq!(n, 0);
}

#[wasm_bindgen_test]
async fn open_with_idb_accepts_custom_chunk_size() {
    let name = unique_db_name("idb_chunk");
    let mut db = WasmDatabase::open_with_idb(&name, Some(1024 * 1024))
        .await
        .unwrap();
    db.exec("CREATE TABLE t (x INTEGER)").unwrap();
    db.exec("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let row = db.query_one("SELECT COUNT(*) AS n FROM t").unwrap();
    assert_eq!(col_int(&row, "n"), 3);
}

#[wasm_bindgen_test]
async fn idb_to_buffer_reads_from_disk() {
    let name = unique_db_name("idb_tobuf");
    let mut db = WasmDatabase::open_with_idb(&name, None).await.unwrap();
    db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
    db.exec("INSERT INTO t VALUES (1)").unwrap();
    db.flush().unwrap();

    let buf = db.to_buffer().unwrap();
    assert!(buf.len() >= 4096);
    assert_eq!(&buf[..16], b"SQLite format 3\0");
}

#[wasm_bindgen_test]
async fn idb_data_survives_flush_and_reopen() {
    let name = unique_db_name("idb_flush");
    {
        let mut db = WasmDatabase::open_with_idb(&name, None).await.unwrap();
        db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        for i in 0..50 {
            let sql = format!("INSERT INTO t VALUES ({i}, 'row{i}')");
            db.exec(&sql).unwrap();
        }
        db.flush().unwrap();
    }
    {
        let mut db = WasmDatabase::open_with_idb(&name, None).await.unwrap();
        let row = db.query_one("SELECT COUNT(*) AS n FROM t").unwrap();
        assert_eq!(col_int(&row, "n"), 50);
        let row = db.query_one("SELECT v FROM t WHERE id = 25").unwrap();
        assert_eq!(col_str(&row, "v"), "row25");
    }
}
