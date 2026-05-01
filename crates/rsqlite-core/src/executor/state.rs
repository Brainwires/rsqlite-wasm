use std::cell::RefCell;
use std::collections::HashMap;

use rsqlite_storage::codec::Value;

use crate::types::QueryResult;

thread_local! {
    static BOUND_PARAMS: RefCell<Vec<Value>> = RefCell::new(Vec::new());
    static LAST_INSERT_ROWID: RefCell<i64> = RefCell::new(0);
    static LAST_CHANGES: RefCell<i64> = RefCell::new(0);
    static TOTAL_CHANGES_COUNT: RefCell<i64> = RefCell::new(0);
    static FOREIGN_KEYS_ENABLED: RefCell<bool> = RefCell::new(false);
    static RECURSIVE_CTE_WORKING: RefCell<HashMap<String, QueryResult>> = RefCell::new(HashMap::new());
    static TRIGGER_DEPTH: RefCell<u32> = RefCell::new(0);
}

pub fn set_params(params: Vec<Value>) {
    BOUND_PARAMS.with(|p| *p.borrow_mut() = params);
}

pub fn clear_params() {
    BOUND_PARAMS.with(|p| p.borrow_mut().clear());
}

pub(super) fn get_param(index: usize) -> Value {
    BOUND_PARAMS.with(|p| p.borrow().get(index).cloned().unwrap_or(Value::Null))
}

pub(super) fn set_last_insert_rowid(rowid: i64) {
    LAST_INSERT_ROWID.with(|r| *r.borrow_mut() = rowid);
}

pub(super) fn get_last_insert_rowid() -> i64 {
    LAST_INSERT_ROWID.with(|r| *r.borrow())
}

pub(super) fn set_changes(count: i64) {
    LAST_CHANGES.with(|c| *c.borrow_mut() = count);
    TOTAL_CHANGES_COUNT.with(|t| *t.borrow_mut() += count);
}

pub(super) fn get_changes() -> i64 {
    LAST_CHANGES.with(|c| *c.borrow())
}

pub(super) fn get_total_changes() -> i64 {
    TOTAL_CHANGES_COUNT.with(|t| *t.borrow())
}

pub fn set_foreign_keys_enabled(enabled: bool) {
    FOREIGN_KEYS_ENABLED.with(|f| *f.borrow_mut() = enabled);
}

pub(super) fn foreign_keys_enabled() -> bool {
    FOREIGN_KEYS_ENABLED.with(|f| *f.borrow())
}

pub fn get_last_insert_rowid_pub() -> i64 {
    get_last_insert_rowid()
}

pub fn get_changes_pub() -> i64 {
    get_changes()
}

pub fn get_total_changes_pub() -> i64 {
    get_total_changes()
}

pub(super) fn cte_working_set_insert(name: String, qr: QueryResult) {
    RECURSIVE_CTE_WORKING.with(|w| {
        w.borrow_mut().insert(name, qr);
    });
}

pub(super) fn cte_working_set_remove(name: &str) {
    RECURSIVE_CTE_WORKING.with(|w| {
        w.borrow_mut().remove(name);
    });
}

pub(super) fn cte_working_set_get(name: &str) -> Option<QueryResult> {
    RECURSIVE_CTE_WORKING.with(|w| w.borrow().get(name).cloned())
}

pub(super) fn trigger_depth_get() -> u32 {
    TRIGGER_DEPTH.with(|d| *d.borrow())
}

pub(super) fn trigger_depth_inc() {
    TRIGGER_DEPTH.with(|d| *d.borrow_mut() += 1);
}

pub(super) fn trigger_depth_dec() {
    TRIGGER_DEPTH.with(|d| *d.borrow_mut() -= 1);
}
