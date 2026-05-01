use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::eval_helpers::value_to_text;
use crate::json::{self, JsonValue};
use crate::planner::PlanExpr;
use crate::types::{QueryResult, Row};

/// Materialize a table-valued function call into a QueryResult. Currently
/// supports `json_each(json [, path])` and `json_tree(json [, path])`.
pub(super) fn execute_table_function(
    name: &str,
    args: &[PlanExpr],
    pager: &mut Pager,
    catalog: &Catalog,
) -> Result<QueryResult> {
    if args.is_empty() {
        return Err(Error::Other(format!(
            "{name}() requires at least 1 argument"
        )));
    }

    // Evaluate args against an empty row — table-valued function calls in
    // FROM clause take constant or correlated subquery args; only constants
    // work in our impl since we don't yet thread a correlated row in.
    let empty_row = Row::new(Vec::new());
    let json_arg = super::eval::eval_expr(&args[0], &empty_row, &[], pager, catalog)?;
    let path_arg = if args.len() >= 2 {
        super::eval::eval_expr(&args[1], &empty_row, &[], pager, catalog)?
    } else {
        Value::Text("$".to_string())
    };

    let columns = vec![
        "key".to_string(),
        "value".to_string(),
        "type".to_string(),
        "atom".to_string(),
        "id".to_string(),
        "parent".to_string(),
        "fullkey".to_string(),
        "path".to_string(),
    ];

    if matches!(json_arg, Value::Null) {
        return Ok(QueryResult {
            columns,
            rows: Vec::new(),
        });
    }

    let json_text = value_to_text(&json_arg);
    let parsed = match json::parse_json(&json_text) {
        Ok(v) => v,
        Err(_) => {
            return Ok(QueryResult {
                columns,
                rows: Vec::new(),
            });
        }
    };

    let path_str = value_to_text(&path_arg);
    let root = match parsed.extract_path(&path_str) {
        Some(v) => v.clone(),
        None => {
            return Ok(QueryResult {
                columns,
                rows: Vec::new(),
            });
        }
    };

    let mut emitter = Emitter::new(path_str);
    let recursive = name == "json_tree";
    if recursive {
        // For json_tree, the very first row describes the root itself.
        emitter.emit_root(&root);
    }
    emitter.walk(&root, None, !recursive);

    Ok(QueryResult {
        columns,
        rows: emitter.rows,
    })
}

struct Emitter {
    base_path: String,
    rows: Vec<Row>,
    next_id: i64,
}

impl Emitter {
    fn new(base_path: String) -> Self {
        Self {
            base_path,
            rows: Vec::new(),
            next_id: 0,
        }
    }

    fn fresh_id(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Emit the root node row (used by json_tree only). It has key=NULL,
    /// parent=NULL and fullkey=path.
    fn emit_root(&mut self, val: &JsonValue) {
        let id = self.fresh_id();
        let (sql_value, atom) = json_value_for_row(val);
        let path_str = parent_path(&self.base_path);
        let key_str = leaf_key(&self.base_path);
        self.rows.push(Row { values: vec![
                key_str,
                sql_value,
                Value::Text(val.type_name().to_string()),
                atom,
                Value::Integer(id),
                Value::Null,
                Value::Text(self.base_path.clone()),
                Value::Text(path_str),
            ], rowid: None });
    }

    /// Walk the value, emitting one row per child encountered.
    /// `shallow` = true means stop at the immediate children (json_each);
    /// false means recurse (json_tree).
    fn walk(&mut self, val: &JsonValue, parent_id: Option<i64>, shallow: bool) {
        match val {
            JsonValue::Object(obj) => {
                for (k, v) in obj {
                    let id = self.fresh_id();
                    let (sql_value, atom) = json_value_for_row(v);
                    let fullkey = format!("{}.{}", self.base_path, k);
                    self.rows.push(Row { values: vec![
                            Value::Text(k.clone()),
                            sql_value,
                            Value::Text(v.type_name().to_string()),
                            atom,
                            Value::Integer(id),
                            parent_id.map(Value::Integer).unwrap_or(Value::Null),
                            Value::Text(fullkey.clone()),
                            Value::Text(self.base_path.clone()),
                        ], rowid: None });
                    // Recurse only into containers — scalars were already
                    // emitted as the child row above.
                    if !shallow && matches!(v, JsonValue::Object(_) | JsonValue::Array(_)) {
                        let saved = std::mem::replace(&mut self.base_path, fullkey);
                        self.walk(v, Some(id), false);
                        self.base_path = saved;
                    }
                }
            }
            JsonValue::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let id = self.fresh_id();
                    let (sql_value, atom) = json_value_for_row(v);
                    let fullkey = format!("{}[{}]", self.base_path, i);
                    self.rows.push(Row { values: vec![
                            Value::Integer(i as i64),
                            sql_value,
                            Value::Text(v.type_name().to_string()),
                            atom,
                            Value::Integer(id),
                            parent_id.map(Value::Integer).unwrap_or(Value::Null),
                            Value::Text(fullkey.clone()),
                            Value::Text(self.base_path.clone()),
                        ], rowid: None });
                    if !shallow && matches!(v, JsonValue::Object(_) | JsonValue::Array(_)) {
                        let saved = std::mem::replace(&mut self.base_path, fullkey);
                        self.walk(v, Some(id), false);
                        self.base_path = saved;
                    }
                }
            }
            // Scalar root: json_each emits one row with key=NULL.
            _ => {
                let id = self.fresh_id();
                let (sql_value, atom) = json_value_for_row(val);
                self.rows.push(Row { values: vec![
                        Value::Null,
                        sql_value,
                        Value::Text(val.type_name().to_string()),
                        atom,
                        Value::Integer(id),
                        parent_id.map(Value::Integer).unwrap_or(Value::Null),
                        Value::Text(self.base_path.clone()),
                        Value::Text(parent_path(&self.base_path)),
                    ], rowid: None });
            }
        }
    }
}

/// Convert a JsonValue to (value-column, atom-column). For containers, value
/// is the JSON-encoded text and atom is NULL.
fn json_value_for_row(val: &JsonValue) -> (Value, Value) {
    match val {
        JsonValue::Null => (Value::Null, Value::Null),
        JsonValue::Bool(b) => (
            Value::Integer(if *b { 1 } else { 0 }),
            Value::Integer(if *b { 1 } else { 0 }),
        ),
        JsonValue::Number(n) => {
            let v = if *n == (*n as i64) as f64 && n.is_finite() {
                Value::Integer(*n as i64)
            } else {
                Value::Real(*n)
            };
            (v.clone(), v)
        }
        JsonValue::String(s) => (Value::Text(s.clone()), Value::Text(s.clone())),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            (Value::Text(val.to_string_repr()), Value::Null)
        }
    }
}

/// Strip the trailing `.foo` or `[N]` from a JSON path. Used to compute the
/// `path` column for the root of json_tree, and for scalar json_each.
fn parent_path(p: &str) -> String {
    if p == "$" {
        return String::new();
    }
    if let Some(idx) = p.rfind('.') {
        return p[..idx].to_string();
    }
    if let Some(idx) = p.rfind('[') {
        return p[..idx].to_string();
    }
    "$".to_string()
}

/// Extract the leaf key from a JSON path: `$.a.b` -> `b`, `$.a[2]` -> `2`,
/// `$` -> NULL. Used for the `key` column of the root row.
fn leaf_key(p: &str) -> Value {
    if p == "$" {
        return Value::Null;
    }
    if let Some(idx) = p.rfind('.') {
        return Value::Text(p[idx + 1..].to_string());
    }
    if let Some(open) = p.rfind('[') {
        if let Some(close) = p.rfind(']') {
            if let Ok(n) = p[open + 1..close].parse::<i64>() {
                return Value::Integer(n);
            }
        }
    }
    Value::Null
}
