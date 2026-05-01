pub use rsqlite_storage::codec::Value;

#[derive(Debug, Clone, Default)]
pub struct Row {
    pub values: Vec<Value>,
    /// Underlying btree rowid for this row, when known. Set by table /
    /// index scans so `SELECT rowid FROM t` works on tables that don't
    /// have an `INTEGER PRIMARY KEY` alias. Computed result rows
    /// (aggregates, joins, projections) leave this as `None`.
    pub rowid: Option<i64>,
}

impl Row {
    /// Build a row whose rowid isn't known (or doesn't apply — e.g. an
    /// aggregate output row).
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            values,
            rowid: None,
        }
    }

    /// Build a row whose rowid is known (typically straight from a btree
    /// cursor). Lets `PlanExpr::Rowid` resolve on tables without an
    /// `INTEGER PRIMARY KEY` alias.
    pub fn with_rowid(values: Vec<Value>, rowid: i64) -> Self {
        Self {
            values,
            rowid: Some(rowid),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}
