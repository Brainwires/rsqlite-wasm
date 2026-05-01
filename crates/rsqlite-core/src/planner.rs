use std::collections::HashMap;

use sqlparser::ast::{self, Expr, SetExpr, Statement, TableFactor};

use crate::catalog::Catalog;
use crate::error::{Error, Result};

type CteMap = HashMap<String, CteDef>;

#[derive(Clone)]
struct CteDef {
    plan: Plan,
    output_columns: Vec<String>,
}

#[path = "plan_expr.rs"]
mod expr;
pub use expr::*;
use expr::{
    collect_aggregates, collect_window_functions, contains_aggregate, contains_window_function,
    plan_limit_expr, plan_order_expr, plan_select_items,
};

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub sql: String,
    pub columns: Vec<CreateColumnDef>,
    pub if_not_exists: bool,
    /// Set when the user wrote `CREATE TABLE … WITHOUT ROWID`. Stored on
    /// the catalog so the SQL is preserved (and external tools that read
    /// `sqlite_schema` see it). Storage is still rowid-keyed for v0.1 —
    /// the PK uniqueness constraint plus the existing composite-PK
    /// uniqueness check give the same query semantics; only on-disk
    /// btree shape differs from SQLite's WITHOUT ROWID layout.
    pub without_rowid: bool,
}

#[derive(Debug, Clone)]
pub struct CreateColumnDef {
    pub name: String,
    pub type_name: String,
    pub is_primary_key: bool,
    pub not_null: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexPlan {
    pub index_name: String,
    pub table_name: String,
    /// Column expressions in source form. Most are simple identifiers; the
    /// AST parser also accepts arbitrary expressions like `lower(name)`.
    pub columns: Vec<String>,
    pub sql: String,
    pub if_not_exists: bool,
    /// WHERE clause for a partial index (in source form). Only rows matching
    /// the predicate are included in the index.
    pub predicate: Option<String>,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table_name: String,
    pub root_page: u32,
    pub table_columns: Vec<ColumnRef>,
    pub target_columns: Option<Vec<String>>,
    pub rows: Vec<Vec<PlanExpr>>,
    pub source_query: Option<Box<Plan>>,
    pub on_conflict: Option<OnConflictPlan>,
    pub or_replace: bool,
    pub returning: Option<Vec<ProjectionItem>>,
    pub conflict_strategy: ConflictStrategy,
}

/// What to do when a row in this INSERT statement violates a constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictStrategy {
    /// Default: abort this row, leave previously-inserted rows in place,
    /// surface the error.
    Abort,
    /// Skip the failing row, continue inserting subsequent rows.
    Ignore,
    /// Stop on the first failure but keep already-inserted rows committed.
    Fail,
    /// Roll the active transaction back, propagating the failure.
    Rollback,
}

#[derive(Debug, Clone)]
pub enum OnConflictPlan {
    DoNothing,
    DoUpdate {
        /// Column names that identify the conflict target. Empty means "any
        /// uniqueness violation" (matches SQLite without an explicit target).
        conflict_columns: Vec<String>,
        assignments: Vec<(String, PlanExpr)>,
        /// WHERE clause on the DO UPDATE branch.
        where_clause: Option<PlanExpr>,
    },
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table_name: String,
    pub root_page: u32,
    pub table_columns: Vec<ColumnRef>,
    pub assignments: Vec<(String, PlanExpr)>,
    pub predicate: Option<PlanExpr>,
    pub returning: Option<Vec<ProjectionItem>>,
    /// Optional FROM clause: target rows are joined with these tables before
    /// the WHERE predicate is evaluated and assignments are computed. The
    /// combined column context lets assignments reference FROM-table values.
    pub from: Option<UpdateFromPlan>,
}

#[derive(Debug, Clone)]
pub struct UpdateFromPlan {
    pub table_name: String,
    pub root_page: u32,
    pub columns: Vec<ColumnRef>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table_name: String,
    pub root_page: u32,
    pub table_columns: Vec<ColumnRef>,
    pub predicate: Option<PlanExpr>,
    pub returning: Option<Vec<ProjectionItem>>,
    pub order_by: Vec<SortKey>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: PlanExpr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum Plan {
    SingleRow,
    /// Table-valued function call like `json_each(x)`. Materialized at exec
    /// time into a temporary QueryResult; the surrounding query treats it as
    /// any other input source.
    TableFunction {
        name: String,
        args: Vec<PlanExpr>,
    },
    RecursiveCte {
        name: String,
        column_names: Vec<String>,
        anchor: Box<Plan>,
        recursive: Box<Plan>,
    },
    RecursiveCteRef {
        name: String,
        columns: Vec<ColumnRef>,
    },
    Union {
        left: Box<Plan>,
        right: Box<Plan>,
        all: bool,
    },
    Intersect {
        left: Box<Plan>,
        right: Box<Plan>,
        all: bool,
    },
    Except {
        left: Box<Plan>,
        right: Box<Plan>,
        all: bool,
    },
    Scan {
        table: String,
        root_page: u32,
        columns: Vec<ColumnRef>,
    },
    /// Materialize a virtual table by invoking its module's scan method.
    /// Filters are NOT pushed down — the surrounding plan wraps the
    /// result in a Filter for any WHERE clause. (xBestIndex is a v0.2
    /// optimization.)
    VirtualScan {
        table: String,
        module: String,
        args: Vec<String>,
        columns: Vec<ColumnRef>,
    },
    /// Register a new virtual table in the catalog. The module must be
    /// registered in the vtab module registry.
    CreateVirtualTable {
        name: String,
        module: String,
        args: Vec<String>,
        if_not_exists: bool,
    },
    /// INSERT INTO a virtual table. Each row is a list of column-aligned
    /// expressions; the executor invokes the module's `insert` hook
    /// per row. UPDATE/DELETE on virtual tables are not yet supported.
    VirtualInsert {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<PlanExpr>>,
    },
    IndexScan {
        table: String,
        table_root_page: u32,
        index_root_page: u32,
        index_name: String,
        columns: Vec<ColumnRef>,
        index_columns: Vec<String>,
        lookup_values: Vec<PlanExpr>,
    },
    IndexRangeScan {
        table: String,
        table_root_page: u32,
        index_root_page: u32,
        index_name: String,
        columns: Vec<ColumnRef>,
        index_column: String,
        lower_bound: Option<(PlanExpr, bool)>,
        upper_bound: Option<(PlanExpr, bool)>,
    },
    Filter {
        input: Box<Plan>,
        predicate: PlanExpr,
    },
    Project {
        input: Box<Plan>,
        outputs: Vec<ProjectionItem>,
    },
    Sort {
        input: Box<Plan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<u64>,
        offset: u64,
    },
    Distinct {
        input: Box<Plan>,
    },
    Aggregate {
        input: Box<Plan>,
        group_by: Vec<PlanExpr>,
        aggregates: Vec<(AggFunc, PlanExpr, bool, Option<PlanExpr>)>,
        having: Option<PlanExpr>,
    },
    NestedLoopJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        condition: Option<PlanExpr>,
        join_type: JoinType,
    },
    CreateTable(CreateTablePlan),
    CreateIndex(CreateIndexPlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    DropIndex {
        index_name: String,
        if_exists: bool,
    },
    AlterTableAddColumn {
        table_name: String,
        column_name: String,
        column_type: String,
    },
    AlterTableRename {
        old_name: String,
        new_name: String,
    },
    CreateView {
        name: String,
        sql: String,
        if_not_exists: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    Window {
        input: Box<Plan>,
        window_exprs: Vec<(PlanExpr, String)>,
        output_columns: Vec<String>,
    },
    CreateTableAsSelect {
        table_name: String,
        if_not_exists: bool,
        query: Box<Plan>,
    },
    Pragma {
        name: String,
        argument: Option<String>,
    },
    Vacuum,
    Reindex {
        /// Target table or index name; empty string means "all".
        target: String,
    },
    Analyze,
    CreateTrigger {
        name: String,
        table_name: String,
        sql: String,
        if_not_exists: bool,
    },
    DropTrigger {
        name: String,
        if_exists: bool,
    },
    AttachDatabase {
        schema_name: String,
        file_path: String,
    },
    DetachDatabase {
        schema_name: String,
    },
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    Release(String),
    RollbackTo(String),
}

pub fn plan_statement(stmt: &Statement, catalog: &Catalog) -> Result<Plan> {
    reset_param_counter();
    match stmt {
        Statement::Query(query) => plan_select(query, catalog, &HashMap::new()),
        Statement::CreateTable(ct) => plan_create_table(ct, catalog),
        Statement::CreateIndex(ci) => plan_create_index(ci),
        Statement::Insert(insert) => plan_insert(insert, catalog),
        Statement::Update {
            table,
            assignments,
            selection,
            returning,
            from,
            ..
        } => plan_update(
            table,
            assignments,
            selection.as_ref(),
            returning.as_deref(),
            from.as_ref(),
            catalog,
        ),
        Statement::Delete(delete) => plan_delete(delete, catalog),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            let name = names
                .first()
                .ok_or_else(|| Error::Other("DROP requires a name".to_string()))?
                .to_string();
            match object_type {
                ast::ObjectType::Table => Ok(Plan::DropTable {
                    table_name: name,
                    if_exists: *if_exists,
                }),
                ast::ObjectType::Index => Ok(Plan::DropIndex {
                    index_name: name,
                    if_exists: *if_exists,
                }),
                ast::ObjectType::View => Ok(Plan::DropView {
                    name,
                    if_exists: *if_exists,
                }),
                other => Err(Error::Other(format!("unsupported DROP {other}"))),
            }
        }
        Statement::AlterTable {
            name, operations, ..
        } => {
            let table_name = name.to_string();
            if operations.len() != 1 {
                return Err(Error::Other(
                    "only single ALTER TABLE operations supported".to_string(),
                ));
            }
            match &operations[0] {
                ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_name = column_def.name.value.clone();
                    let col_type = if column_def.data_type == ast::DataType::Unspecified {
                        String::new()
                    } else {
                        column_def.data_type.to_string()
                    };
                    Ok(Plan::AlterTableAddColumn {
                        table_name,
                        column_name: col_name,
                        column_type: col_type,
                    })
                }
                ast::AlterTableOperation::RenameTable {
                    table_name: new_name,
                } => Ok(Plan::AlterTableRename {
                    old_name: table_name,
                    new_name: new_name.to_string(),
                }),
                other => Err(Error::Other(format!(
                    "unsupported ALTER TABLE operation: {other}"
                ))),
            }
        }
        Statement::CreateView {
            name,
            query,
            or_replace,
            columns: _view_columns,
            ..
        } => {
            let view_name = name.to_string();
            if !or_replace {
                if catalog.get_view(&view_name).is_some() {
                    return Err(Error::Other(format!("view {view_name} already exists")));
                }
            }
            plan_select(query, catalog, &HashMap::new())?;
            let sql = format!("{stmt}");
            Ok(Plan::CreateView {
                name: view_name,
                sql,
                if_not_exists: false,
            })
        }
        Statement::Pragma { name, value, .. } => {
            let pragma_name = name.to_string().to_lowercase();
            if pragma_name == "__vacuum" {
                return Ok(Plan::Vacuum);
            }
            if pragma_name == "__reindex" {
                let target = match &value {
                    Some(ast::Value::SingleQuotedString(s)) => s.clone(),
                    _ => String::new(),
                };
                return Ok(Plan::Reindex { target });
            }
            if pragma_name == "__analyze" {
                return Ok(Plan::Analyze);
            }
            if pragma_name == "__create_trigger" {
                if let Some(val) = &value {
                    let encoded = match val {
                        ast::Value::SingleQuotedString(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let parts: Vec<&str> = encoded.splitn(7, '|').collect();
                    if parts.len() == 7 {
                        let trigger_name = parts[0].to_string();
                        let trigger_table = parts[1].to_string();
                        let trigger_if_not_exists = parts[4] == "1";
                        return Ok(Plan::CreateTrigger {
                            name: trigger_name,
                            table_name: trigger_table,
                            sql: encoded,
                            if_not_exists: trigger_if_not_exists,
                        });
                    }
                }
                return Err(Error::Other("invalid CREATE TRIGGER syntax".to_string()));
            }
            if pragma_name == "__drop_trigger" {
                if let Some(val) = &value {
                    let encoded = match val {
                        ast::Value::SingleQuotedString(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let parts: Vec<&str> = encoded.splitn(2, '|').collect();
                    if parts.len() == 2 {
                        return Ok(Plan::DropTrigger {
                            name: parts[0].to_string(),
                            if_exists: parts[1] == "1",
                        });
                    }
                }
                return Err(Error::Other("invalid DROP TRIGGER syntax".to_string()));
            }
            if pragma_name == "__create_virtual_table" {
                if let Some(val) = &value {
                    let encoded = match val {
                        ast::Value::SingleQuotedString(s) => s.clone(),
                        other => other.to_string(),
                    };
                    let parts: Vec<&str> = encoded.splitn(4, '|').collect();
                    if parts.len() == 4 {
                        let if_not_exists = parts[0] == "1";
                        let name = parts[1].to_string();
                        let module = parts[2].to_string();
                        let args = if parts[3].is_empty() {
                            Vec::new()
                        } else {
                            parts[3]
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .collect()
                        };
                        return Ok(Plan::CreateVirtualTable {
                            name,
                            module,
                            args,
                            if_not_exists,
                        });
                    }
                }
                return Err(Error::Other(
                    "invalid CREATE VIRTUAL TABLE syntax".to_string(),
                ));
            }
            if pragma_name == "__detach" {
                if let Some(val) = &value {
                    let schema_name = match val {
                        ast::Value::SingleQuotedString(s) => s.clone(),
                        other => other.to_string(),
                    };
                    return Ok(Plan::DetachDatabase { schema_name });
                }
                return Err(Error::Other("DETACH requires a schema name".to_string()));
            }
            let argument = value.as_ref().map(|v| match v {
                ast::Value::SingleQuotedString(s) => s.clone(),
                ast::Value::Number(n, _) => n.clone(),
                other => other.to_string(),
            });
            Ok(Plan::Pragma {
                name: pragma_name,
                argument,
            })
        }
        Statement::StartTransaction { .. } => Ok(Plan::Begin),
        Statement::Commit { .. } => Ok(Plan::Commit),
        Statement::Rollback {
            savepoint: Some(name),
            ..
        } => Ok(Plan::RollbackTo(name.value.clone())),
        Statement::Rollback { .. } => Ok(Plan::Rollback),
        Statement::Savepoint { name } => Ok(Plan::Savepoint(name.value.clone())),
        Statement::ReleaseSavepoint { name } => Ok(Plan::Release(name.value.clone())),
        Statement::AttachDatabase {
            schema_name,
            database_file_name,
            ..
        } => {
            let file_path = database_file_name
                .to_string()
                .trim_matches('\'')
                .to_string();
            Ok(Plan::AttachDatabase {
                schema_name: schema_name.value.clone(),
                file_path,
            })
        }
        _ => Err(Error::Other(format!("unsupported statement type: {stmt}"))),
    }
}

pub fn plan_query(stmt: &Statement, catalog: &Catalog) -> Result<Plan> {
    plan_statement(stmt, catalog)
}

fn resolve_table_factor(
    relation: &TableFactor,
    catalog: &Catalog,
    ctes: &CteMap,
) -> Result<(Plan, Vec<ColumnRef>)> {
    match relation {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            let table_name = name.to_string();

            // `FROM json_each(x)` — args is Some when SQL parsed it as a
            // function call rather than a plain table name.
            if let Some(fn_args) = args {
                let lower = table_name.to_lowercase();
                if matches!(lower.as_str(), "json_each" | "json_tree") {
                    let prefix = alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_else(|| table_name.clone());
                    let columns = json_table_function_columns(&prefix);
                    let arg_exprs = fn_args
                        .args
                        .iter()
                        .filter_map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                            _ => None,
                        })
                        .map(|e| plan_expr(e, &[], catalog))
                        .collect::<Result<Vec<_>>>()?;
                    let plan = Plan::TableFunction {
                        name: lower,
                        args: arg_exprs,
                    };
                    return Ok((plan, columns));
                }
                return Err(Error::Other(format!(
                    "unknown table-valued function: {table_name}"
                )));
            }

            if let Some(cte_def) = ctes.get(&table_name.to_lowercase()) {
                let prefix = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());

                let columns: Vec<ColumnRef> = cte_def
                    .output_columns
                    .iter()
                    .enumerate()
                    .map(|(i, col_name)| ColumnRef {
                        name: col_name.clone(),
                        column_index: i,
                        is_rowid_alias: false,
                        table: Some(prefix.clone()),
                        nullable: true,
                        is_primary_key: false,
                        is_unique: false,
                    })
                    .collect();

                return Ok((cte_def.plan.clone(), columns));
            }

            if let Some(table_def) = catalog.get_table(&table_name) {
                let prefix = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());

                let columns: Vec<ColumnRef> = table_def
                    .columns
                    .iter()
                    .map(|c| ColumnRef {
                        name: c.name.clone(),
                        column_index: c.column_index,
                        is_rowid_alias: c.is_rowid_alias,
                        table: Some(prefix.clone()),
                        nullable: c.nullable,
                        is_primary_key: c.is_primary_key,
                        is_unique: c.is_unique,
                    })
                    .collect();

                let plan = Plan::Scan {
                    table: table_name,
                    root_page: table_def.root_page,
                    columns: columns.clone(),
                };

                return Ok((plan, columns));
            }

            if let Some(view_def) = catalog.get_view(&table_name) {
                return resolve_view(&view_def.sql, &table_name, alias, catalog);
            }

            if let Some(vt) = catalog.virtual_tables.get(&table_name.to_lowercase()) {
                let prefix = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| vt.name.clone());
                // Ask the module for its column shape — virtual tables don't
                // store columns in `sqlite_schema`, so we re-instantiate.
                let module = crate::vtab::lookup_module(&vt.module).ok_or_else(|| {
                    Error::Other(format!(
                        "virtual-table module not registered: {}",
                        vt.module
                    ))
                })?;
                let probe = module.create(&vt.name, &vt.args)?;
                let columns: Vec<ColumnRef> = probe
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, name)| ColumnRef {
                        name: name.clone(),
                        column_index: i,
                        is_rowid_alias: false,
                        table: Some(prefix.clone()),
                        nullable: true,
                        is_primary_key: false,
                        is_unique: false,
                    })
                    .collect();
                let plan = Plan::VirtualScan {
                    table: vt.name.clone(),
                    module: vt.module.clone(),
                    args: vt.args.clone(),
                    columns: columns.clone(),
                };
                return Ok((plan, columns));
            }

            Err(Error::Other(format!("table not found: {table_name}")))
        }
        _ => Err(Error::Other(
            "only simple table references are supported".to_string(),
        )),
    }
}

/// Standard column shape for json_each / json_tree (matches SQLite).
fn json_table_function_columns(prefix: &str) -> Vec<ColumnRef> {
    [
        "key", "value", "type", "atom", "id", "parent", "fullkey", "path",
    ]
    .iter()
    .enumerate()
    .map(|(i, name)| ColumnRef {
        name: (*name).to_string(),
        column_index: i,
        is_rowid_alias: false,
        table: Some(prefix.to_string()),
        nullable: true,
        is_primary_key: false,
        is_unique: false,
    })
    .collect()
}

fn resolve_view(
    view_sql: &str,
    view_name: &str,
    alias: &Option<ast::TableAlias>,
    catalog: &Catalog,
) -> Result<(Plan, Vec<ColumnRef>)> {
    let stmts = rsqlite_parser::parse::parse_sql(view_sql)
        .map_err(|e| Error::Other(format!("failed to parse view SQL: {e}")))?;

    match stmts.into_iter().next() {
        Some(Statement::CreateView {
            query,
            columns: view_cols,
            ..
        }) => {
            let prefix = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| view_name.to_string());

            let plan = plan_select(&query, catalog, &HashMap::new())?;

            let output_names = extract_plan_output_names(&plan, &view_cols);
            let columns: Vec<ColumnRef> = output_names
                .iter()
                .enumerate()
                .map(|(i, name)| ColumnRef {
                    name: name.clone(),
                    column_index: i,
                    is_rowid_alias: false,
                    table: Some(prefix.clone()),
                    nullable: true,
                    is_primary_key: false,
                    is_unique: false,
                })
                .collect();

            Ok((plan, columns))
        }
        _ => Err(Error::Other(format!(
            "invalid view definition for {view_name}"
        ))),
    }
}

fn extract_plan_output_names(plan: &Plan, view_cols: &[ast::ViewColumnDef]) -> Vec<String> {
    if !view_cols.is_empty() {
        return view_cols.iter().map(|c| c.name.value.clone()).collect();
    }

    match plan {
        Plan::Project { outputs, .. } => outputs.iter().map(|o| o.alias.clone()).collect(),
        Plan::Distinct { input } => extract_plan_output_names(input, view_cols),
        Plan::Sort { input, .. } => extract_plan_output_names(input, view_cols),
        Plan::Limit { input, .. } => extract_plan_output_names(input, view_cols),
        Plan::Filter { input, .. } => extract_plan_output_names(input, view_cols),
        Plan::Window { output_columns, .. } => output_columns.clone(),
        _ => vec![],
    }
}

fn rewrite_window_refs(expr: &PlanExpr, win_idx: &mut usize, base_col_offset: usize) -> PlanExpr {
    match expr {
        PlanExpr::WindowFunction { .. } => {
            let idx = *win_idx;
            *win_idx += 1;
            PlanExpr::Column(ColumnRef {
                name: format!("__window_{idx}"),
                column_index: base_col_offset + idx,
                is_rowid_alias: false,
                table: None,
                nullable: true,
                is_primary_key: false,
                is_unique: false,
            })
        }
        PlanExpr::BinaryOp { left, op, right } => PlanExpr::BinaryOp {
            left: Box::new(rewrite_window_refs(left, win_idx, base_col_offset)),
            op: *op,
            right: Box::new(rewrite_window_refs(right, win_idx, base_col_offset)),
        },
        PlanExpr::UnaryOp { op, operand } => PlanExpr::UnaryOp {
            op: *op,
            operand: Box::new(rewrite_window_refs(operand, win_idx, base_col_offset)),
        },
        PlanExpr::Cast { expr, type_name } => PlanExpr::Cast {
            expr: Box::new(rewrite_window_refs(expr, win_idx, base_col_offset)),
            type_name: type_name.clone(),
        },
        PlanExpr::Function { name, args } => PlanExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_window_refs(a, win_idx, base_col_offset))
                .collect(),
        },
        other => other.clone(),
    }
}

fn plan_set_expr(set_expr: &SetExpr, catalog: &Catalog, ctes: &CteMap) -> Result<Plan> {
    match set_expr {
        SetExpr::Select(s) => plan_select_body(s, catalog, ctes).map(|(plan, _, _)| plan),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left_plan = plan_set_expr(left, catalog, ctes)?;
            let right_plan = plan_set_expr(right, catalog, ctes)?;
            let all = matches!(set_quantifier, ast::SetQuantifier::All);
            match op {
                ast::SetOperator::Union => Ok(Plan::Union {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    all,
                }),
                ast::SetOperator::Intersect => Ok(Plan::Intersect {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    all,
                }),
                ast::SetOperator::Except => Ok(Plan::Except {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    all,
                }),
                _ => Err(Error::Other(format!("unsupported set operation: {op}"))),
            }
        }
        _ => Err(Error::Other("unsupported set expression".to_string())),
    }
}

fn body_references_name(body: &SetExpr, name: &str) -> bool {
    match body {
        SetExpr::Select(s) => {
            for item in &s.from {
                if let ast::TableFactor::Table { name: tname, .. } = &item.relation {
                    if tname.to_string().to_lowercase() == name {
                        return true;
                    }
                }
                for join in &item.joins {
                    if let ast::TableFactor::Table { name: tname, .. } = &join.relation {
                        if tname.to_string().to_lowercase() == name {
                            return true;
                        }
                    }
                }
            }
            false
        }
        SetExpr::SetOperation { left, right, .. } => {
            body_references_name(left, name) || body_references_name(right, name)
        }
        _ => false,
    }
}

fn is_recursive_cte(cte: &ast::Cte, name: &str) -> bool {
    body_references_name(cte.query.body.as_ref(), name)
}

fn plan_recursive_cte(
    cte: &ast::Cte,
    name: &str,
    column_names: &[String],
    catalog: &Catalog,
    parent_ctes: &CteMap,
) -> Result<(Plan, Plan)> {
    match cte.query.body.as_ref() {
        SetExpr::SetOperation {
            left, right, op, ..
        } if *op == ast::SetOperator::Union => {
            let (anchor_body, recursive_body) = if body_references_name(right, name) {
                (left.as_ref(), right.as_ref())
            } else if body_references_name(left, name) {
                (right.as_ref(), left.as_ref())
            } else {
                return Err(Error::Other(
                    "recursive CTE does not reference itself".into(),
                ));
            };

            let anchor_plan = plan_set_expr(anchor_body, catalog, parent_ctes)?;

            let ref_columns: Vec<ColumnRef> = column_names
                .iter()
                .enumerate()
                .map(|(i, cn)| ColumnRef {
                    name: cn.clone(),
                    column_index: i,
                    is_rowid_alias: false,
                    table: Some(name.to_string()),
                    nullable: true,
                    is_primary_key: false,
                    is_unique: false,
                })
                .collect();

            let mut recursive_ctes = parent_ctes.clone();
            recursive_ctes.insert(
                name.to_string(),
                CteDef {
                    plan: Plan::RecursiveCteRef {
                        name: name.to_string(),
                        columns: ref_columns,
                    },
                    output_columns: column_names.to_vec(),
                },
            );

            let recursive_plan = plan_set_expr(recursive_body, catalog, &recursive_ctes)?;

            Ok((anchor_plan, recursive_plan))
        }
        _ => Err(Error::Other(
            "recursive CTE must use UNION or UNION ALL".into(),
        )),
    }
}

fn plan_select(query: &ast::Query, catalog: &Catalog, parent_ctes: &CteMap) -> Result<Plan> {
    let mut ctes = parent_ctes.clone();

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.to_lowercase();

            if with.recursive && is_recursive_cte(cte, &name) {
                let column_names: Vec<String> = cte
                    .alias
                    .columns
                    .iter()
                    .map(|c| c.name.value.clone())
                    .collect();
                let (anchor_plan, recursive_plan) =
                    plan_recursive_cte(cte, &name, &column_names, catalog, &ctes)?;
                let rcte_plan = Plan::RecursiveCte {
                    name: name.clone(),
                    column_names: column_names.clone(),
                    anchor: Box::new(anchor_plan),
                    recursive: Box::new(recursive_plan),
                };
                let output_columns = if column_names.is_empty() {
                    extract_plan_output_names(&rcte_plan, &[])
                } else {
                    column_names.clone()
                };
                ctes.insert(
                    name,
                    CteDef {
                        plan: rcte_plan,
                        output_columns,
                    },
                );
                continue;
            }

            let mut cte_plan = plan_select(&cte.query, catalog, &ctes)?;
            let output_columns = if cte.alias.columns.is_empty() {
                extract_plan_output_names(&cte_plan, &[])
            } else {
                let orig_names = extract_plan_output_names(&cte_plan, &[]);
                let new_names: Vec<String> = cte
                    .alias
                    .columns
                    .iter()
                    .map(|c| c.name.value.clone())
                    .collect();
                let outputs: Vec<ProjectionItem> = new_names
                    .iter()
                    .enumerate()
                    .map(|(i, alias)| {
                        let orig = orig_names.get(i).cloned().unwrap_or_default();
                        ProjectionItem {
                            expr: PlanExpr::Column(ColumnRef {
                                name: orig,
                                column_index: i,
                                is_rowid_alias: false,
                                table: None,
                                nullable: true,
                                is_primary_key: false,
                                is_unique: false,
                            }),
                            alias: alias.clone(),
                        }
                    })
                    .collect();
                cte_plan = Plan::Project {
                    input: Box::new(cte_plan),
                    outputs,
                };
                new_names
            };
            ctes.insert(
                name,
                CteDef {
                    plan: cte_plan,
                    output_columns,
                },
            );
        }
    }

    match query.body.as_ref() {
        SetExpr::SetOperation { .. } => plan_set_expr(query.body.as_ref(), catalog, &ctes),
        SetExpr::Select(s) => plan_simple_select(query, s, catalog, &ctes),
        _ => Err(Error::Other("unsupported query form".to_string())),
    }
}

fn plan_simple_select(
    query: &ast::Query,
    select: &ast::Select,
    catalog: &Catalog,
    ctes: &CteMap,
) -> Result<Plan> {
    let (mut plan, all_columns, output_names) = plan_select_body(select, catalog, ctes)?;

    // ORDER BY
    if let Some(order_by) = &query.order_by {
        if let ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            let mut keys = Vec::new();
            for ob in exprs {
                let expr = plan_order_expr(&ob.expr, &all_columns, &output_names, catalog)?;
                let descending = ob.options.asc == Some(false);
                keys.push(SortKey {
                    expr,
                    descending,
                    nulls_first: ob.options.nulls_first,
                });
            }
            if !keys.is_empty() {
                // Two competing structures:
                //
                // - Sort(Project(...)): Sort sees the projected columns,
                //   so it can resolve `ORDER BY <alias>` referencing a
                //   computed projection like `SELECT a + b AS sum
                //   FROM t ORDER BY sum`.
                // - Project(Sort(...)): Sort sees the underlying scan
                //   columns, so it can resolve `ORDER BY <col>` where
                //   `<col>` isn't in the SELECT list (e.g.
                //   `SELECT rowid FROM t ORDER BY n`).
                //
                // Pick based on whether any sort key references an alias
                // that doesn't exist as an underlying table column. If
                // yes → keep the old Sort(Project) shape; if no → push
                // Sort below Project.
                let needs_alias_resolution = keys.iter().any(|k| {
                    plan_expr_references_alias_only(&k.expr, &output_names, &all_columns)
                });
                plan = match (plan, needs_alias_resolution) {
                    (Plan::Project { input, outputs }, false) => Plan::Project {
                        input: Box::new(Plan::Sort { input, keys }),
                        outputs,
                    },
                    (other, _) => Plan::Sort {
                        input: Box::new(other),
                        keys,
                    },
                };
            }
        }
    }

    // LIMIT / OFFSET
    let limit_val = query.limit.as_ref().map(plan_limit_expr).transpose()?;
    let offset_val = query
        .offset
        .as_ref()
        .map(|o| plan_limit_expr(&o.value))
        .transpose()?
        .unwrap_or(0);

    if limit_val.is_some() || offset_val > 0 {
        plan = Plan::Limit {
            input: Box::new(plan),
            limit: limit_val,
            offset: offset_val,
        };
    }

    Ok(plan)
}

fn plan_select_body(
    select: &ast::Select,
    catalog: &Catalog,
    ctes: &CteMap,
) -> Result<(Plan, Vec<ColumnRef>, Vec<String>)> {
    // Build a name -> WindowSpec map from the SELECT's WINDOW clause. Resolve
    // chained references (WINDOW w1 AS w2) eagerly.
    let mut named_windows: HashMap<String, ast::WindowSpec> = HashMap::new();
    for def in &select.named_window {
        let name = def.0.value.clone();
        let resolved_spec = match &def.1 {
            ast::NamedWindowExpr::WindowSpec(spec) => spec.clone(),
            ast::NamedWindowExpr::NamedWindow(other) => {
                named_windows.get(&other.value).cloned().ok_or_else(|| {
                    Error::Other(format!(
                        "named window references unknown window: {}",
                        other.value
                    ))
                })?
            }
        };
        named_windows.insert(name, resolved_spec);
    }

    expr::with_named_windows(named_windows, || {
        plan_select_body_inner(select, catalog, ctes)
    })
}

fn plan_select_body_inner(
    select: &ast::Select,
    catalog: &Catalog,
    ctes: &CteMap,
) -> Result<(Plan, Vec<ColumnRef>, Vec<String>)> {
    if select.from.is_empty() {
        let plan = Plan::SingleRow;
        let all_columns: Vec<ColumnRef> = vec![];
        let outputs = plan_select_items(&select.projection, &all_columns, catalog)?;
        let output_names: Vec<String> = outputs.iter().map(|o| o.alias.clone()).collect();
        let project = Plan::Project {
            input: Box::new(plan),
            outputs,
        };
        return Ok((project, all_columns, output_names));
    }

    let from = &select.from[0];
    let (mut plan, mut all_columns) = resolve_table_factor(&from.relation, catalog, ctes)?;

    for join in &from.joins {
        let (right_plan, right_columns) = resolve_table_factor(&join.relation, catalog, ctes)?;
        let left_len = all_columns.len();
        let combined_columns: Vec<ColumnRef> = all_columns
            .iter()
            .chain(right_columns.iter())
            .cloned()
            .collect();

        let (join_type, condition) = match &join.join_operator {
            ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => {
                let cond = plan_join_constraint_with_split(
                    constraint,
                    &combined_columns,
                    left_len,
                    catalog,
                )?;
                (JoinType::Inner, cond)
            }
            ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => {
                let cond = plan_join_constraint_with_split(
                    constraint,
                    &combined_columns,
                    left_len,
                    catalog,
                )?;
                (JoinType::Left, cond)
            }
            ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => {
                let cond = plan_join_constraint_with_split(
                    constraint,
                    &combined_columns,
                    left_len,
                    catalog,
                )?;
                (JoinType::Right, cond)
            }
            ast::JoinOperator::FullOuter(constraint) => {
                let cond = plan_join_constraint_with_split(
                    constraint,
                    &combined_columns,
                    left_len,
                    catalog,
                )?;
                (JoinType::Full, cond)
            }
            ast::JoinOperator::CrossJoin => (JoinType::Cross, None),
            _ => {
                return Err(Error::Other(
                    "only INNER, LEFT, RIGHT, FULL, and CROSS JOIN are supported".to_string(),
                ));
            }
        };

        plan = Plan::NestedLoopJoin {
            left: Box::new(plan),
            right: Box::new(right_plan),
            condition,
            join_type,
        };
        all_columns = combined_columns;
    }

    for extra_from in &select.from[1..] {
        let (right_plan, right_columns) =
            resolve_table_factor(&extra_from.relation, catalog, ctes)?;
        let combined_columns: Vec<ColumnRef> = all_columns
            .iter()
            .chain(right_columns.iter())
            .cloned()
            .collect();

        plan = Plan::NestedLoopJoin {
            left: Box::new(plan),
            right: Box::new(right_plan),
            condition: None,
            join_type: JoinType::Cross,
        };
        all_columns = combined_columns;

        for join in &extra_from.joins {
            let (right_plan, right_cols) = resolve_table_factor(&join.relation, catalog, ctes)?;
            let left_len = all_columns.len();
            let combined: Vec<ColumnRef> = all_columns
                .iter()
                .chain(right_cols.iter())
                .cloned()
                .collect();

            let (join_type, condition) = match &join.join_operator {
                ast::JoinOperator::Inner(c) | ast::JoinOperator::Join(c) => {
                    let cond = plan_join_constraint_with_split(c, &combined, left_len, catalog)?;
                    (JoinType::Inner, cond)
                }
                ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => {
                    let cond = plan_join_constraint_with_split(c, &combined, left_len, catalog)?;
                    (JoinType::Left, cond)
                }
                ast::JoinOperator::Right(c) | ast::JoinOperator::RightOuter(c) => {
                    let cond = plan_join_constraint_with_split(c, &combined, left_len, catalog)?;
                    (JoinType::Right, cond)
                }
                ast::JoinOperator::FullOuter(c) => {
                    let cond = plan_join_constraint_with_split(c, &combined, left_len, catalog)?;
                    (JoinType::Full, cond)
                }
                ast::JoinOperator::CrossJoin => (JoinType::Cross, None),
                _ => {
                    return Err(Error::Other(
                        "only INNER, LEFT, RIGHT, FULL, and CROSS JOIN are supported".to_string(),
                    ));
                }
            };

            plan = Plan::NestedLoopJoin {
                left: Box::new(plan),
                right: Box::new(right_plan),
                condition,
                join_type,
            };
            all_columns = combined;
        }
    }

    // WHERE clause -> Filter (with index optimization)
    if let Some(selection) = &select.selection {
        let predicate = plan_expr(selection, &all_columns, catalog)?;

        if let Some(index_plan) = try_index_scan(&plan, &predicate, &all_columns, catalog) {
            plan = index_plan;
        } else {
            plan = Plan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }
    }

    let mut outputs = plan_select_items(&select.projection, &all_columns, catalog)?;
    let output_names: Vec<String> = outputs.iter().map(|o| o.alias.clone()).collect();

    let group_by_exprs = match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
            let mut planned = Vec::new();
            for e in exprs {
                if let Expr::Value(val) = e {
                    if let ast::Value::Number(n, _) = &val.value {
                        if let Ok(idx) = n.parse::<usize>() {
                            if idx >= 1 && idx <= output_names.len() {
                                let name = &output_names[idx - 1];
                                if let Some(col) = all_columns
                                    .iter()
                                    .find(|c| c.name.eq_ignore_ascii_case(name))
                                {
                                    planned.push(PlanExpr::Column(col.clone()));
                                    continue;
                                }
                            }
                        }
                    }
                }
                planned.push(plan_expr(e, &all_columns, catalog)?);
            }
            planned
        }
        _ => Vec::new(),
    };
    let has_aggregates = outputs.iter().any(|o| contains_aggregate(&o.expr));

    if has_aggregates || !group_by_exprs.is_empty() {
        let mut aggregates: Vec<(AggFunc, PlanExpr, bool, Option<PlanExpr>)> = Vec::new();
        for o in &outputs {
            collect_aggregates(&o.expr, &mut aggregates);
        }

        let having = select
            .having
            .as_ref()
            .map(|e| plan_expr(e, &all_columns, catalog))
            .transpose()?;

        if let Some(ref having_expr) = having {
            collect_aggregates(having_expr, &mut aggregates);
        }

        plan = Plan::Aggregate {
            input: Box::new(plan),
            group_by: group_by_exprs,
            aggregates,
            having,
        };
    }

    let has_windows = outputs.iter().any(|o| contains_window_function(&o.expr));

    if has_windows {
        let mut win_funcs: Vec<PlanExpr> = Vec::new();
        for o in &outputs {
            collect_window_functions(&o.expr, &mut win_funcs);
        }

        let mut window_exprs: Vec<(PlanExpr, String)> = Vec::new();
        for (i, wf) in win_funcs.iter().enumerate() {
            let alias = format!("__window_{i}");
            window_exprs.push((wf.clone(), alias));
        }

        let current_output_names: Vec<String> = match &plan {
            Plan::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut names = Vec::new();
                for gb in group_by {
                    if let PlanExpr::Column(c) = gb {
                        names.push(c.name.clone());
                    } else {
                        names.push(format!("{:?}", gb));
                    }
                }
                for (func, arg, distinct, _filter) in aggregates {
                    names.push(agg_column_name(func, arg, *distinct));
                }
                names
            }
            Plan::Filter { .. } | Plan::Scan { .. } | Plan::NestedLoopJoin { .. } => {
                all_columns.iter().map(|c| c.name.clone()).collect()
            }
            _ => all_columns.iter().map(|c| c.name.clone()).collect(),
        };

        let mut all_output_names = current_output_names.clone();
        for (_, alias) in &window_exprs {
            all_output_names.push(alias.clone());
        }

        plan = Plan::Window {
            input: Box::new(plan),
            window_exprs,
            output_columns: all_output_names.clone(),
        };

        let mut win_idx = 0;
        let mut new_outputs = Vec::new();
        for o in &outputs {
            let rewritten = rewrite_window_refs(&o.expr, &mut win_idx, current_output_names.len());
            new_outputs.push(ProjectionItem {
                expr: rewritten,
                alias: o.alias.clone(),
            });
        }
        outputs = new_outputs;
    }

    let output_names: Vec<String> = outputs.iter().map(|o| o.alias.clone()).collect();
    plan = Plan::Project {
        input: Box::new(plan),
        outputs,
    };

    if select.distinct.is_some() {
        plan = Plan::Distinct {
            input: Box::new(plan),
        };
    }

    Ok((plan, all_columns, output_names))
}

/// Plan a join constraint. `left_len` is the number of columns from the
/// left input — required so USING/NATURAL can find columns on each side
/// independently. The legacy `On` and `None` arms ignore it.
fn plan_join_constraint_with_split(
    constraint: &ast::JoinConstraint,
    columns: &[ColumnRef],
    left_len: usize,
    catalog: &Catalog,
) -> Result<Option<PlanExpr>> {
    plan_join_constraint_inner(constraint, columns, left_len, catalog)
}

fn plan_join_constraint_inner(
    constraint: &ast::JoinConstraint,
    columns: &[ColumnRef],
    left_len: usize,
    catalog: &Catalog,
) -> Result<Option<PlanExpr>> {
    match constraint {
        ast::JoinConstraint::On(expr) => {
            let planned = plan_expr(expr, columns, catalog)?;
            Ok(Some(planned))
        }
        ast::JoinConstraint::None => Ok(None),
        ast::JoinConstraint::Using(names) => {
            let left_cols = &columns[..left_len];
            let right_cols = &columns[left_len..];
            build_using_condition(names, left_cols, right_cols, left_len)
        }
        ast::JoinConstraint::Natural => {
            let left_cols = &columns[..left_len];
            let right_cols = &columns[left_len..];
            build_natural_condition(left_cols, right_cols, left_len)
        }
    }
}

fn build_using_condition(
    names: &[ast::ObjectName],
    left_cols: &[ColumnRef],
    right_cols: &[ColumnRef],
    right_offset: usize,
) -> Result<Option<PlanExpr>> {
    let mut conjuncts: Vec<PlanExpr> = Vec::new();
    for name_obj in names {
        let name = name_obj.to_string();
        let left = left_cols
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(&name))
            .ok_or_else(|| Error::Other(format!("USING column not found on left: {name}")))?;
        let right = right_cols
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(&name))
            .ok_or_else(|| Error::Other(format!("USING column not found on right: {name}")))?;
        let mut left_ref = left.1.clone();
        left_ref.column_index = left.0;
        let mut right_ref = right.1.clone();
        right_ref.column_index = right_offset + right.0;
        conjuncts.push(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column(left_ref)),
            op: BinOp::Eq,
            right: Box::new(PlanExpr::Column(right_ref)),
        });
    }
    Ok(combine_and(conjuncts))
}

fn build_natural_condition(
    left_cols: &[ColumnRef],
    right_cols: &[ColumnRef],
    right_offset: usize,
) -> Result<Option<PlanExpr>> {
    let mut conjuncts: Vec<PlanExpr> = Vec::new();
    for (li, lc) in left_cols.iter().enumerate() {
        if let Some((ri, rc)) = right_cols
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(&lc.name))
        {
            let mut left_ref = lc.clone();
            left_ref.column_index = li;
            let mut right_ref = rc.clone();
            right_ref.column_index = right_offset + ri;
            conjuncts.push(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column(left_ref)),
                op: BinOp::Eq,
                right: Box::new(PlanExpr::Column(right_ref)),
            });
        }
    }
    Ok(combine_and(conjuncts))
}

fn combine_and(mut conjuncts: Vec<PlanExpr>) -> Option<PlanExpr> {
    if conjuncts.is_empty() {
        return None;
    }
    let mut acc = conjuncts.remove(0);
    for c in conjuncts {
        acc = PlanExpr::BinaryOp {
            left: Box::new(acc),
            op: BinOp::And,
            right: Box::new(c),
        };
    }
    Some(acc)
}

fn plan_create_table(ct: &ast::CreateTable, catalog: &Catalog) -> Result<Plan> {
    let table_name = ct.name.to_string();

    if let Some(query) = &ct.query {
        let query_plan = plan_select(query, catalog, &HashMap::new())?;
        return Ok(Plan::CreateTableAsSelect {
            table_name,
            if_not_exists: ct.if_not_exists,
            query: Box::new(query_plan),
        });
    }

    let mut columns = Vec::new();

    let mut table_pk_cols: Vec<String> = Vec::new();
    for constraint in &ct.constraints {
        if let ast::TableConstraint::PrimaryKey {
            columns: pk_cols, ..
        } = constraint
        {
            for col in pk_cols {
                table_pk_cols.push(col.value.to_lowercase());
            }
        }
    }

    for col in &ct.columns {
        let type_name = col.data_type.to_string();
        let is_pk_inline = col.options.iter().any(|opt| {
            matches!(
                opt.option,
                ast::ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            )
        });
        let is_pk_from_table = table_pk_cols.contains(&col.name.value.to_lowercase());
        let is_primary_key = is_pk_inline || is_pk_from_table;

        let not_null = col
            .options
            .iter()
            .any(|opt| matches!(opt.option, ast::ColumnOption::NotNull))
            || is_primary_key;

        columns.push(CreateColumnDef {
            name: col.name.value.clone(),
            type_name,
            is_primary_key,
            not_null,
        });
    }

    let sql = format!("{ct}");

    if ct.without_rowid {
        // SQLite requires WITHOUT ROWID tables to declare a PRIMARY KEY
        // (composite or single-column). Reject the missing-PK case at
        // plan time so the error is precise instead of surfacing later.
        let has_any_pk = !table_pk_cols.is_empty()
            || columns.iter().any(|c| c.is_primary_key);
        if !has_any_pk {
            return Err(Error::Other(format!(
                "table {table_name} declared WITHOUT ROWID but has no PRIMARY KEY"
            )));
        }
    }

    Ok(Plan::CreateTable(CreateTablePlan {
        table_name,
        sql,
        columns,
        if_not_exists: ct.if_not_exists,
        without_rowid: ct.without_rowid,
    }))
}

/// True when every And-conjunct of `b` appears (structurally) as a top-level
/// conjunct of `a`. A small, conservative implication checker — covers the
/// common partial-index case where the user spells out the index's WHERE in
/// their query.
fn query_predicate_implies_index_predicate(
    query_predicate: &PlanExpr,
    index_predicate_src: &str,
    columns: &[ColumnRef],
    catalog: &Catalog,
) -> bool {
    // Re-parse the index's stored predicate string into a PlanExpr.
    let parsed = match rsqlite_parser::parse::parse_sql(&format!(
        "SELECT 1 WHERE {index_predicate_src}"
    )) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let expr_ast = parsed.into_iter().next().and_then(|stmt| {
        if let sqlparser::ast::Statement::Query(q) = stmt {
            if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                return sel.selection;
            }
        }
        None
    });
    let Some(expr_ast) = expr_ast else {
        return false;
    };
    let Ok(idx_predicate) = plan_expr(&expr_ast, columns, catalog) else {
        return false;
    };

    let query_conjuncts = collect_and_conjuncts(query_predicate);
    let idx_conjuncts = collect_and_conjuncts(&idx_predicate);

    // Each conjunct of the index's WHERE must be implied by the query's WHERE.
    // For each idx conjunct, look for a query conjunct that semantically
    // implies it — verbatim equality is the easiest case, then range
    // tightening / equality-into-range / IN-list subsetting.
    idx_conjuncts.iter().all(|ip| {
        query_conjuncts
            .iter()
            .any(|qp| query_conjunct_implies(qp, ip))
    })
}

/// Does the query conjunct `qp` imply the index conjunct `ip`?
///
/// The recursion handles structural equality first (the cheap case shipped
/// originally), then specific comparison-to-comparison shapes for ranges,
/// equality-into-range, and IN-list subsetting. Returns false for anything
/// not explicitly recognized — partial-index matching must err on the side
/// of *not* picking the index when implication is unclear.
fn query_conjunct_implies(qp: &PlanExpr, ip: &PlanExpr) -> bool {
    if plan_exprs_structurally_equal(qp, ip) {
        return true;
    }
    if let (
        PlanExpr::BinaryOp { left: ql, op: qo, right: qr },
        PlanExpr::BinaryOp { left: il, op: io, right: ir },
    ) = (qp, ip)
    {
        // The two predicates must be talking about the same column.
        // Allow either orientation: `col op lit` and `lit op col`.
        if let Some((q_op, q_lit)) = column_compared_to_literal(ql, qo, qr) {
            if let Some((i_op, i_lit)) = column_compared_to_literal(il, io, ir) {
                if matches!(qp_column_name(ql, qr), Some(qn)
                    if matches!(qp_column_name(il, ir), Some(in_) if qn.eq_ignore_ascii_case(&in_)))
                {
                    return comparison_implies(q_op, &q_lit, i_op, &i_lit);
                }
            }
        }
        // IN-list subset check: qp = `col IN (a, b)`, ip = `col IS NOT NULL`.
        // Handled below.
    }
    if let (PlanExpr::InList { expr: q_e, list: q_list, negated: false }, _) = (qp, ip) {
        if let PlanExpr::IsNotNull(i_e) = ip {
            if plan_exprs_structurally_equal(q_e, i_e) && !q_list.is_empty() {
                let all_non_null = q_list
                    .iter()
                    .all(|e| !matches!(e, PlanExpr::Literal(LiteralValue::Null)));
                if all_non_null {
                    return true;
                }
            }
        }
        if let PlanExpr::InList { expr: i_e, list: i_list, negated: false } = ip {
            // q's list must be a subset of i's list. Conservative literal-equality.
            if plan_exprs_structurally_equal(q_e, i_e) {
                return q_list.iter().all(|q_item| {
                    i_list
                        .iter()
                        .any(|i_item| plan_exprs_structurally_equal(q_item, i_item))
                });
            }
        }
    }
    false
}

/// If `(left op right)` is `<column> <op> <literal>` (or its mirror),
/// return the operator (oriented as column-on-the-left) plus the literal.
fn column_compared_to_literal<'a>(
    left: &'a PlanExpr,
    op: &BinOp,
    right: &'a PlanExpr,
) -> Option<(BinOp, &'a LiteralValue)> {
    match (left, right) {
        (PlanExpr::Column(_), PlanExpr::Literal(lit)) => Some((*op, lit)),
        (PlanExpr::Literal(lit), PlanExpr::Column(_)) => Some((flip_comparison(*op), lit)),
        _ => None,
    }
}

fn qp_column_name(left: &PlanExpr, right: &PlanExpr) -> Option<String> {
    if let PlanExpr::Column(c) = left {
        return Some(c.name.clone());
    }
    if let PlanExpr::Column(c) = right {
        return Some(c.name.clone());
    }
    None
}

/// Re-orient a comparison when its operands are swapped: `5 > col` ↔
/// `col < 5`.
fn flip_comparison(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::LtEq => BinOp::GtEq,
        BinOp::Gt => BinOp::Lt,
        BinOp::GtEq => BinOp::LtEq,
        // Symmetric ops stay the same.
        other => other,
    }
}

/// Does `(col q_op q_lit)` imply `(col i_op i_lit)` for the same column?
///
/// All `BinOp` variants reaching here are comparisons (Eq/NotEq/Lt/LtEq/Gt/GtEq).
fn comparison_implies(q_op: BinOp, q_lit: &LiteralValue, i_op: BinOp, i_lit: &LiteralValue) -> bool {
    let cmp = match compare_literal(q_lit, i_lit) {
        Some(c) => c,
        None => return false,
    };
    use std::cmp::Ordering;
    match (q_op, i_op) {
        // Equality implies any range that admits the value.
        (BinOp::Eq, BinOp::Eq) => cmp == Ordering::Equal,
        (BinOp::Eq, BinOp::Lt) => cmp == Ordering::Less,
        (BinOp::Eq, BinOp::LtEq) => cmp != Ordering::Greater,
        (BinOp::Eq, BinOp::Gt) => cmp == Ordering::Greater,
        (BinOp::Eq, BinOp::GtEq) => cmp != Ordering::Less,
        // Range tightening (same direction).
        (BinOp::Gt, BinOp::Gt) | (BinOp::GtEq, BinOp::Gt) => cmp == Ordering::Greater,
        (BinOp::Gt, BinOp::GtEq) | (BinOp::GtEq, BinOp::GtEq) => cmp != Ordering::Less,
        (BinOp::Lt, BinOp::Lt) | (BinOp::LtEq, BinOp::Lt) => cmp == Ordering::Less,
        (BinOp::Lt, BinOp::LtEq) | (BinOp::LtEq, BinOp::LtEq) => cmp != Ordering::Greater,
        // Mixed directions / NotEq don't imply cleanly.
        _ => false,
    }
}

fn compare_literal(a: &LiteralValue, b: &LiteralValue) -> Option<std::cmp::Ordering> {
    use LiteralValue::*;
    match (a, b) {
        (Integer(x), Integer(y)) => Some(x.cmp(y)),
        (Real(x), Real(y)) => x.partial_cmp(y),
        (Integer(x), Real(y)) => (*x as f64).partial_cmp(y),
        (Real(x), Integer(y)) => x.partial_cmp(&(*y as f64)),
        (Text(x), Text(y)) => Some(x.cmp(y)),
        (Bool(x), Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Flatten a nested `And` tree into a list of leaf conjuncts. Non-And nodes
/// are returned as a single-element list.
fn collect_and_conjuncts(expr: &PlanExpr) -> Vec<&PlanExpr> {
    fn walk<'a>(e: &'a PlanExpr, out: &mut Vec<&'a PlanExpr>) {
        if let PlanExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } = e
        {
            walk(left, out);
            walk(right, out);
        } else {
            out.push(e);
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// Conservative structural equality on PlanExpr — covers the cases we need
/// for partial-index and expression-index matching (Column, Literal,
/// BinaryOp, UnaryOp, IsNull, IsNotNull, Function, Cast). Anything else
/// returns false rather than risk a false match.
fn plan_exprs_structurally_equal(a: &PlanExpr, b: &PlanExpr) -> bool {
    match (a, b) {
        (PlanExpr::Column(ca), PlanExpr::Column(cb)) => {
            ca.name.eq_ignore_ascii_case(&cb.name)
                && ca.table.as_deref().map(|s| s.to_ascii_lowercase())
                    == cb.table.as_deref().map(|s| s.to_ascii_lowercase())
        }
        (PlanExpr::Rowid, PlanExpr::Rowid) => true,
        (PlanExpr::Literal(la), PlanExpr::Literal(lb)) => la == lb,
        (PlanExpr::BinaryOp { left: la, op: oa, right: ra },
         PlanExpr::BinaryOp { left: lb, op: ob, right: rb }) => {
            oa == ob
                && plan_exprs_structurally_equal(la, lb)
                && plan_exprs_structurally_equal(ra, rb)
        }
        (PlanExpr::UnaryOp { op: oa, operand: aa },
         PlanExpr::UnaryOp { op: ob, operand: bb }) => {
            oa == ob && plan_exprs_structurally_equal(aa, bb)
        }
        (PlanExpr::IsNull(aa), PlanExpr::IsNull(bb))
        | (PlanExpr::IsNotNull(aa), PlanExpr::IsNotNull(bb)) => {
            plan_exprs_structurally_equal(aa, bb)
        }
        (PlanExpr::Function { name: na, args: aa },
         PlanExpr::Function { name: nb, args: ab }) => {
            na.eq_ignore_ascii_case(nb)
                && aa.len() == ab.len()
                && aa.iter()
                    .zip(ab.iter())
                    .all(|(x, y)| plan_exprs_structurally_equal(x, y))
        }
        (PlanExpr::Cast { expr: ea, type_name: ta },
         PlanExpr::Cast { expr: eb, type_name: tb }) => {
            ta.eq_ignore_ascii_case(tb) && plan_exprs_structurally_equal(ea, eb)
        }
        _ => false,
    }
}

/// Estimated rows touched per equality lookup using the index named
/// `idx_name`'s leading `prefix_len` columns. Read from
/// `catalog.index_stats` (populated by ANALYZE); returns `i64::MAX`
/// when no stats are available so the planner falls back to its old
/// first-match-wins behavior.
fn index_lookup_cost(catalog: &Catalog, idx_name: &str, prefix_len: usize) -> i64 {
    let stat = match catalog.index_stats.get(&idx_name.to_lowercase()) {
        Some(s) => s,
        None => return i64::MAX,
    };
    if prefix_len == 0 {
        return stat.row_count.max(1);
    }
    let idx = prefix_len.min(stat.avg_per_prefix.len()).saturating_sub(1);
    stat.avg_per_prefix
        .get(idx)
        .copied()
        .unwrap_or(stat.row_count.max(1))
}

/// Walk `expr` and return true if any embedded Column refers to a name
/// that's an alias from `output_names` but does not exist as a real
/// underlying column in `table_columns`. Used to decide whether a Sort
/// must run after Project (so the alias is resolvable) or can run
/// before Project (so non-projected scan columns are reachable).
fn plan_expr_references_alias_only(
    expr: &PlanExpr,
    output_names: &[String],
    table_columns: &[ColumnRef],
) -> bool {
    match expr {
        PlanExpr::Column(c) => {
            output_names.iter().any(|n| n.eq_ignore_ascii_case(&c.name))
                && !table_columns
                    .iter()
                    .any(|tc| tc.name.eq_ignore_ascii_case(&c.name))
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            plan_expr_references_alias_only(left, output_names, table_columns)
                || plan_expr_references_alias_only(right, output_names, table_columns)
        }
        PlanExpr::UnaryOp { operand, .. } => {
            plan_expr_references_alias_only(operand, output_names, table_columns)
        }
        PlanExpr::Function { args, .. } => args
            .iter()
            .any(|a| plan_expr_references_alias_only(a, output_names, table_columns)),
        PlanExpr::Cast { expr, .. } => {
            plan_expr_references_alias_only(expr, output_names, table_columns)
        }
        PlanExpr::IsNull(e) | PlanExpr::IsNotNull(e) | PlanExpr::Collate { expr: e, .. } => {
            plan_expr_references_alias_only(e, output_names, table_columns)
        }
        _ => false,
    }
}

fn try_index_scan(
    current_plan: &Plan,
    predicate: &PlanExpr,
    _all_columns: &[ColumnRef],
    catalog: &Catalog,
) -> Option<Plan> {
    let (table_name, table_root, columns) = match current_plan {
        Plan::Scan {
            table,
            root_page,
            columns,
        } => (table, *root_page, columns),
        _ => return None,
    };

    let eq_parts = extract_equality_parts(predicate);
    let eq_parts_ref: &[_] = eq_parts.as_deref().unwrap_or(&[]);

    // Collect all equality-matching candidate indexes, then pick the most
    // selective one according to ANALYZE's per-index avg_per_first_col.
    // Without stats every candidate scores i64::MAX and the iteration
    // order of the catalog HashMap decides — same as the previous
    // first-match behavior.
    let mut candidates: Vec<(i64, Plan)> = Vec::new();
    for idx_def in catalog.indexes.values() {
        if !idx_def.table_name.eq_ignore_ascii_case(table_name) {
            continue;
        }
        if idx_def.columns.is_empty() {
            continue;
        }
        // Partial indexes: only usable when the query predicate implies the
        // index predicate. We accept the (common) case where the index's
        // predicate appears verbatim as a top-level conjunct of the query
        // WHERE — anything fancier still falls through to a full scan.
        if let Some(idx_pred_src) = idx_def.predicate.as_deref() {
            if !query_predicate_implies_index_predicate(predicate, idx_pred_src, _all_columns, catalog) {
                continue;
            }
        }

        if eq_parts_ref.len() >= idx_def.columns.len() {
            let mut lookup_values = Vec::new();
            let mut all_matched = true;

            for idx_col in &idx_def.columns {
                let found = eq_parts_ref
                    .iter()
                    .find(|(col_name, _)| col_name.eq_ignore_ascii_case(idx_col));
                match found {
                    Some((_, val_expr)) => lookup_values.push(val_expr.clone()),
                    None => {
                        all_matched = false;
                        break;
                    }
                }
            }

            if all_matched {
                let remaining_predicate = build_remaining_predicate(predicate, &idx_def.columns);

                let index_scan = Plan::IndexScan {
                    table: table_name.clone(),
                    table_root_page: table_root,
                    index_root_page: idx_def.root_page,
                    index_name: idx_def.name.clone(),
                    columns: columns.clone(),
                    index_columns: idx_def.columns.clone(),
                    lookup_values,
                };

                let plan = if let Some(remaining) = remaining_predicate {
                    Plan::Filter {
                        input: Box::new(index_scan),
                        predicate: remaining,
                    }
                } else {
                    index_scan
                };
                let cost = index_lookup_cost(catalog, &idx_def.name, idx_def.columns.len());
                candidates.push((cost, plan));
            }
        }
    }
    if !candidates.is_empty() {
        candidates.sort_by_key(|(cost, _)| *cost);
        return Some(candidates.into_iter().next().unwrap().1);
    }

    // Try expression-index lookup. Single-column expression indexes only
    // for now; matches `<idx_expr> = <literal>` (or its mirror) anywhere
    // in the WHERE's top-level And tree. Remaining conjuncts are wrapped
    // in a Filter — slight redundant work for the matched part, but
    // correct.
    if let Some(plan) =
        try_expression_index_scan(table_name, table_root, columns, predicate, _all_columns, catalog)
    {
        return Some(plan);
    }

    if let Some(range_plan) = try_range_scan(table_name, table_root, columns, predicate, catalog) {
        return Some(range_plan);
    }

    None
}

fn try_expression_index_scan(
    table_name: &str,
    table_root: u32,
    columns: &[ColumnRef],
    predicate: &PlanExpr,
    all_columns: &[ColumnRef],
    catalog: &Catalog,
) -> Option<Plan> {
    // Collect top-level `<anything> = <literal>` equalities from the WHERE.
    let mut general_eqs: Vec<(PlanExpr, PlanExpr)> = Vec::new();
    collect_general_equalities(predicate, &mut general_eqs);
    if general_eqs.is_empty() {
        return None;
    }

    for idx_def in catalog.indexes.values() {
        if !idx_def.table_name.eq_ignore_ascii_case(table_name) {
            continue;
        }
        if idx_def.columns.is_empty() {
            continue;
        }
        if idx_def.predicate.is_some() {
            continue;
        }
        // At least one column must be an expression (not a plain table
        // column) — otherwise the main `try_index_scan` path already
        // handles this index.
        let any_expr = idx_def.columns.iter().any(|src| {
            !all_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(src))
        });
        if !any_expr {
            continue;
        }

        // For each indexed column (expression OR plain), find a matching
        // equality conjunct in the WHERE. Plain columns can match either
        // a plain `col = ?` from `general_eqs` or be skipped onto the
        // Filter wrap; expression columns must structurally match.
        let mut lookup_values: Vec<PlanExpr> = Vec::new();
        let mut all_matched = true;
        for col_src in &idx_def.columns {
            let parsed = match rsqlite_parser::parse::parse_sql(&format!("SELECT {col_src}")) {
                Ok(p) => p,
                Err(_) => {
                    all_matched = false;
                    break;
                }
            };
            let expr_ast = parsed.into_iter().next().and_then(|stmt| {
                if let sqlparser::ast::Statement::Query(q) = stmt {
                    if let sqlparser::ast::SetExpr::Select(sel) = *q.body {
                        return sel.projection.into_iter().next().and_then(|item| {
                            if let sqlparser::ast::SelectItem::UnnamedExpr(e) = item {
                                Some(e)
                            } else {
                                None
                            }
                        });
                    }
                }
                None
            });
            let Some(expr_ast) = expr_ast else {
                all_matched = false;
                break;
            };
            let Ok(idx_expr) = plan_expr(&expr_ast, all_columns, catalog) else {
                all_matched = false;
                break;
            };
            let matched_value = general_eqs
                .iter()
                .find(|(lhs, _)| plan_exprs_structurally_equal(lhs, &idx_expr))
                .map(|(_, rhs)| rhs.clone());
            match matched_value {
                Some(v) => lookup_values.push(v),
                None => {
                    all_matched = false;
                    break;
                }
            }
        }

        if all_matched {
            {
                let index_scan = Plan::IndexScan {
                    table: table_name.to_string(),
                    table_root_page: table_root,
                    index_root_page: idx_def.root_page,
                    index_name: idx_def.name.clone(),
                    columns: columns.to_vec(),
                    index_columns: idx_def.columns.clone(),
                    lookup_values,
                };
                // Wrap with the full predicate as a Filter; the index narrows
                // the candidate set but doesn't strip already-matched
                // conjuncts (a v0.2 optimization).
                return Some(Plan::Filter {
                    input: Box::new(index_scan),
                    predicate: predicate.clone(),
                });
            }
        }
    }
    None
}

/// Collect every top-level `<expr> = <literal>` equality from the WHERE
/// (And-tree). The literal can be on either side. Used by
/// expression-index matching, where a column lookup isn't enough.
fn collect_general_equalities(expr: &PlanExpr, out: &mut Vec<(PlanExpr, PlanExpr)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_general_equalities(left, out);
            collect_general_equalities(right, out);
        }
        PlanExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if matches!(right.as_ref(), PlanExpr::Literal(_)) {
                out.push((*left.clone(), *right.clone()));
            } else if matches!(left.as_ref(), PlanExpr::Literal(_)) {
                out.push((*right.clone(), *left.clone()));
            }
        }
        _ => {}
    }
}

fn try_range_scan(
    table_name: &str,
    table_root: u32,
    columns: &[ColumnRef],
    predicate: &PlanExpr,
    catalog: &Catalog,
) -> Option<Plan> {
    let bounds = extract_range_bounds(predicate);
    if bounds.is_empty() {
        return None;
    }

    for idx_def in catalog.indexes.values() {
        if !idx_def.table_name.eq_ignore_ascii_case(table_name) {
            continue;
        }
        if idx_def.columns.len() != 1 {
            continue;
        }
        // Same caveat as try_index_scan: skip partial indexes.
        if idx_def.predicate.is_some() {
            continue;
        }

        let idx_col = &idx_def.columns[0];
        let col_bounds: Vec<_> = bounds
            .iter()
            .filter(|(col, _, _, _)| col.eq_ignore_ascii_case(idx_col))
            .collect();

        if col_bounds.is_empty() {
            continue;
        }

        let mut lower: Option<(PlanExpr, bool)> = None;
        let mut upper: Option<(PlanExpr, bool)> = None;

        for (_, val, is_lower, inclusive) in &col_bounds {
            if *is_lower {
                lower = Some((val.clone(), *inclusive));
            } else {
                upper = Some((val.clone(), *inclusive));
            }
        }

        let remaining = build_remaining_range_predicate(predicate, idx_col);

        let range_scan = Plan::IndexRangeScan {
            table: table_name.to_string(),
            table_root_page: table_root,
            index_root_page: idx_def.root_page,
            index_name: idx_def.name.clone(),
            columns: columns.to_vec(),
            index_column: idx_col.clone(),
            lower_bound: lower,
            upper_bound: upper,
        };

        return if let Some(remaining) = remaining {
            Some(Plan::Filter {
                input: Box::new(range_scan),
                predicate: remaining,
            })
        } else {
            Some(range_scan)
        };
    }

    None
}

fn extract_range_bounds(predicate: &PlanExpr) -> Vec<(String, PlanExpr, bool, bool)> {
    let mut bounds = Vec::new();
    collect_range_parts(predicate, &mut bounds);
    bounds
}

fn collect_range_parts(expr: &PlanExpr, out: &mut Vec<(String, PlanExpr, bool, bool)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_range_parts(left, out);
            collect_range_parts(right, out);
        }
        PlanExpr::BinaryOp { left, op, right } => {
            let bound = if let PlanExpr::Column(col) = left.as_ref() {
                if matches!(right.as_ref(), PlanExpr::Column(_)) {
                    None
                } else {
                    match op {
                        BinOp::Gt => Some((col.name.clone(), *right.clone(), true, false)),
                        BinOp::GtEq => Some((col.name.clone(), *right.clone(), true, true)),
                        BinOp::Lt => Some((col.name.clone(), *right.clone(), false, false)),
                        BinOp::LtEq => Some((col.name.clone(), *right.clone(), false, true)),
                        _ => None,
                    }
                }
            } else if let PlanExpr::Column(col) = right.as_ref() {
                if matches!(left.as_ref(), PlanExpr::Column(_)) {
                    None
                } else {
                    match op {
                        BinOp::Gt => Some((col.name.clone(), *left.clone(), false, false)),
                        BinOp::GtEq => Some((col.name.clone(), *left.clone(), false, true)),
                        BinOp::Lt => Some((col.name.clone(), *left.clone(), true, false)),
                        BinOp::LtEq => Some((col.name.clone(), *left.clone(), true, true)),
                        _ => None,
                    }
                }
            } else {
                None
            };

            if let Some(b) = bound {
                out.push(b);
            }
        }
        _ => {}
    }
}

fn build_remaining_range_predicate(predicate: &PlanExpr, index_column: &str) -> Option<PlanExpr> {
    match predicate {
        PlanExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let l = build_remaining_range_predicate(left, index_column);
            let r = build_remaining_range_predicate(right, index_column);
            match (l, r) {
                (Some(l), Some(r)) => Some(PlanExpr::BinaryOp {
                    left: Box::new(l),
                    op: BinOp::And,
                    right: Box::new(r),
                }),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        PlanExpr::BinaryOp { left, op, right } => {
            let is_range_on_idx = match op {
                BinOp::Gt | BinOp::GtEq | BinOp::Lt | BinOp::LtEq => {
                    if let PlanExpr::Column(col) = left.as_ref() {
                        col.name.eq_ignore_ascii_case(index_column)
                            && !matches!(right.as_ref(), PlanExpr::Column(_))
                    } else if let PlanExpr::Column(col) = right.as_ref() {
                        col.name.eq_ignore_ascii_case(index_column)
                            && !matches!(left.as_ref(), PlanExpr::Column(_))
                    } else {
                        false
                    }
                }
                _ => false,
            };
            if is_range_on_idx {
                None
            } else {
                Some(predicate.clone())
            }
        }
        _ => Some(predicate.clone()),
    }
}

fn extract_equality_parts(predicate: &PlanExpr) -> Option<Vec<(String, PlanExpr)>> {
    let mut parts = Vec::new();
    collect_and_equalities(predicate, &mut parts);
    if parts.is_empty() { None } else { Some(parts) }
}

fn collect_and_equalities(expr: &PlanExpr, out: &mut Vec<(String, PlanExpr)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_and_equalities(left, out);
            collect_and_equalities(right, out);
        }
        PlanExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if let PlanExpr::Column(col) = left.as_ref() {
                if !matches!(right.as_ref(), PlanExpr::Column(_)) {
                    out.push((col.name.clone(), *right.clone()));
                }
            } else if let PlanExpr::Column(col) = right.as_ref() {
                if !matches!(left.as_ref(), PlanExpr::Column(_)) {
                    out.push((col.name.clone(), *left.clone()));
                }
            }
        }
        _ => {}
    }
}

fn build_remaining_predicate(predicate: &PlanExpr, index_columns: &[String]) -> Option<PlanExpr> {
    match predicate {
        PlanExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left_remaining = build_remaining_predicate(left, index_columns);
            let right_remaining = build_remaining_predicate(right, index_columns);
            match (left_remaining, right_remaining) {
                (Some(l), Some(r)) => Some(PlanExpr::BinaryOp {
                    left: Box::new(l),
                    op: BinOp::And,
                    right: Box::new(r),
                }),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        PlanExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let is_index_eq = if let PlanExpr::Column(col) = left.as_ref() {
                index_columns
                    .iter()
                    .any(|ic| ic.eq_ignore_ascii_case(&col.name))
                    && !matches!(right.as_ref(), PlanExpr::Column(_))
            } else if let PlanExpr::Column(col) = right.as_ref() {
                index_columns
                    .iter()
                    .any(|ic| ic.eq_ignore_ascii_case(&col.name))
                    && !matches!(left.as_ref(), PlanExpr::Column(_))
            } else {
                false
            };
            if is_index_eq {
                None
            } else {
                Some(predicate.clone())
            }
        }
        _ => Some(predicate.clone()),
    }
}

fn plan_create_index(ci: &ast::CreateIndex) -> Result<Plan> {
    let index_name = ci.name.as_ref().map(|n| n.to_string()).unwrap_or_default();
    let table_name = ci.table_name.to_string();

    let columns: Vec<String> = ci.columns.iter().map(|c| c.expr.to_string()).collect();
    let predicate = ci.predicate.as_ref().map(|p| p.to_string());

    let sql = format!("{ci}");

    Ok(Plan::CreateIndex(CreateIndexPlan {
        index_name,
        table_name,
        columns,
        sql,
        if_not_exists: ci.if_not_exists,
        predicate,
    }))
}

fn plan_insert(insert: &ast::Insert, catalog: &Catalog) -> Result<Plan> {
    let table_name = match &insert.table {
        ast::TableObject::TableName(name) => name.to_string(),
        _ => {
            return Err(Error::Other(
                "only simple table names are supported in INSERT".to_string(),
            ));
        }
    };

    // Virtual-table INSERT: route to a separate plan variant so the
    // executor can dispatch through the module's `insert` hook.
    if let Some(vt) = catalog.virtual_tables.get(&table_name.to_lowercase()) {
        let cols = vt.instance.columns();
        let rows = match insert.source.as_deref() {
            Some(query) => match query.body.as_ref() {
                ast::SetExpr::Values(values) => values
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|e| plan_expr(e, &[], catalog))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?,
                _ => {
                    return Err(Error::Other(
                        "INSERT into virtual table only supports VALUES (...)".into(),
                    ));
                }
            },
            None => Vec::new(),
        };
        return Ok(Plan::VirtualInsert {
            table: vt.name.clone(),
            columns: cols,
            rows,
        });
    }

    let table_def = catalog
        .get_table(&table_name)
        .ok_or_else(|| Error::Other(format!("table not found: {table_name}")))?;

    let all_columns: Vec<ColumnRef> = table_def
        .columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
            table: None,
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
            is_unique: c.is_unique,
        })
        .collect();

    let target_columns = if insert.columns.is_empty() {
        None
    } else {
        Some(insert.columns.iter().map(|c| c.value.clone()).collect())
    };

    let (rows, source_query) = match insert.source.as_ref() {
        None => (vec![vec![]], None),
        Some(source) => match source.body.as_ref() {
            SetExpr::Values(values) => {
                let mut planned_rows = Vec::new();
                for row in &values.rows {
                    let mut exprs = Vec::new();
                    for expr in row {
                        exprs.push(plan_expr(expr, &all_columns, catalog)?);
                    }
                    planned_rows.push(exprs);
                }
                (planned_rows, None)
            }
            _ => {
                let query_plan = plan_select(source, catalog, &HashMap::new())?;
                (vec![], Some(Box::new(query_plan)))
            }
        },
    };

    let or_replace =
        insert.replace_into || matches!(insert.or, Some(ast::SqliteOnConflict::Replace));

    let on_conflict = if matches!(insert.or, Some(ast::SqliteOnConflict::Ignore)) {
        Some(OnConflictPlan::DoNothing)
    } else if matches!(insert.or, Some(ast::SqliteOnConflict::Abort)) {
        None
    } else {
        match &insert.on {
            Some(ast::OnInsert::OnConflict(oc)) => match &oc.action {
                ast::OnConflictAction::DoNothing => Some(OnConflictPlan::DoNothing),
                ast::OnConflictAction::DoUpdate(do_update) => {
                    let conflict_columns = match &oc.conflict_target {
                        Some(ast::ConflictTarget::Columns(cols)) => {
                            cols.iter().map(|c| c.value.clone()).collect()
                        }
                        _ => Vec::new(),
                    };
                    // Planning context: include `excluded.<col>` references
                    // alongside the table's regular columns.
                    let mut excluded_columns: Vec<ColumnRef> = all_columns
                        .iter()
                        .map(|c| ColumnRef {
                            name: c.name.clone(),
                            column_index: all_columns.len() + c.column_index,
                            is_rowid_alias: false,
                            table: Some("excluded".to_string()),
                            nullable: c.nullable,
                            is_primary_key: c.is_primary_key,
                            is_unique: c.is_unique,
                        })
                        .collect();
                    let mut planning_columns = all_columns.clone();
                    planning_columns.append(&mut excluded_columns);

                    let mut assignments = Vec::new();
                    for assign in &do_update.assignments {
                        let col_name = match &assign.target {
                            ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                            ast::AssignmentTarget::Tuple(_) => {
                                return Err(Error::Other(
                                    "tuple assignment not supported".to_string(),
                                ));
                            }
                        };
                        let expr = plan_expr(&assign.value, &planning_columns, catalog)?;
                        assignments.push((col_name, expr));
                    }
                    let where_clause = do_update
                        .selection
                        .as_ref()
                        .map(|e| plan_expr(e, &planning_columns, catalog))
                        .transpose()?;
                    Some(OnConflictPlan::DoUpdate {
                        conflict_columns,
                        assignments,
                        where_clause,
                    })
                }
            },
            _ => None,
        }
    };

    let returning = insert
        .returning
        .as_ref()
        .map(|items| plan_select_items(items, &all_columns, catalog))
        .transpose()?;

    let conflict_strategy = match insert.or {
        Some(ast::SqliteOnConflict::Rollback) => ConflictStrategy::Rollback,
        Some(ast::SqliteOnConflict::Fail) => ConflictStrategy::Fail,
        Some(ast::SqliteOnConflict::Ignore) => ConflictStrategy::Ignore,
        // Replace is handled via or_replace + on_conflict; treat as Abort
        // for the strategy field (won't fire because conflicts get handled
        // before strategy applies).
        Some(ast::SqliteOnConflict::Replace) | Some(ast::SqliteOnConflict::Abort) | None => {
            ConflictStrategy::Abort
        }
    };

    Ok(Plan::Insert(InsertPlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        target_columns,
        rows,
        source_query,
        on_conflict,
        or_replace,
        returning,
        conflict_strategy,
    }))
}

fn plan_update(
    table: &ast::TableWithJoins,
    assignments: &[ast::Assignment],
    selection: Option<&Expr>,
    returning: Option<&[ast::SelectItem]>,
    from: Option<&ast::UpdateTableFromKind>,
    catalog: &Catalog,
) -> Result<Plan> {
    let table_name = match &table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(Error::Other(
                "only simple table references are supported in UPDATE".to_string(),
            ));
        }
    };

    let table_def = catalog
        .get_table(&table_name)
        .ok_or_else(|| Error::Other(format!("table not found: {table_name}")))?;

    let all_columns: Vec<ColumnRef> = table_def
        .columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
            table: Some(table_name.clone()),
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
            is_unique: c.is_unique,
        })
        .collect();

    // Optional FROM clause: parse the first table in the joins list. Only a
    // single FROM table is supported; assignments and the WHERE predicate
    // see the combined column context.
    let from_plan: Option<UpdateFromPlan> = match from {
        None => None,
        Some(ast::UpdateTableFromKind::BeforeSet(t))
        | Some(ast::UpdateTableFromKind::AfterSet(t)) => {
            let first = t
                .first()
                .ok_or_else(|| Error::Other("UPDATE FROM requires a table".to_string()))?;
            let from_table_name = match &first.relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => {
                    return Err(Error::Other(
                        "only simple table references are supported in UPDATE FROM".to_string(),
                    ));
                }
            };
            let from_def = catalog
                .get_table(&from_table_name)
                .ok_or_else(|| Error::Other(format!("table not found: {from_table_name}")))?;
            let from_columns: Vec<ColumnRef> = from_def
                .columns
                .iter()
                .map(|c| ColumnRef {
                    name: c.name.clone(),
                    column_index: all_columns.len() + c.column_index,
                    is_rowid_alias: c.is_rowid_alias,
                    table: Some(from_table_name.clone()),
                    nullable: c.nullable,
                    is_primary_key: c.is_primary_key,
                    is_unique: c.is_unique,
                })
                .collect();
            Some(UpdateFromPlan {
                table_name: from_table_name,
                root_page: from_def.root_page,
                columns: from_columns,
            })
        }
    };

    let mut combined_columns = all_columns.clone();
    if let Some(fp) = &from_plan {
        combined_columns.extend(fp.columns.iter().cloned());
    }

    let mut planned_assignments = Vec::new();
    for assignment in assignments {
        let col_name = match &assignment.target {
            ast::AssignmentTarget::ColumnName(name) => name.to_string(),
            ast::AssignmentTarget::Tuple(_) => {
                return Err(Error::Other("tuple assignment not supported".to_string()));
            }
        };
        let expr = plan_expr(&assignment.value, &combined_columns, catalog)?;
        planned_assignments.push((col_name, expr));
    }

    let predicate = selection
        .map(|s| plan_expr(s, &combined_columns, catalog))
        .transpose()?;

    let returning_planned = returning
        .map(|items| plan_select_items(items, &all_columns, catalog))
        .transpose()?;

    Ok(Plan::Update(UpdatePlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        assignments: planned_assignments,
        predicate,
        returning: returning_planned,
        from: from_plan,
    }))
}

fn plan_delete(delete: &ast::Delete, catalog: &Catalog) -> Result<Plan> {
    let tables = match &delete.from {
        ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => tables,
    };

    if tables.len() != 1 {
        return Err(Error::Other(
            "exactly one table in DELETE FROM is required".to_string(),
        ));
    }

    let table_name = match &tables[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(Error::Other(
                "only simple table references are supported in DELETE".to_string(),
            ));
        }
    };

    let table_def = catalog
        .get_table(&table_name)
        .ok_or_else(|| Error::Other(format!("table not found: {table_name}")))?;

    let all_columns: Vec<ColumnRef> = table_def
        .columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
            table: None,
            nullable: c.nullable,
            is_primary_key: c.is_primary_key,
            is_unique: c.is_unique,
        })
        .collect();

    let predicate = delete
        .selection
        .as_ref()
        .map(|s| plan_expr(s, &all_columns, catalog))
        .transpose()?;

    let returning = delete
        .returning
        .as_ref()
        .map(|items| plan_select_items(items, &all_columns, catalog))
        .transpose()?;

    let order_by = delete
        .order_by
        .iter()
        .map(|ob| -> Result<SortKey> {
            let expr = plan_expr(&ob.expr, &all_columns, catalog)?;
            Ok(SortKey {
                expr,
                descending: ob.options.asc == Some(false),
                nulls_first: ob.options.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let limit = delete
        .limit
        .as_ref()
        .map(|e| plan_limit_expr(e).map(|n| n as i64))
        .transpose()?;

    Ok(Plan::Delete(DeletePlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        predicate,
        returning,
        order_by,
        limit,
    }))
}
