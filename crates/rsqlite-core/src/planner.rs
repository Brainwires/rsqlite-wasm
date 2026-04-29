use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Statement, TableFactor};

use crate::catalog::Catalog;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub table_name: String,
    pub sql: String,
    pub columns: Vec<CreateColumnDef>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateColumnDef {
    pub name: String,
    pub type_name: String,
    pub is_primary_key: bool,
    pub not_null: bool,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table_name: String,
    pub root_page: u32,
    pub table_columns: Vec<ColumnRef>,
    pub target_columns: Option<Vec<String>>,
    pub rows: Vec<Vec<PlanExpr>>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table_name: String,
    pub root_page: u32,
    pub table_columns: Vec<ColumnRef>,
    pub assignments: Vec<(String, PlanExpr)>,
    pub predicate: Option<PlanExpr>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table_name: String,
    pub root_page: u32,
    pub table_columns: Vec<ColumnRef>,
    pub predicate: Option<PlanExpr>,
}

#[derive(Debug, Clone)]
pub enum Plan {
    Scan {
        table: String,
        root_page: u32,
        columns: Vec<ColumnRef>,
    },
    Filter {
        input: Box<Plan>,
        predicate: PlanExpr,
    },
    Project {
        input: Box<Plan>,
        outputs: Vec<ProjectionItem>,
    },
    CreateTable(CreateTablePlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub name: String,
    pub column_index: usize,
    pub is_rowid_alias: bool,
}

#[derive(Debug, Clone)]
pub struct ProjectionItem {
    pub expr: PlanExpr,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum PlanExpr {
    Column(ColumnRef),
    Rowid,
    Literal(LiteralValue),
    BinaryOp {
        left: Box<PlanExpr>,
        op: BinOp,
        right: Box<PlanExpr>,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<PlanExpr>,
    },
    IsNull(Box<PlanExpr>),
    IsNotNull(Box<PlanExpr>),
    Wildcard,
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy)]
pub enum BinOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Not,
    Neg,
}

pub fn plan_statement(stmt: &Statement, catalog: &Catalog) -> Result<Plan> {
    match stmt {
        Statement::Query(query) => plan_select(query, catalog),
        Statement::CreateTable(ct) => plan_create_table(ct),
        Statement::Insert(insert) => plan_insert(insert, catalog),
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => plan_update(table, assignments, selection.as_ref(), catalog),
        Statement::Delete(delete) => plan_delete(delete, catalog),
        Statement::StartTransaction { .. } => Ok(Plan::Begin),
        Statement::Commit { .. } => Ok(Plan::Commit),
        Statement::Rollback { .. } => Ok(Plan::Rollback),
        _ => Err(Error::Other(format!(
            "unsupported statement type: {stmt}"
        ))),
    }
}

pub fn plan_query(stmt: &Statement, catalog: &Catalog) -> Result<Plan> {
    plan_statement(stmt, catalog)
}

fn plan_select(query: &ast::Query, catalog: &Catalog) -> Result<Plan> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(Error::Other(
                "only simple SELECT is supported".to_string(),
            ))
        }
    };

    if select.from.len() != 1 {
        return Err(Error::Other(
            "exactly one table in FROM is required".to_string(),
        ));
    }

    let from = &select.from[0];
    let table_name = match &from.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(Error::Other(
                "only simple table references are supported".to_string(),
            ))
        }
    };

    let table_def = catalog.get_table(&table_name).ok_or_else(|| {
        Error::Other(format!("table not found: {table_name}"))
    })?;

    let all_columns: Vec<ColumnRef> = table_def
        .columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
        })
        .collect();

    let mut plan = Plan::Scan {
        table: table_name.clone(),
        root_page: table_def.root_page,
        columns: all_columns.clone(),
    };

    // WHERE clause -> Filter
    if let Some(selection) = &select.selection {
        let predicate = plan_expr(selection, &all_columns)?;
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }

    // SELECT list -> Project
    let outputs = plan_select_items(&select.projection, &all_columns)?;
    plan = Plan::Project {
        input: Box::new(plan),
        outputs,
    };

    Ok(plan)
}

fn plan_select_items(
    items: &[SelectItem],
    columns: &[ColumnRef],
) -> Result<Vec<ProjectionItem>> {
    let mut outputs = Vec::new();

    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let plan_expr = plan_expr(expr, columns)?;
                let alias = expr.to_string();
                outputs.push(ProjectionItem {
                    expr: plan_expr,
                    alias,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let plan_expr = plan_expr(expr, columns)?;
                outputs.push(ProjectionItem {
                    expr: plan_expr,
                    alias: alias.value.clone(),
                });
            }
            SelectItem::Wildcard(_) => {
                for col in columns {
                    outputs.push(ProjectionItem {
                        expr: PlanExpr::Column(col.clone()),
                        alias: col.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(_, _) => {
                for col in columns {
                    outputs.push(ProjectionItem {
                        expr: PlanExpr::Column(col.clone()),
                        alias: col.name.clone(),
                    });
                }
            }
        }
    }

    Ok(outputs)
}

fn plan_expr(expr: &Expr, columns: &[ColumnRef]) -> Result<PlanExpr> {
    match expr {
        Expr::Identifier(ident) => {
            let name = &ident.value;
            if name.eq_ignore_ascii_case("rowid") {
                return Ok(PlanExpr::Rowid);
            }
            let col = columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or_else(|| Error::Other(format!("unknown column: {name}")))?;
            Ok(PlanExpr::Column(col.clone()))
        }
        Expr::Value(val) => Ok(PlanExpr::Literal(plan_value(&val.value)?)),
        Expr::BinaryOp { left, op, right } => {
            let left = plan_expr(left, columns)?;
            let right = plan_expr(right, columns)?;
            let op = plan_binop(op)?;
            Ok(PlanExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        Expr::UnaryOp { op, expr } => {
            let operand = plan_expr(expr, columns)?;
            let op = match op {
                ast::UnaryOperator::Not => UnaryOp::Not,
                ast::UnaryOperator::Minus => UnaryOp::Neg,
                _ => {
                    return Err(Error::Other(format!(
                        "unsupported unary operator: {op}"
                    )))
                }
            };
            Ok(PlanExpr::UnaryOp {
                op,
                operand: Box::new(operand),
            })
        }
        Expr::IsNull(e) => {
            let inner = plan_expr(e, columns)?;
            Ok(PlanExpr::IsNull(Box::new(inner)))
        }
        Expr::IsNotNull(e) => {
            let inner = plan_expr(e, columns)?;
            Ok(PlanExpr::IsNotNull(Box::new(inner)))
        }
        Expr::Nested(e) => plan_expr(e, columns),
        _ => Err(Error::Other(format!(
            "unsupported expression: {expr}"
        ))),
    }
}

fn plan_value(val: &ast::Value) -> Result<LiteralValue> {
    match val {
        ast::Value::Null => Ok(LiteralValue::Null),
        ast::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(LiteralValue::Integer(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(LiteralValue::Real(f))
            } else {
                Err(Error::Other(format!("invalid number: {n}")))
            }
        }
        ast::Value::SingleQuotedString(s) => Ok(LiteralValue::Text(s.clone())),
        ast::Value::Boolean(b) => Ok(LiteralValue::Bool(*b)),
        _ => Err(Error::Other(format!("unsupported literal: {val}"))),
    }
}

fn plan_binop(op: &ast::BinaryOperator) -> Result<BinOp> {
    match op {
        ast::BinaryOperator::Eq => Ok(BinOp::Eq),
        ast::BinaryOperator::NotEq => Ok(BinOp::NotEq),
        ast::BinaryOperator::Lt => Ok(BinOp::Lt),
        ast::BinaryOperator::LtEq => Ok(BinOp::LtEq),
        ast::BinaryOperator::Gt => Ok(BinOp::Gt),
        ast::BinaryOperator::GtEq => Ok(BinOp::GtEq),
        ast::BinaryOperator::And => Ok(BinOp::And),
        ast::BinaryOperator::Or => Ok(BinOp::Or),
        ast::BinaryOperator::Plus => Ok(BinOp::Add),
        ast::BinaryOperator::Minus => Ok(BinOp::Sub),
        ast::BinaryOperator::Multiply => Ok(BinOp::Mul),
        ast::BinaryOperator::Divide => Ok(BinOp::Div),
        ast::BinaryOperator::Modulo => Ok(BinOp::Mod),
        _ => Err(Error::Other(format!("unsupported operator: {op}"))),
    }
}

fn plan_create_table(ct: &ast::CreateTable) -> Result<Plan> {
    let table_name = ct.name.to_string();

    let mut columns = Vec::new();

    // Collect table-level PK columns
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

    Ok(Plan::CreateTable(CreateTablePlan {
        table_name,
        sql,
        columns,
        if_not_exists: ct.if_not_exists,
    }))
}

fn plan_insert(insert: &ast::Insert, catalog: &Catalog) -> Result<Plan> {
    let table_name = match &insert.table {
        ast::TableObject::TableName(name) => name.to_string(),
        _ => {
            return Err(Error::Other(
                "only simple table names are supported in INSERT".to_string(),
            ))
        }
    };

    let table_def = catalog.get_table(&table_name).ok_or_else(|| {
        Error::Other(format!("table not found: {table_name}"))
    })?;

    let all_columns: Vec<ColumnRef> = table_def
        .columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
        })
        .collect();

    let target_columns = if insert.columns.is_empty() {
        None
    } else {
        Some(
            insert
                .columns
                .iter()
                .map(|c| c.value.clone())
                .collect(),
        )
    };

    let source = insert.source.as_ref().ok_or_else(|| {
        Error::Other("INSERT requires VALUES".to_string())
    })?;

    let rows = match source.body.as_ref() {
        SetExpr::Values(values) => {
            let mut planned_rows = Vec::new();
            for row in &values.rows {
                let mut exprs = Vec::new();
                for expr in row {
                    exprs.push(plan_expr(expr, &all_columns)?);
                }
                planned_rows.push(exprs);
            }
            planned_rows
        }
        _ => {
            return Err(Error::Other(
                "only INSERT ... VALUES is supported".to_string(),
            ))
        }
    };

    Ok(Plan::Insert(InsertPlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        target_columns,
        rows,
    }))
}

fn plan_update(
    table: &ast::TableWithJoins,
    assignments: &[ast::Assignment],
    selection: Option<&Expr>,
    catalog: &Catalog,
) -> Result<Plan> {
    let table_name = match &table.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => {
            return Err(Error::Other(
                "only simple table references are supported in UPDATE".to_string(),
            ))
        }
    };

    let table_def = catalog.get_table(&table_name).ok_or_else(|| {
        Error::Other(format!("table not found: {table_name}"))
    })?;

    let all_columns: Vec<ColumnRef> = table_def
        .columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
        })
        .collect();

    let mut planned_assignments = Vec::new();
    for assignment in assignments {
        let col_name = match &assignment.target {
            ast::AssignmentTarget::ColumnName(name) => name.to_string(),
            ast::AssignmentTarget::Tuple(_) => {
                return Err(Error::Other(
                    "tuple assignment not supported".to_string(),
                ))
            }
        };
        let expr = plan_expr(&assignment.value, &all_columns)?;
        planned_assignments.push((col_name, expr));
    }

    let predicate = selection
        .map(|s| plan_expr(s, &all_columns))
        .transpose()?;

    Ok(Plan::Update(UpdatePlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        assignments: planned_assignments,
        predicate,
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
            ))
        }
    };

    let table_def = catalog.get_table(&table_name).ok_or_else(|| {
        Error::Other(format!("table not found: {table_name}"))
    })?;

    let all_columns: Vec<ColumnRef> = table_def
        .columns
        .iter()
        .map(|c| ColumnRef {
            name: c.name.clone(),
            column_index: c.column_index,
            is_rowid_alias: c.is_rowid_alias,
        })
        .collect();

    let predicate = delete
        .selection
        .as_ref()
        .map(|s| plan_expr(s, &all_columns))
        .transpose()?;

    Ok(Plan::Delete(DeletePlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        predicate,
    }))
}
