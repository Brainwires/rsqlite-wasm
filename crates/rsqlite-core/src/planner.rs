use sqlparser::ast::{self, Expr, SelectItem, SetExpr, Statement, TableFactor};

use crate::catalog::Catalog;
use crate::error::{Error, Result};

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

pub fn plan_query(stmt: &Statement, catalog: &Catalog) -> Result<Plan> {
    match stmt {
        Statement::Query(query) => plan_select(query, catalog),
        _ => Err(Error::Other(format!(
            "unsupported statement type: {stmt}"
        ))),
    }
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
