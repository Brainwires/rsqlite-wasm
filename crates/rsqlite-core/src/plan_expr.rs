use sqlparser::ast::{self, Expr, SelectItem};

use crate::catalog::Catalog;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub name: String,
    pub column_index: usize,
    pub is_rowid_alias: bool,
    pub table: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProjectionItem {
    pub expr: PlanExpr,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
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
    Aggregate {
        func: AggFunc,
        arg: Box<PlanExpr>,
        distinct: bool,
    },
    Function {
        name: String,
        args: Vec<PlanExpr>,
    },
    Like {
        expr: Box<PlanExpr>,
        pattern: Box<PlanExpr>,
        negated: bool,
    },
    InList {
        expr: Box<PlanExpr>,
        list: Vec<PlanExpr>,
        negated: bool,
    },
    Case {
        operand: Option<Box<PlanExpr>>,
        when_clauses: Vec<(PlanExpr, PlanExpr)>,
        else_result: Option<Box<PlanExpr>>,
    },
    Cast {
        expr: Box<PlanExpr>,
        type_name: String,
    },
    Subquery(Box<super::Plan>),
    InSubquery {
        expr: Box<PlanExpr>,
        subquery: Box<super::Plan>,
        negated: bool,
    },
    Exists {
        subquery: Box<super::Plan>,
        negated: bool,
    },
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
    Concat,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Not,
    Neg,
}

pub(super) fn plan_select_items(
    items: &[SelectItem],
    columns: &[ColumnRef],
    catalog: &Catalog,
) -> Result<Vec<ProjectionItem>> {
    let mut outputs = Vec::new();

    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let planned = plan_expr(expr, columns, catalog)?;
                let alias = match expr {
                    Expr::CompoundIdentifier(parts) if !parts.is_empty() => {
                        parts.last().unwrap().value.clone()
                    }
                    _ => expr.to_string(),
                };
                outputs.push(ProjectionItem {
                    expr: planned,
                    alias,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let planned = plan_expr(expr, columns, catalog)?;
                outputs.push(ProjectionItem {
                    expr: planned,
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

pub(super) fn plan_expr(expr: &Expr, columns: &[ColumnRef], catalog: &Catalog) -> Result<PlanExpr> {
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
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let table = &parts[0].value;
            let col_name = &parts[1].value;
            let col = columns
                .iter()
                .find(|c| {
                    c.name.eq_ignore_ascii_case(col_name)
                        && c.table
                            .as_ref()
                            .is_some_and(|t| t.eq_ignore_ascii_case(table))
                })
                .or_else(|| {
                    columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(col_name))
                })
                .ok_or_else(|| {
                    Error::Other(format!("unknown column: {table}.{col_name}"))
                })?;
            Ok(PlanExpr::Column(col.clone()))
        }
        Expr::Value(val) => Ok(PlanExpr::Literal(plan_value(&val.value)?)),
        Expr::BinaryOp { left, op, right } => {
            let left = plan_expr(left, columns, catalog)?;
            let right = plan_expr(right, columns, catalog)?;
            let op = plan_binop(op)?;
            Ok(PlanExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        Expr::UnaryOp { op, expr } => {
            let operand = plan_expr(expr, columns, catalog)?;
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
            let inner = plan_expr(e, columns, catalog)?;
            Ok(PlanExpr::IsNull(Box::new(inner)))
        }
        Expr::IsNotNull(e) => {
            let inner = plan_expr(e, columns, catalog)?;
            Ok(PlanExpr::IsNotNull(Box::new(inner)))
        }
        Expr::Nested(e) => plan_expr(e, columns, catalog),
        Expr::Function(func) => plan_function_expr(func, columns, catalog),
        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            ..
        } => {
            let inner = plan_expr(expr, columns, catalog)?;
            let func_name = match trim_where {
                Some(ast::TrimWhereField::Leading) => "LTRIM",
                Some(ast::TrimWhereField::Trailing) => "RTRIM",
                _ => "TRIM",
            };
            let mut args = vec![inner];
            if let Some(what) = trim_what {
                args.push(plan_expr(what, columns, catalog)?);
            }
            Ok(PlanExpr::Function {
                name: func_name.to_string(),
                args,
            })
        }
        Expr::Like {
            negated,
            expr: like_expr,
            pattern,
            ..
        } => {
            let e = plan_expr(like_expr, columns, catalog)?;
            let p = plan_expr(pattern, columns, catalog)?;
            Ok(PlanExpr::Like {
                expr: Box::new(e),
                pattern: Box::new(p),
                negated: *negated,
            })
        }
        Expr::ILike {
            negated,
            expr: like_expr,
            pattern,
            ..
        } => {
            let e = plan_expr(like_expr, columns, catalog)?;
            let p = plan_expr(pattern, columns, catalog)?;
            Ok(PlanExpr::Like {
                expr: Box::new(e),
                pattern: Box::new(p),
                negated: *negated,
            })
        }
        Expr::Between {
            expr: between_expr,
            negated,
            low,
            high,
        } => {
            let e = plan_expr(between_expr, columns, catalog)?;
            let lo = plan_expr(low, columns, catalog)?;
            let hi = plan_expr(high, columns, catalog)?;
            let gte = PlanExpr::BinaryOp {
                left: Box::new(e.clone()),
                op: BinOp::GtEq,
                right: Box::new(lo),
            };
            let lte = PlanExpr::BinaryOp {
                left: Box::new(e),
                op: BinOp::LtEq,
                right: Box::new(hi),
            };
            let combined = PlanExpr::BinaryOp {
                left: Box::new(gte),
                op: BinOp::And,
                right: Box::new(lte),
            };
            if *negated {
                Ok(PlanExpr::UnaryOp {
                    op: UnaryOp::Not,
                    operand: Box::new(combined),
                })
            } else {
                Ok(combined)
            }
        }
        Expr::InList {
            expr: in_expr,
            list,
            negated,
        } => {
            let e = plan_expr(in_expr, columns, catalog)?;
            let items = list
                .iter()
                .map(|item| plan_expr(item, columns, catalog))
                .collect::<Result<Vec<_>>>()?;
            Ok(PlanExpr::InList {
                expr: Box::new(e),
                list: items,
                negated: *negated,
            })
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            let op = operand
                .as_ref()
                .map(|e| plan_expr(e, columns, catalog))
                .transpose()?;
            let when_clauses = conditions
                .iter()
                .map(|cw| {
                    let cond = plan_expr(&cw.condition, columns, catalog)?;
                    let result = plan_expr(&cw.result, columns, catalog)?;
                    Ok((cond, result))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_r = else_result
                .as_ref()
                .map(|e| plan_expr(e, columns, catalog))
                .transpose()?;
            Ok(PlanExpr::Case {
                operand: op.map(Box::new),
                when_clauses,
                else_result: else_r.map(Box::new),
            })
        }
        Expr::Cast {
            expr: cast_expr,
            data_type,
            ..
        } => {
            let e = plan_expr(cast_expr, columns, catalog)?;
            let type_name = data_type.to_string().to_uppercase();
            Ok(PlanExpr::Cast {
                expr: Box::new(e),
                type_name,
            })
        }
        Expr::InSubquery {
            expr: in_expr,
            subquery,
            negated,
        } => {
            let e = plan_expr(in_expr, columns, catalog)?;
            let sub_plan = super::plan_select(subquery, catalog)?;
            Ok(PlanExpr::InSubquery {
                expr: Box::new(e),
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::Subquery(query) => {
            let sub_plan = super::plan_select(query, catalog)?;
            Ok(PlanExpr::Subquery(Box::new(sub_plan)))
        }
        Expr::Exists { subquery, negated } => {
            let sub_plan = super::plan_select(subquery, catalog)?;
            Ok(PlanExpr::Exists {
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        _ => Err(Error::Other(format!(
            "unsupported expression: {expr}"
        ))),
    }
}

fn plan_function_expr(func: &ast::Function, columns: &[ColumnRef], catalog: &Catalog) -> Result<PlanExpr> {
    let name = func.name.to_string().to_uppercase();

    let agg_func = match name.as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        _ => None,
    };

    if let Some(func_type) = agg_func {
        let (arg, distinct) = match &func.args {
            ast::FunctionArguments::List(list) => {
                let distinct = list.duplicate_treatment
                    == Some(ast::DuplicateTreatment::Distinct);
                if list.args.is_empty() {
                    (PlanExpr::Wildcard, distinct)
                } else {
                    match &list.args[0] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                            (PlanExpr::Wildcard, distinct)
                        }
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            (plan_expr(e, columns, catalog)?, distinct)
                        }
                        _ => {
                            return Err(Error::Other(format!(
                                "unsupported aggregate argument: {}",
                                func
                            )))
                        }
                    }
                }
            }
            ast::FunctionArguments::None => (PlanExpr::Wildcard, false),
            _ => {
                return Err(Error::Other(format!(
                    "unsupported aggregate arguments: {}",
                    func
                )))
            }
        };

        return Ok(PlanExpr::Aggregate {
            func: func_type,
            arg: Box::new(arg),
            distinct,
        });
    }

    let scalar_args = match &func.args {
        ast::FunctionArguments::List(list) => {
            let mut args = Vec::new();
            for a in &list.args {
                match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                        args.push(plan_expr(e, columns, catalog)?);
                    }
                    _ => {
                        return Err(Error::Other(format!(
                            "unsupported function argument: {func}"
                        )));
                    }
                }
            }
            args
        }
        ast::FunctionArguments::None => Vec::new(),
        _ => {
            return Err(Error::Other(format!(
                "unsupported function arguments: {func}"
            )));
        }
    };

    static KNOWN_SCALARS: &[&str] = &[
        "LENGTH", "SUBSTR", "SUBSTRING", "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM",
        "REPLACE", "INSTR", "COALESCE", "IFNULL", "NULLIF", "TYPEOF", "ABS", "RANDOM",
        "HEX", "QUOTE", "ZEROBLOB", "UNICODE", "CHAR",
    ];

    if KNOWN_SCALARS.contains(&name.as_str()) {
        return Ok(PlanExpr::Function {
            name,
            args: scalar_args,
        });
    }

    Err(Error::Other(format!("unknown function: {name}")))
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
        ast::BinaryOperator::StringConcat => Ok(BinOp::Concat),
        _ => Err(Error::Other(format!("unsupported operator: {op}"))),
    }
}

pub(super) fn plan_order_expr(
    expr: &Expr,
    table_columns: &[ColumnRef],
    output_names: &[String],
    catalog: &Catalog,
) -> Result<PlanExpr> {
    if let Expr::Identifier(ident) = expr {
        let name = &ident.value;
        if let Some(col) = table_columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            return Ok(PlanExpr::Column(col.clone()));
        }
        if output_names
            .iter()
            .any(|n| n.eq_ignore_ascii_case(name))
        {
            let col_ref = ColumnRef {
                name: name.clone(),
                column_index: 0,
                is_rowid_alias: false,
                table: None,
            };
            return Ok(PlanExpr::Column(col_ref));
        }
    }
    plan_expr(expr, table_columns, catalog)
}

pub(super) fn plan_limit_expr(expr: &Expr) -> Result<u64> {
    match expr {
        Expr::Value(val) => match &val.value {
            ast::Value::Number(n, _) => n.parse::<u64>().map_err(|_| {
                Error::Other(format!("invalid LIMIT/OFFSET value: {n}"))
            }),
            _ => Err(Error::Other(format!(
                "LIMIT/OFFSET must be a number, got: {val}"
            ))),
        },
        _ => Err(Error::Other(format!(
            "LIMIT/OFFSET must be a literal, got: {expr}"
        ))),
    }
}

pub(super) fn contains_aggregate(expr: &PlanExpr) -> bool {
    match expr {
        PlanExpr::Aggregate { .. } => true,
        PlanExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        PlanExpr::UnaryOp { operand, .. } => contains_aggregate(operand),
        PlanExpr::IsNull(inner) | PlanExpr::IsNotNull(inner) => contains_aggregate(inner),
        PlanExpr::Function { args, .. } => args.iter().any(contains_aggregate),
        PlanExpr::Like { expr, pattern, .. } => {
            contains_aggregate(expr) || contains_aggregate(pattern)
        }
        PlanExpr::InList { expr, list, .. } => {
            contains_aggregate(expr) || list.iter().any(contains_aggregate)
        }
        PlanExpr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || when_clauses
                    .iter()
                    .any(|(c, r)| contains_aggregate(c) || contains_aggregate(r))
                || else_result
                    .as_ref()
                    .is_some_and(|e| contains_aggregate(e))
        }
        PlanExpr::Cast { expr, .. } => contains_aggregate(expr),
        PlanExpr::InSubquery { expr, .. } => contains_aggregate(expr),
        PlanExpr::Column(_)
        | PlanExpr::Rowid
        | PlanExpr::Literal(_)
        | PlanExpr::Wildcard
        | PlanExpr::Subquery(_)
        | PlanExpr::Exists { .. } => false,
    }
}

pub(super) fn collect_aggregates(expr: &PlanExpr, out: &mut Vec<(AggFunc, PlanExpr, bool)>) {
    match expr {
        PlanExpr::Aggregate { func, arg, distinct } => {
            out.push((*func, arg.as_ref().clone(), *distinct));
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_aggregates(left, out);
            collect_aggregates(right, out);
        }
        PlanExpr::UnaryOp { operand, .. } => {
            collect_aggregates(operand, out);
        }
        PlanExpr::IsNull(inner) | PlanExpr::IsNotNull(inner) => {
            collect_aggregates(inner, out);
        }
        PlanExpr::Function { args, .. } => {
            for a in args {
                collect_aggregates(a, out);
            }
        }
        _ => {}
    }
}

pub fn agg_column_name(func: &AggFunc, arg: &PlanExpr, distinct: bool) -> String {
    let func_name = match func {
        AggFunc::Count => "COUNT",
        AggFunc::Sum => "SUM",
        AggFunc::Avg => "AVG",
        AggFunc::Min => "MIN",
        AggFunc::Max => "MAX",
    };
    let arg_str = match arg {
        PlanExpr::Wildcard => "*".to_string(),
        PlanExpr::Column(c) => c.name.clone(),
        _ => format!("{:?}", arg),
    };
    if distinct {
        format!("{func_name}(DISTINCT {arg_str})")
    } else {
        format!("{func_name}({arg_str})")
    }
}
