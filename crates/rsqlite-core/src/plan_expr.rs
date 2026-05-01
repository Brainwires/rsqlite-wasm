use std::cell::RefCell;

use sqlparser::ast::{self, Expr, SelectItem};

use crate::catalog::Catalog;
use crate::error::{Error, Result};

thread_local! {
    pub(super) static PARAM_AUTO_INDEX: RefCell<usize> = RefCell::new(0);
    /// Named windows visible to the current SELECT being planned.
    /// Populated by plan_select_body via with_named_windows; consulted by
    /// plan_window_function when an OVER clause references a name.
    pub(super) static NAMED_WINDOWS: RefCell<std::collections::HashMap<String, ast::WindowSpec>>
        = RefCell::new(std::collections::HashMap::new());
}

/// RAII helper: install a fresh named-windows map for the duration of the
/// closure, then restore the previous one. Required so subqueries don't see
/// the outer SELECT's WINDOW clause.
pub fn with_named_windows<R>(
    map: std::collections::HashMap<String, ast::WindowSpec>,
    f: impl FnOnce() -> R,
) -> R {
    let prev = NAMED_WINDOWS.with(|cell| std::mem::replace(&mut *cell.borrow_mut(), map));
    let result = f();
    NAMED_WINDOWS.with(|cell| *cell.borrow_mut() = prev);
    result
}

#[derive(Debug, Clone)]
pub struct ColumnRef {
    pub name: String,
    pub column_index: usize,
    pub is_rowid_alias: bool,
    pub table: Option<String>,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub is_unique: bool,
}

#[derive(Debug, Clone)]
pub struct ProjectionItem {
    pub expr: PlanExpr,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Total,
    GroupConcat { separator: Option<String> },
    JsonGroupArray,
    JsonGroupObject { key: Box<PlanExpr> },
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
        filter: Option<Box<PlanExpr>>,
    },
    Function {
        name: String,
        args: Vec<PlanExpr>,
    },
    Like {
        expr: Box<PlanExpr>,
        pattern: Box<PlanExpr>,
        negated: bool,
        escape_char: Option<char>,
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
    Param(usize),
    WindowFunction {
        func_name: String,
        args: Vec<PlanExpr>,
        partition_by: Vec<PlanExpr>,
        order_by: Vec<(PlanExpr, bool)>,
        frame: Option<WindowFrameSpec>,
        filter: Option<Box<PlanExpr>>,
    },
    Collate {
        expr: Box<PlanExpr>,
        collation: String,
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
    BitAnd,
    BitOr,
    ShiftLeft,
    ShiftRight,
    Is,
    IsNot,
    JsonArrow,
    JsonLongArrow,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Not,
    Neg,
    BitNot,
}

#[derive(Debug, Clone)]
pub struct WindowFrameSpec {
    pub units: FrameUnits,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
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

pub fn plan_expr(expr: &Expr, columns: &[ColumnRef], catalog: &Catalog) -> Result<PlanExpr> {
    match expr {
        Expr::Identifier(ident) => {
            let name = &ident.value;
            if name.eq_ignore_ascii_case("rowid") {
                // When the table has an INTEGER PRIMARY KEY (rowid alias),
                // bare `rowid` resolves to that column — its values already
                // carry the rowid. Tables without an alias would need a
                // synthetic rowid column threaded through every Row, which
                // is documented as a limitation.
                if let Some(alias) = columns.iter().find(|c| c.is_rowid_alias) {
                    return Ok(PlanExpr::Column(alias.clone()));
                }
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
            // `t.rowid` resolves to the rowid-alias column of table `t`.
            if col_name.eq_ignore_ascii_case("rowid") {
                if let Some(alias) = columns.iter().find(|c| {
                    c.is_rowid_alias
                        && c.table
                            .as_ref()
                            .is_some_and(|t| t.eq_ignore_ascii_case(table))
                }) {
                    return Ok(PlanExpr::Column(alias.clone()));
                }
                return Ok(PlanExpr::Rowid);
            }
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
                .ok_or_else(|| Error::Other(format!("unknown column: {table}.{col_name}")))?;
            Ok(PlanExpr::Column(col.clone()))
        }
        Expr::Value(val) => {
            if let ast::Value::Placeholder(s) = &val.value {
                PARAM_AUTO_INDEX.with(|c| {
                    let idx = parse_placeholder(s, &mut c.borrow_mut());
                    Ok(PlanExpr::Param(idx))
                })
            } else {
                Ok(PlanExpr::Literal(plan_value(&val.value)?))
            }
        }
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
                ast::UnaryOperator::PGBitwiseNot => UnaryOp::BitNot,
                _ => return Err(Error::Other(format!("unsupported unary operator: {op}"))),
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
        Expr::IsDistinctFrom(a, b) => {
            let l = plan_expr(a, columns, catalog)?;
            let r = plan_expr(b, columns, catalog)?;
            Ok(PlanExpr::BinaryOp {
                left: Box::new(l),
                op: BinOp::IsNot,
                right: Box::new(r),
            })
        }
        Expr::IsNotDistinctFrom(a, b) => {
            let l = plan_expr(a, columns, catalog)?;
            let r = plan_expr(b, columns, catalog)?;
            Ok(PlanExpr::BinaryOp {
                left: Box::new(l),
                op: BinOp::Is,
                right: Box::new(r),
            })
        }
        Expr::Nested(e) => plan_expr(e, columns, catalog),
        Expr::Collate { expr, collation } => {
            let inner = plan_expr(expr, columns, catalog)?;
            Ok(PlanExpr::Collate {
                expr: Box::new(inner),
                collation: collation.to_string().to_uppercase(),
            })
        }
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
            escape_char,
            ..
        } => {
            let e = plan_expr(like_expr, columns, catalog)?;
            let p = plan_expr(pattern, columns, catalog)?;
            let esc = escape_char.as_ref().and_then(|s| s.chars().next());
            Ok(PlanExpr::Like {
                expr: Box::new(e),
                pattern: Box::new(p),
                negated: *negated,
                escape_char: esc,
            })
        }
        Expr::ILike {
            negated,
            expr: like_expr,
            pattern,
            escape_char,
            ..
        } => {
            let e = plan_expr(like_expr, columns, catalog)?;
            let p = plan_expr(pattern, columns, catalog)?;
            let esc = escape_char.as_ref().and_then(|s| s.chars().next());
            Ok(PlanExpr::Like {
                expr: Box::new(e),
                pattern: Box::new(p),
                negated: *negated,
                escape_char: esc,
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
            let sub_plan =
                super::plan_select(subquery, catalog, &std::collections::HashMap::new())?;
            Ok(PlanExpr::InSubquery {
                expr: Box::new(e),
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::Subquery(query) => {
            let sub_plan = super::plan_select(query, catalog, &std::collections::HashMap::new())?;
            Ok(PlanExpr::Subquery(Box::new(sub_plan)))
        }
        Expr::Exists { subquery, negated } => {
            let sub_plan =
                super::plan_select(subquery, catalog, &std::collections::HashMap::new())?;
            Ok(PlanExpr::Exists {
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        _ => Err(Error::Other(format!("unsupported expression: {expr}"))),
    }
}

fn plan_function_expr(
    func: &ast::Function,
    columns: &[ColumnRef],
    catalog: &Catalog,
) -> Result<PlanExpr> {
    let name = func.name.to_string().to_uppercase();

    let filter = func
        .filter
        .as_ref()
        .map(|e| plan_expr(e, columns, catalog).map(Box::new))
        .transpose()?;

    if let Some(ref over) = func.over {
        return plan_window_function(&name, func, over, columns, catalog, filter);
    }

    if name == "GROUP_CONCAT" {
        let (arg, separator, distinct) = match &func.args {
            ast::FunctionArguments::List(list) => {
                let distinct = list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct);
                let first_arg = match list.args.first() {
                    Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))) => {
                        plan_expr(e, columns, catalog)?
                    }
                    _ => {
                        return Err(Error::Other(
                            "GROUP_CONCAT requires at least 1 argument".into(),
                        ));
                    }
                };
                let sep = if list.args.len() > 1 {
                    if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))) =
                        list.args.get(1)
                    {
                        if let Expr::Value(v) = e {
                            if let ast::Value::SingleQuotedString(s) = &v.value {
                                Some(s.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                (first_arg, sep, distinct)
            }
            _ => return Err(Error::Other("GROUP_CONCAT requires arguments".into())),
        };
        return Ok(PlanExpr::Aggregate {
            func: AggFunc::GroupConcat { separator },
            arg: Box::new(arg),
            distinct,
            filter,
        });
    }

    if name == "JSON_GROUP_ARRAY" {
        let arg = match &func.args {
            ast::FunctionArguments::List(list) => match list.args.first() {
                Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))) => {
                    plan_expr(e, columns, catalog)?
                }
                _ => return Err(Error::Other("json_group_array requires 1 argument".into())),
            },
            _ => return Err(Error::Other("json_group_array requires 1 argument".into())),
        };
        return Ok(PlanExpr::Aggregate {
            func: AggFunc::JsonGroupArray,
            arg: Box::new(arg),
            distinct: false,
            filter,
        });
    }

    if name == "JSON_GROUP_OBJECT" {
        let (key, value) = match &func.args {
            ast::FunctionArguments::List(list) if list.args.len() == 2 => {
                let parse_at = |idx: usize| -> Result<PlanExpr> {
                    match &list.args[idx] {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            plan_expr(e, columns, catalog)
                        }
                        _ => Err(Error::Other(
                            "json_group_object arguments must be expressions".into(),
                        )),
                    }
                };
                (parse_at(0)?, parse_at(1)?)
            }
            _ => {
                return Err(Error::Other(
                    "json_group_object requires 2 arguments (key, value)".into(),
                ));
            }
        };
        return Ok(PlanExpr::Aggregate {
            func: AggFunc::JsonGroupObject { key: Box::new(key) },
            arg: Box::new(value),
            distinct: false,
            filter,
        });
    }

    let arg_count = match &func.args {
        ast::FunctionArguments::List(list) => list.args.len(),
        _ => 0,
    };

    let agg_func = match name.as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" if arg_count <= 1 => Some(AggFunc::Min),
        "MAX" if arg_count <= 1 => Some(AggFunc::Max),
        "TOTAL" => Some(AggFunc::Total),
        _ => None,
    };

    if let Some(func_type) = agg_func {
        let (arg, distinct) = match &func.args {
            ast::FunctionArguments::List(list) => {
                let distinct = list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct);
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
                            )));
                        }
                    }
                }
            }
            ast::FunctionArguments::None => (PlanExpr::Wildcard, false),
            _ => {
                return Err(Error::Other(format!(
                    "unsupported aggregate arguments: {}",
                    func
                )));
            }
        };

        return Ok(PlanExpr::Aggregate {
            func: func_type,
            arg: Box::new(arg),
            distinct,
            filter,
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
        "LENGTH",
        "SUBSTR",
        "SUBSTRING",
        "UPPER",
        "LOWER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REPLACE",
        "INSTR",
        "COALESCE",
        "IFNULL",
        "NULLIF",
        "TYPEOF",
        "ABS",
        "RANDOM",
        "HEX",
        "QUOTE",
        "ZEROBLOB",
        "UNICODE",
        "CHAR",
        "GLOB",
        "ROUND",
        "LAST_INSERT_ROWID",
        "CHANGES",
        "TOTAL_CHANGES",
        "PRINTF",
        "FORMAT",
        "LIKELY",
        "UNLIKELY",
        "LIKELIHOOD",
        "SIGN",
        "SQLITE_VERSION",
        "SQLITE_SOURCE_ID",
        "RANDOMBLOB",
        "MIN",
        "MAX",
        "DATE",
        "TIME",
        "DATETIME",
        "JULIANDAY",
        "UNIXEPOCH",
        "STRFTIME",
        "IIF",
        "VEC_DISTANCE_COSINE",
        "VEC_DISTANCE_L2",
        "VEC_DISTANCE_DOT",
        "VEC_LENGTH",
        "VEC_NORMALIZE",
        "VEC_FROM_JSON",
        "VEC_TO_JSON",
        "JSON",
        "JSON_EXTRACT",
        "JSON_TYPE",
        "JSON_VALID",
        "JSON_ARRAY",
        "JSON_OBJECT",
        "JSON_ARRAY_LENGTH",
        "JSON_INSERT",
        "JSON_REPLACE",
        "JSON_SET",
        "JSON_REMOVE",
        "JSON_QUOTE",
        "JSON_PATCH",
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

pub(super) fn reset_param_counter() {
    PARAM_AUTO_INDEX.with(|c| *c.borrow_mut() = 0);
}

fn parse_placeholder(s: &str, auto_idx: &mut usize) -> usize {
    if s == "?" {
        let idx = *auto_idx;
        *auto_idx += 1;
        idx
    } else if let Some(rest) = s.strip_prefix('?') {
        rest.parse::<usize>().unwrap_or(1).saturating_sub(1)
    } else {
        let idx = *auto_idx;
        *auto_idx += 1;
        idx
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
        ast::BinaryOperator::BitwiseAnd => Ok(BinOp::BitAnd),
        ast::BinaryOperator::BitwiseOr => Ok(BinOp::BitOr),
        ast::BinaryOperator::PGBitwiseShiftLeft => Ok(BinOp::ShiftLeft),
        ast::BinaryOperator::PGBitwiseShiftRight => Ok(BinOp::ShiftRight),
        ast::BinaryOperator::Arrow => Ok(BinOp::JsonArrow),
        ast::BinaryOperator::LongArrow => Ok(BinOp::JsonLongArrow),
        _ => Err(Error::Other(format!("unsupported operator: {op}"))),
    }
}

pub(super) fn plan_order_expr(
    expr: &Expr,
    table_columns: &[ColumnRef],
    output_names: &[String],
    catalog: &Catalog,
) -> Result<PlanExpr> {
    if let Expr::Value(val) = expr {
        if let ast::Value::Number(n, _) = &val.value {
            if let Ok(idx) = n.parse::<usize>() {
                if idx >= 1 && idx <= output_names.len() {
                    let name = &output_names[idx - 1];
                    if let Some(col) = table_columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(name))
                    {
                        return Ok(PlanExpr::Column(col.clone()));
                    }
                    return Ok(PlanExpr::Column(ColumnRef {
                        name: name.clone(),
                        column_index: 0,
                        is_rowid_alias: false,
                        table: None,
                        nullable: true,
                        is_primary_key: false,
                        is_unique: false,
                    }));
                }
            }
        }
    }
    if let Expr::Identifier(ident) = expr {
        let name = &ident.value;
        if let Some(col) = table_columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            return Ok(PlanExpr::Column(col.clone()));
        }
        if output_names.iter().any(|n| n.eq_ignore_ascii_case(name)) {
            let col_ref = ColumnRef {
                name: name.clone(),
                column_index: 0,
                is_rowid_alias: false,
                table: None,
                nullable: true,
                is_primary_key: false,
                is_unique: false,
            };
            return Ok(PlanExpr::Column(col_ref));
        }
    }
    plan_expr(expr, table_columns, catalog)
}

pub(super) fn plan_limit_expr(expr: &Expr) -> Result<u64> {
    match expr {
        Expr::Value(val) => match &val.value {
            ast::Value::Number(n, _) => n
                .parse::<u64>()
                .map_err(|_| Error::Other(format!("invalid LIMIT/OFFSET value: {n}"))),
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
                || else_result.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        PlanExpr::Cast { expr, .. } => contains_aggregate(expr),
        PlanExpr::InSubquery { expr, .. } => contains_aggregate(expr),
        PlanExpr::Column(_)
        | PlanExpr::Rowid
        | PlanExpr::Literal(_)
        | PlanExpr::Wildcard
        | PlanExpr::Subquery(_)
        | PlanExpr::Exists { .. }
        | PlanExpr::Param(_)
        | PlanExpr::WindowFunction { .. } => false,
        PlanExpr::Collate { expr, .. } => contains_aggregate(expr),
    }
}

pub(super) fn collect_aggregates(
    expr: &PlanExpr,
    out: &mut Vec<(AggFunc, PlanExpr, bool, Option<PlanExpr>)>,
) {
    match expr {
        PlanExpr::Aggregate {
            func,
            arg,
            distinct,
            filter,
        } => {
            out.push((
                func.clone(),
                arg.as_ref().clone(),
                *distinct,
                filter.as_ref().map(|f| (**f).clone()),
            ));
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
        PlanExpr::Collate { expr, .. } => {
            collect_aggregates(expr, out);
        }
        _ => {}
    }
}

fn plan_window_function(
    name: &str,
    func: &ast::Function,
    over: &ast::WindowType,
    columns: &[ColumnRef],
    catalog: &Catalog,
    filter: Option<Box<PlanExpr>>,
) -> Result<PlanExpr> {
    // Resolve the OVER clause to a concrete WindowSpec, following named
    // window references and inheritance.
    let owned_spec_storage;
    let spec: &ast::WindowSpec = match over {
        ast::WindowType::WindowSpec(spec) => spec,
        ast::WindowType::NamedWindow(name) => {
            let resolved = NAMED_WINDOWS.with(|cell| cell.borrow().get(&name.value).cloned());
            match resolved {
                Some(s) => {
                    owned_spec_storage = s;
                    &owned_spec_storage
                }
                None => {
                    return Err(Error::Other(format!(
                        "unknown named window: {}",
                        name.value
                    )));
                }
            }
        }
    };

    let partition_by = spec
        .partition_by
        .iter()
        .map(|e| plan_expr(e, columns, catalog))
        .collect::<Result<Vec<_>>>()?;

    let order_by = spec
        .order_by
        .iter()
        .map(|ob| {
            let expr = plan_expr(&ob.expr, columns, catalog)?;
            let desc = ob.options.asc == Some(false);
            Ok((expr, desc))
        })
        .collect::<Result<Vec<_>>>()?;

    let args = match &func.args {
        ast::FunctionArguments::List(list) => {
            let mut planned = Vec::new();
            for a in &list.args {
                match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                        planned.push(plan_expr(e, columns, catalog)?);
                    }
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                        planned.push(PlanExpr::Wildcard);
                    }
                    _ => {}
                }
            }
            planned
        }
        ast::FunctionArguments::None => Vec::new(),
        _ => Vec::new(),
    };

    static KNOWN_WINDOW_FUNCS: &[&str] = &[
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "NTILE",
        "LAG",
        "LEAD",
        "FIRST_VALUE",
        "LAST_VALUE",
        "NTH_VALUE",
        "PERCENT_RANK",
        "CUME_DIST",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "TOTAL",
    ];

    if !KNOWN_WINDOW_FUNCS.contains(&name) {
        return Err(Error::Other(format!("unknown window function: {name}")));
    }

    let frame = spec.window_frame.as_ref().map(|wf| {
        let units = match wf.units {
            ast::WindowFrameUnits::Rows => FrameUnits::Rows,
            ast::WindowFrameUnits::Range => FrameUnits::Range,
            ast::WindowFrameUnits::Groups => FrameUnits::Groups,
        };
        let start = plan_frame_bound(&wf.start_bound);
        let end = wf
            .end_bound
            .as_ref()
            .map(plan_frame_bound)
            .unwrap_or(FrameBound::CurrentRow);
        WindowFrameSpec { units, start, end }
    });

    Ok(PlanExpr::WindowFunction {
        func_name: name.to_string(),
        args,
        partition_by,
        order_by,
        frame,
        filter,
    })
}

fn plan_frame_bound(bound: &ast::WindowFrameBound) -> FrameBound {
    match bound {
        ast::WindowFrameBound::CurrentRow => FrameBound::CurrentRow,
        ast::WindowFrameBound::Preceding(None) => FrameBound::UnboundedPreceding,
        ast::WindowFrameBound::Following(None) => FrameBound::UnboundedFollowing,
        ast::WindowFrameBound::Preceding(Some(expr)) => {
            FrameBound::Preceding(eval_static_int(expr).unwrap_or(0))
        }
        ast::WindowFrameBound::Following(Some(expr)) => {
            FrameBound::Following(eval_static_int(expr).unwrap_or(0))
        }
    }
}

/// Best-effort static evaluation of a frame-bound expression. SQL only allows
/// constants here, so an integer literal (with optional unary minus) covers
/// every realistic case.
fn eval_static_int(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.parse::<i64>().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => eval_static_int(inner).map(|n| -n),
        _ => None,
    }
}

pub(super) fn contains_window_function(expr: &PlanExpr) -> bool {
    match expr {
        PlanExpr::WindowFunction { .. } => true,
        PlanExpr::BinaryOp { left, right, .. } => {
            contains_window_function(left) || contains_window_function(right)
        }
        PlanExpr::UnaryOp { operand, .. } => contains_window_function(operand),
        PlanExpr::IsNull(inner) | PlanExpr::IsNotNull(inner) => contains_window_function(inner),
        PlanExpr::Function { args, .. } => args.iter().any(contains_window_function),
        PlanExpr::Like { expr, pattern, .. } => {
            contains_window_function(expr) || contains_window_function(pattern)
        }
        PlanExpr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|e| contains_window_function(e))
                || when_clauses
                    .iter()
                    .any(|(c, r)| contains_window_function(c) || contains_window_function(r))
                || else_result
                    .as_ref()
                    .is_some_and(|e| contains_window_function(e))
        }
        PlanExpr::Cast { expr, .. } => contains_window_function(expr),
        _ => false,
    }
}

pub(super) fn collect_window_functions(expr: &PlanExpr, out: &mut Vec<PlanExpr>) {
    match expr {
        PlanExpr::WindowFunction { .. } => {
            out.push(expr.clone());
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_window_functions(left, out);
            collect_window_functions(right, out);
        }
        PlanExpr::UnaryOp { operand, .. } => {
            collect_window_functions(operand, out);
        }
        PlanExpr::IsNull(inner) | PlanExpr::IsNotNull(inner) => {
            collect_window_functions(inner, out);
        }
        PlanExpr::Function { args, .. } => {
            for a in args {
                collect_window_functions(a, out);
            }
        }
        PlanExpr::Cast { expr, .. } | PlanExpr::Collate { expr, .. } => {
            collect_window_functions(expr, out);
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
        AggFunc::Total => "TOTAL",
        AggFunc::GroupConcat { .. } => "GROUP_CONCAT",
        AggFunc::JsonGroupArray => "json_group_array",
        AggFunc::JsonGroupObject { .. } => "json_group_object",
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
