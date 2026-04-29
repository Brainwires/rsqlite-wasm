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
pub struct CreateIndexPlan {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub sql: String,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table_name: String,
    pub root_page: u32,
    pub table_columns: Vec<ColumnRef>,
    pub target_columns: Option<Vec<String>>,
    pub rows: Vec<Vec<PlanExpr>>,
    pub on_conflict: Option<OnConflictPlan>,
}

#[derive(Debug, Clone)]
pub enum OnConflictPlan {
    DoNothing,
    DoUpdate {
        assignments: Vec<(String, PlanExpr)>,
    },
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
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
    Union {
        left: Box<Plan>,
        right: Box<Plan>,
        all: bool,
    },
    Scan {
        table: String,
        root_page: u32,
        columns: Vec<ColumnRef>,
    },
    IndexScan {
        table: String,
        table_root_page: u32,
        index_root_page: u32,
        columns: Vec<ColumnRef>,
        index_columns: Vec<String>,
        lookup_values: Vec<PlanExpr>,
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
        aggregates: Vec<(AggFunc, PlanExpr, bool)>,
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
    Pragma {
        name: String,
        argument: Option<String>,
    },
    Begin,
    Commit,
    Rollback,
}

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
    Subquery(Box<Plan>),
    InSubquery {
        expr: Box<PlanExpr>,
        subquery: Box<Plan>,
        negated: bool,
    },
    Exists {
        subquery: Box<Plan>,
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

pub fn plan_statement(stmt: &Statement, catalog: &Catalog) -> Result<Plan> {
    match stmt {
        Statement::Query(query) => plan_select(query, catalog),
        Statement::CreateTable(ct) => plan_create_table(ct),
        Statement::CreateIndex(ci) => plan_create_index(ci),
        Statement::Insert(insert) => plan_insert(insert, catalog),
        Statement::Update {
            table,
            assignments,
            selection,
            ..
        } => plan_update(table, assignments, selection.as_ref(), catalog),
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
                ast::AlterTableOperation::RenameTable { table_name: new_name } => {
                    Ok(Plan::AlterTableRename {
                        old_name: table_name,
                        new_name: new_name.to_string(),
                    })
                }
                other => Err(Error::Other(format!(
                    "unsupported ALTER TABLE operation: {other}"
                ))),
            }
        }
        Statement::Pragma { name, value, .. } => {
            let pragma_name = name.to_string().to_lowercase();
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
        Statement::Rollback { .. } => Ok(Plan::Rollback),
        _ => Err(Error::Other(format!(
            "unsupported statement type: {stmt}"
        ))),
    }
}

pub fn plan_query(stmt: &Statement, catalog: &Catalog) -> Result<Plan> {
    plan_statement(stmt, catalog)
}

fn resolve_table_factor(
    relation: &TableFactor,
    catalog: &Catalog,
) -> Result<(Plan, Vec<ColumnRef>)> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            let table_def = catalog.get_table(&table_name).ok_or_else(|| {
                Error::Other(format!("table not found: {table_name}"))
            })?;

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
                })
                .collect();

            let plan = Plan::Scan {
                table: table_name,
                root_page: table_def.root_page,
                columns: columns.clone(),
            };

            Ok((plan, columns))
        }
        _ => Err(Error::Other(
            "only simple table references are supported".to_string(),
        )),
    }
}

fn plan_set_expr(set_expr: &SetExpr, catalog: &Catalog) -> Result<Plan> {
    match set_expr {
        SetExpr::Select(s) => plan_select_body(s, catalog).map(|(plan, _, _)| plan),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if *op != ast::SetOperator::Union {
                return Err(Error::Other(format!("unsupported set operation: {op}")));
            }
            let left_plan = plan_set_expr(left, catalog)?;
            let right_plan = plan_set_expr(right, catalog)?;
            let all = matches!(set_quantifier, ast::SetQuantifier::All);
            Ok(Plan::Union {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                all,
            })
        }
        _ => Err(Error::Other(
            "unsupported set expression".to_string(),
        )),
    }
}

fn plan_select(query: &ast::Query, catalog: &Catalog) -> Result<Plan> {
    match query.body.as_ref() {
        SetExpr::SetOperation { .. } => plan_set_expr(query.body.as_ref(), catalog),
        SetExpr::Select(s) => plan_simple_select(query, s, catalog),
        _ => Err(Error::Other("unsupported query form".to_string())),
    }
}

fn plan_simple_select(
    query: &ast::Query,
    select: &ast::Select,
    catalog: &Catalog,
) -> Result<Plan> {
    let (mut plan, all_columns, output_names) = plan_select_body(select, catalog)?;

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
                plan = Plan::Sort {
                    input: Box::new(plan),
                    keys,
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

    // Build plan from FROM clause (first item + its joins)
    let from = &select.from[0];
    let (mut plan, mut all_columns) = resolve_table_factor(&from.relation, catalog)?;

    // Handle explicit JOINs
    for join in &from.joins {
        let (right_plan, right_columns) = resolve_table_factor(&join.relation, catalog)?;
        let combined_columns: Vec<ColumnRef> =
            all_columns.iter().chain(right_columns.iter()).cloned().collect();

        let (join_type, condition) = match &join.join_operator {
            ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => {
                let cond = plan_join_constraint(constraint, &combined_columns, catalog)?;
                (JoinType::Inner, cond)
            }
            ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => {
                let cond = plan_join_constraint(constraint, &combined_columns, catalog)?;
                (JoinType::Left, cond)
            }
            ast::JoinOperator::CrossJoin => (JoinType::Cross, None),
            _ => {
                return Err(Error::Other(
                    "only INNER, LEFT, and CROSS JOIN are supported".to_string(),
                ))
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

    // Handle implicit cross-joins (multiple comma-separated tables)
    for extra_from in &select.from[1..] {
        let (right_plan, right_columns) = resolve_table_factor(&extra_from.relation, catalog)?;
        let combined_columns: Vec<ColumnRef> =
            all_columns.iter().chain(right_columns.iter()).cloned().collect();

        plan = Plan::NestedLoopJoin {
            left: Box::new(plan),
            right: Box::new(right_plan),
            condition: None,
            join_type: JoinType::Cross,
        };
        all_columns = combined_columns;

        for join in &extra_from.joins {
            let (right_plan, right_cols) = resolve_table_factor(&join.relation, catalog)?;
            let combined: Vec<ColumnRef> =
                all_columns.iter().chain(right_cols.iter()).cloned().collect();

            let (join_type, condition) = match &join.join_operator {
                ast::JoinOperator::Inner(c) | ast::JoinOperator::Join(c) => {
                    let cond = plan_join_constraint(c, &combined, catalog)?;
                    (JoinType::Inner, cond)
                }
                ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => {
                    let cond = plan_join_constraint(c, &combined, catalog)?;
                    (JoinType::Left, cond)
                }
                ast::JoinOperator::CrossJoin => (JoinType::Cross, None),
                _ => {
                    return Err(Error::Other(
                        "only INNER, LEFT, and CROSS JOIN are supported".to_string(),
                    ))
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

        if let Some(index_plan) =
            try_index_scan(&plan, &predicate, &all_columns, catalog)
        {
            plan = index_plan;
        } else {
            plan = Plan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }
    }

    // Check for GROUP BY and aggregate functions
    let group_by_exprs = match &select.group_by {
        ast::GroupByExpr::Expressions(exprs, _) if !exprs.is_empty() => {
            let mut planned = Vec::new();
            for e in exprs {
                planned.push(plan_expr(e, &all_columns, catalog)?);
            }
            planned
        }
        _ => Vec::new(),
    };

    let outputs = plan_select_items(&select.projection, &all_columns, catalog)?;
    let has_aggregates = outputs.iter().any(|o| contains_aggregate(&o.expr));

    if has_aggregates || !group_by_exprs.is_empty() {
        let mut aggregates: Vec<(AggFunc, PlanExpr, bool)> = Vec::new();
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

    // SELECT list -> Project
    let output_names: Vec<String> = outputs.iter().map(|o| o.alias.clone()).collect();
    plan = Plan::Project {
        input: Box::new(plan),
        outputs,
    };

    // DISTINCT
    if select.distinct.is_some() {
        plan = Plan::Distinct {
            input: Box::new(plan),
        };
    }

    Ok((plan, all_columns, output_names))
}

fn plan_select_items(
    items: &[SelectItem],
    columns: &[ColumnRef],
    catalog: &Catalog,
) -> Result<Vec<ProjectionItem>> {
    let mut outputs = Vec::new();

    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let plan_expr = plan_expr(expr, columns, catalog)?;
                let alias = expr.to_string();
                outputs.push(ProjectionItem {
                    expr: plan_expr,
                    alias,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let plan_expr = plan_expr(expr, columns, catalog)?;
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

fn plan_expr(expr: &Expr, columns: &[ColumnRef], catalog: &Catalog) -> Result<PlanExpr> {
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
            let sub_plan = plan_select(subquery, catalog)?;
            Ok(PlanExpr::InSubquery {
                expr: Box::new(e),
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::Subquery(query) => {
            let sub_plan = plan_select(query, catalog)?;
            Ok(PlanExpr::Subquery(Box::new(sub_plan)))
        }
        Expr::Exists { subquery, negated } => {
            let sub_plan = plan_select(subquery, catalog)?;
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

fn plan_order_expr(
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

fn plan_limit_expr(expr: &Expr) -> Result<u64> {
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

fn plan_join_constraint(
    constraint: &ast::JoinConstraint,
    columns: &[ColumnRef],
    catalog: &Catalog,
) -> Result<Option<PlanExpr>> {
    match constraint {
        ast::JoinConstraint::On(expr) => {
            let planned = plan_expr(expr, columns, catalog)?;
            Ok(Some(planned))
        }
        ast::JoinConstraint::None | ast::JoinConstraint::Natural => Ok(None),
        ast::JoinConstraint::Using(_) => Err(Error::Other(
            "USING clause not yet supported".to_string(),
        )),
    }
}

fn contains_aggregate(expr: &PlanExpr) -> bool {
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

fn collect_aggregates(expr: &PlanExpr, out: &mut Vec<(AggFunc, PlanExpr, bool)>) {
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

    let eq_parts = extract_equality_parts(predicate)?;

    for idx_def in catalog.indexes.values() {
        if !idx_def.table_name.eq_ignore_ascii_case(table_name) {
            continue;
        }
        if idx_def.columns.is_empty() {
            continue;
        }

        if eq_parts.len() >= idx_def.columns.len() {
            let mut lookup_values = Vec::new();
            let mut all_matched = true;

            for idx_col in &idx_def.columns {
                let found = eq_parts.iter().find(|(col_name, _)| {
                    col_name.eq_ignore_ascii_case(idx_col)
                });
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
                    columns: columns.clone(),
                    index_columns: idx_def.columns.clone(),
                    lookup_values,
                };

                return if let Some(remaining) = remaining_predicate {
                    Some(Plan::Filter {
                        input: Box::new(index_scan),
                        predicate: remaining,
                    })
                } else {
                    Some(index_scan)
                };
            }
        }
    }

    None
}

fn extract_equality_parts(predicate: &PlanExpr) -> Option<Vec<(String, PlanExpr)>> {
    let mut parts = Vec::new();
    collect_and_equalities(predicate, &mut parts);
    if parts.is_empty() {
        None
    } else {
        Some(parts)
    }
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
    let index_name = ci
        .name
        .as_ref()
        .map(|n| n.to_string())
        .unwrap_or_default();
    let table_name = ci.table_name.to_string();

    let columns: Vec<String> = ci
        .columns
        .iter()
        .map(|c| c.expr.to_string())
        .collect();

    let sql = format!("{ci}");

    Ok(Plan::CreateIndex(CreateIndexPlan {
        index_name,
        table_name,
        columns,
        sql,
        if_not_exists: ci.if_not_exists,
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
            table: None,
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

    let rows = match insert.source.as_ref() {
        None => {
            vec![vec![]]
        }
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
                planned_rows
            }
            _ => {
                return Err(Error::Other(
                    "only INSERT ... VALUES is supported".to_string(),
                ))
            }
        },
    };

    let on_conflict = match &insert.on {
        Some(ast::OnInsert::OnConflict(oc)) => match &oc.action {
            ast::OnConflictAction::DoNothing => Some(OnConflictPlan::DoNothing),
            ast::OnConflictAction::DoUpdate(do_update) => {
                let mut assignments = Vec::new();
                for assign in &do_update.assignments {
                    let col_name = match &assign.target {
                        ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                        ast::AssignmentTarget::Tuple(_) => {
                            return Err(Error::Other(
                                "tuple assignment not supported".to_string(),
                            ))
                        }
                    };
                    let expr = plan_expr(&assign.value, &all_columns, catalog)?;
                    assignments.push((col_name, expr));
                }
                Some(OnConflictPlan::DoUpdate { assignments })
            }
        },
        _ => None,
    };

    Ok(Plan::Insert(InsertPlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        target_columns,
        rows,
        on_conflict,
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
            table: None,
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
        let expr = plan_expr(&assignment.value, &all_columns, catalog)?;
        planned_assignments.push((col_name, expr));
    }

    let predicate = selection
        .map(|s| plan_expr(s, &all_columns, catalog))
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
            table: None,
        })
        .collect();

    let predicate = delete
        .selection
        .as_ref()
        .map(|s| plan_expr(s, &all_columns, catalog))
        .transpose()?;

    Ok(Plan::Delete(DeletePlan {
        table_name,
        root_page: table_def.root_page,
        table_columns: all_columns,
        predicate,
    }))
}
