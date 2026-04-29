use std::num::NonZero;

use lru::LruCache;
use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;
use rsqlite_vfs::Vfs;
use sqlparser::ast::Statement;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::executor::{self, ExecResult};
use crate::planner::{self, Plan};
use crate::types::QueryResult;

const PLAN_CACHE_SIZE: usize = 64;

pub struct Database {
    pager: Pager,
    catalog: Catalog,
    plan_cache: LruCache<String, Plan>,
}

impl Database {
    pub fn open(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::open(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self {
            pager,
            catalog,
            plan_cache: LruCache::new(NonZero::new(PLAN_CACHE_SIZE).unwrap()),
        })
    }

    pub fn create(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::create(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self {
            pager,
            catalog,
            plan_cache: LruCache::new(NonZero::new(PLAN_CACHE_SIZE).unwrap()),
        })
    }

    pub fn query_with_params(&mut self, sql: &str, params: Vec<Value>) -> Result<QueryResult> {
        executor::set_params(params);
        let result = self.query(sql);
        executor::clear_params();
        result
    }

    pub fn execute_with_params(&mut self, sql: &str, params: Vec<Value>) -> Result<ExecResult> {
        executor::set_params(params);
        let result = self.execute(sql);
        executor::clear_params();
        result
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        if let Some(result) = self.try_explain_query_plan(sql)? {
            return Ok(result);
        }
        let plan = self.get_or_plan(sql)?;
        if let Plan::Pragma { ref name, ref argument } = plan {
            return executor::execute_pragma(name, argument.as_deref(), &self.pager, &self.catalog);
        }
        executor::execute(&plan, &mut self.pager, &self.catalog)
    }

    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let plan = self.get_or_plan(sql)?;
        let is_ddl = matches!(
            plan,
            Plan::CreateTable(_)
                | Plan::CreateIndex(_)
                | Plan::DropTable { .. }
                | Plan::DropIndex { .. }
                | Plan::AlterTableAddColumn { .. }
                | Plan::AlterTableRename { .. }
                | Plan::CreateView { .. }
                | Plan::DropView { .. }
                | Plan::CreateTableAsSelect { .. }
        );
        let result = executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)?;
        if is_ddl {
            self.plan_cache.clear();
        }
        Ok(result)
    }

    fn try_explain_query_plan(&mut self, sql: &str) -> Result<Option<QueryResult>> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(None);
        }
        if let Statement::Explain { query_plan: true, statement, .. } = &stmts[0] {
            let plan = planner::plan_statement(statement, &self.catalog)?;
            return Ok(Some(describe_plan(&plan)));
        }
        Ok(None)
    }

    fn get_or_plan(&mut self, sql: &str) -> Result<Plan> {
        if let Some(cached) = self.plan_cache.get(sql) {
            return Ok(cached.clone());
        }
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(Plan::SingleRow);
        }
        let plan = planner::plan_statement(&stmts[0], &self.catalog)?;
        self.plan_cache.put(sql.to_string(), plan.clone());
        Ok(plan)
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<SqlResult> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(SqlResult::Execute(ExecResult { rows_affected: 0 }));
        }

        let stmt = &stmts[0];
        let is_query = is_query_statement(stmt);
        let plan = self.get_or_plan(sql)?;

        if let Plan::Pragma { ref name, ref argument } = plan {
            return Ok(SqlResult::Query(executor::execute_pragma(
                name,
                argument.as_deref(),
                &self.pager,
                &self.catalog,
            )?));
        }

        if is_query {
            Ok(SqlResult::Query(executor::execute(
                &plan,
                &mut self.pager,
                &self.catalog,
            )?))
        } else {
            let is_ddl = matches!(
                plan,
                Plan::CreateTable(_)
                    | Plan::CreateIndex(_)
                    | Plan::DropTable { .. }
                    | Plan::DropIndex { .. }
                    | Plan::AlterTableAddColumn { .. }
                    | Plan::AlterTableRename { .. }
                    | Plan::CreateView { .. }
                    | Plan::DropView { .. }
                    | Plan::CreateTableAsSelect { .. }
            );
            let result = executor::execute_mut(
                &plan,
                &mut self.pager,
                &mut self.catalog,
            )?;
            if is_ddl {
                self.plan_cache.clear();
            }
            Ok(SqlResult::Execute(result))
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}

pub enum SqlResult {
    Query(QueryResult),
    Execute(ExecResult),
}

fn is_query_statement(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Query(_))
}

fn describe_plan(plan: &Plan) -> QueryResult {
    let columns = vec![
        "id".to_string(),
        "parent".to_string(),
        "notused".to_string(),
        "detail".to_string(),
    ];
    let mut rows = Vec::new();
    let mut id = 0i64;
    describe_plan_recursive(plan, &mut rows, &mut id, 0, 0);
    QueryResult { columns, rows }
}

fn describe_plan_recursive(
    plan: &Plan,
    rows: &mut Vec<crate::types::Row>,
    id: &mut i64,
    parent: i64,
    depth: usize,
) {
    use crate::types::Row;
    let indent = "   ".repeat(depth);
    let my_id = *id;
    *id += 1;

    match plan {
        Plan::Scan { table, .. } => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}SCAN TABLE {table}")),
            ]});
        }
        Plan::IndexScan { table, index_columns, .. } => {
            let cols = index_columns.join(", ");
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}SEARCH TABLE {table} USING INDEX ({cols})")),
            ]});
        }
        Plan::IndexRangeScan { table, index_column, .. } => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}SEARCH TABLE {table} USING INDEX ({index_column} range)")),
            ]});
        }
        Plan::Filter { input, .. } => {
            describe_plan_recursive(input, rows, id, parent, depth);
        }
        Plan::Project { input, .. } => {
            describe_plan_recursive(input, rows, id, parent, depth);
        }
        Plan::Sort { input, .. } => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}USE TEMP B-TREE FOR ORDER BY")),
            ]});
            describe_plan_recursive(input, rows, id, my_id, depth + 1);
        }
        Plan::Limit { input, .. } => {
            describe_plan_recursive(input, rows, id, parent, depth);
        }
        Plan::Aggregate { input, .. } => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}USE TEMP B-TREE FOR GROUP BY")),
            ]});
            describe_plan_recursive(input, rows, id, my_id, depth + 1);
        }
        Plan::NestedLoopJoin { left, right, join_type, .. } => {
            let jt = match join_type {
                planner::JoinType::Inner => "INNER",
                planner::JoinType::Left => "LEFT",
                planner::JoinType::Cross => "CROSS",
            };
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}NESTED LOOP {jt} JOIN")),
            ]});
            describe_plan_recursive(left, rows, id, my_id, depth + 1);
            describe_plan_recursive(right, rows, id, my_id, depth + 1);
        }
        Plan::Union { left, right, all } => {
            let op = if *all { "UNION ALL" } else { "UNION" };
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}COMPOUND QUERY ({op})")),
            ]});
            describe_plan_recursive(left, rows, id, my_id, depth + 1);
            describe_plan_recursive(right, rows, id, my_id, depth + 1);
        }
        Plan::Window { input, .. } => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}WINDOW FUNCTION")),
            ]});
            describe_plan_recursive(input, rows, id, my_id, depth + 1);
        }
        Plan::Insert(p) => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}INSERT INTO {}", p.table_name)),
            ]});
        }
        Plan::Update(p) => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}UPDATE {}", p.table_name)),
            ]});
        }
        Plan::Delete(p) => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}DELETE FROM {}", p.table_name)),
            ]});
        }
        Plan::SingleRow => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}SCAN CONSTANT ROW")),
            ]});
        }
        _ => {
            rows.push(Row { values: vec![
                Value::Integer(my_id), Value::Integer(parent),
                Value::Integer(0), Value::Text(format!("{indent}PLAN NODE")),
            ]});
        }
    }
}

#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;
