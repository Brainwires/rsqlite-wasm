use std::collections::HashMap;
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

pub struct AttachedDb {
    pub pager: Pager,
    pub catalog: Catalog,
}

pub struct Database {
    pager: Pager,
    catalog: Catalog,
    plan_cache: LruCache<String, Plan>,
    vfs: Box<dyn Vfs>,
    attached: HashMap<String, AttachedDb>,
}

impl Database {
    pub fn open(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::open(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self {
            pager,
            catalog,
            plan_cache: LruCache::new(NonZero::new(PLAN_CACHE_SIZE).unwrap()),
            vfs: vfs.clone_box(),
            attached: HashMap::new(),
        })
    }

    pub fn create(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::create(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self {
            pager,
            catalog,
            plan_cache: LruCache::new(NonZero::new(PLAN_CACHE_SIZE).unwrap()),
            vfs: vfs.clone_box(),
            attached: HashMap::new(),
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
        if let Plan::Pragma {
            ref name,
            ref argument,
        } = plan
        {
            if name == "database_list" {
                return Ok(self.pragma_database_list());
            }
            return executor::execute_pragma(
                name,
                argument.as_deref(),
                &mut self.pager,
                &self.catalog,
            );
        }
        if plan_has_returning(&plan) {
            let result = executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)?;
            return result
                .returning
                .ok_or_else(|| crate::error::Error::Other("RETURNING produced no result".into()));
        }
        executor::execute(&plan, &mut self.pager, &self.catalog)
    }

    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let plan = self.get_or_plan(sql)?;
        if let Plan::Pragma {
            ref name,
            ref argument,
        } = plan
        {
            if name == "database_list" {
                return Ok(ExecResult::affected(0));
            }
            let _ = executor::execute_pragma(
                name,
                argument.as_deref(),
                &mut self.pager,
                &self.catalog,
            )?;
            return Ok(ExecResult::affected(0));
        }
        if let Plan::AttachDatabase {
            ref schema_name,
            ref file_path,
        } = plan
        {
            return self.execute_attach(schema_name, file_path);
        }
        if let Plan::DetachDatabase { ref schema_name } = plan {
            return self.execute_detach(schema_name);
        }
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
                | Plan::Vacuum
                | Plan::CreateTrigger { .. }
                | Plan::DropTrigger { .. }
        );
        let result = executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)?;
        if is_ddl {
            self.plan_cache.clear();
        }
        Ok(result)
    }

    pub fn page_count(&self) -> u32 {
        self.pager.page_count()
    }

    fn try_explain_query_plan(&mut self, sql: &str) -> Result<Option<QueryResult>> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(None);
        }
        if let Statement::Explain {
            query_plan: true,
            statement,
            ..
        } = &stmts[0]
        {
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
            return Ok(SqlResult::Execute(ExecResult::affected(0)));
        }

        let stmt = &stmts[0];
        let is_query = is_query_statement(stmt);
        let plan = self.get_or_plan(sql)?;

        if let Plan::Pragma {
            ref name,
            ref argument,
        } = plan
        {
            if name == "database_list" {
                return Ok(SqlResult::Query(self.pragma_database_list()));
            }
            return Ok(SqlResult::Query(executor::execute_pragma(
                name,
                argument.as_deref(),
                &mut self.pager,
                &self.catalog,
            )?));
        }

        if let Plan::AttachDatabase {
            ref schema_name,
            ref file_path,
        } = plan
        {
            return Ok(SqlResult::Execute(
                self.execute_attach(schema_name, file_path)?,
            ));
        }
        if let Plan::DetachDatabase { ref schema_name } = plan {
            return Ok(SqlResult::Execute(self.execute_detach(schema_name)?));
        }

        if is_query {
            Ok(SqlResult::Query(executor::execute(
                &plan,
                &mut self.pager,
                &self.catalog,
            )?))
        } else if plan_has_returning(&plan) {
            let result = executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)?;
            Ok(SqlResult::Query(result.returning.unwrap_or(QueryResult {
                columns: vec![],
                rows: vec![],
            })))
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
                    | Plan::Vacuum
                    | Plan::CreateTrigger { .. }
                    | Plan::DropTrigger { .. }
            );
            let result = executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)?;
            if is_ddl {
                self.plan_cache.clear();
            }
            Ok(SqlResult::Execute(result))
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    fn pragma_database_list(&self) -> QueryResult {
        use crate::types::Row;
        let mut rows = vec![Row {
            values: vec![
                Value::Integer(0),
                Value::Text("main".to_string()),
                Value::Text(String::new()),
            ],
        }];
        for (i, name) in self.attached.keys().enumerate() {
            rows.push(Row {
                values: vec![
                    Value::Integer((i + 1) as i64),
                    Value::Text(name.clone()),
                    Value::Text(String::new()),
                ],
            });
        }
        QueryResult {
            columns: vec!["seq".to_string(), "name".to_string(), "file".to_string()],
            rows,
        }
    }

    fn execute_attach(&mut self, schema_name: &str, file_path: &str) -> Result<ExecResult> {
        if schema_name.eq_ignore_ascii_case("main") || schema_name.eq_ignore_ascii_case("temp") {
            return Err(crate::error::Error::Other(format!(
                "cannot ATTACH database as '{schema_name}': reserved name"
            )));
        }
        if self.attached.contains_key(schema_name) {
            return Err(crate::error::Error::Other(format!(
                "database '{schema_name}' is already attached"
            )));
        }
        let mut pager = Pager::open(&*self.vfs, file_path)?;
        let catalog = Catalog::load(&mut pager)?;
        self.attached
            .insert(schema_name.to_string(), AttachedDb { pager, catalog });
        Ok(ExecResult::affected(0))
    }

    fn execute_detach(&mut self, schema_name: &str) -> Result<ExecResult> {
        if self.attached.remove(schema_name).is_none() {
            return Err(crate::error::Error::Other(format!(
                "no such database: {schema_name}"
            )));
        }
        Ok(ExecResult::affected(0))
    }
}

pub enum SqlResult {
    Query(QueryResult),
    Execute(ExecResult),
}

fn plan_has_returning(plan: &Plan) -> bool {
    match plan {
        Plan::Insert(p) => p.returning.is_some(),
        Plan::Update(p) => p.returning.is_some(),
        Plan::Delete(p) => p.returning.is_some(),
        _ => false,
    }
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
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}SCAN TABLE {table}")),
                ],
            });
        }
        Plan::IndexScan {
            table,
            index_columns,
            ..
        } => {
            let cols = index_columns.join(", ");
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}SEARCH TABLE {table} USING INDEX ({cols})")),
                ],
            });
        }
        Plan::IndexRangeScan {
            table,
            index_column,
            ..
        } => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!(
                        "{indent}SEARCH TABLE {table} USING INDEX ({index_column} range)"
                    )),
                ],
            });
        }
        Plan::Filter { input, .. } => {
            describe_plan_recursive(input, rows, id, parent, depth);
        }
        Plan::Project { input, .. } => {
            describe_plan_recursive(input, rows, id, parent, depth);
        }
        Plan::Sort { input, .. } => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}USE TEMP B-TREE FOR ORDER BY")),
                ],
            });
            describe_plan_recursive(input, rows, id, my_id, depth + 1);
        }
        Plan::Limit { input, .. } => {
            describe_plan_recursive(input, rows, id, parent, depth);
        }
        Plan::Aggregate { input, .. } => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}USE TEMP B-TREE FOR GROUP BY")),
                ],
            });
            describe_plan_recursive(input, rows, id, my_id, depth + 1);
        }
        Plan::NestedLoopJoin {
            left,
            right,
            join_type,
            ..
        } => {
            let jt = match join_type {
                planner::JoinType::Inner => "INNER",
                planner::JoinType::Left => "LEFT",
                planner::JoinType::Right => "RIGHT",
                planner::JoinType::Full => "FULL OUTER",
                planner::JoinType::Cross => "CROSS",
            };
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}NESTED LOOP {jt} JOIN")),
                ],
            });
            describe_plan_recursive(left, rows, id, my_id, depth + 1);
            describe_plan_recursive(right, rows, id, my_id, depth + 1);
        }
        Plan::Union { left, right, all } => {
            let op = if *all { "UNION ALL" } else { "UNION" };
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}COMPOUND QUERY ({op})")),
                ],
            });
            describe_plan_recursive(left, rows, id, my_id, depth + 1);
            describe_plan_recursive(right, rows, id, my_id, depth + 1);
        }
        Plan::Intersect { left, right, all } => {
            let op = if *all { "INTERSECT ALL" } else { "INTERSECT" };
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}COMPOUND QUERY ({op})")),
                ],
            });
            describe_plan_recursive(left, rows, id, my_id, depth + 1);
            describe_plan_recursive(right, rows, id, my_id, depth + 1);
        }
        Plan::Except { left, right, all } => {
            let op = if *all { "EXCEPT ALL" } else { "EXCEPT" };
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}COMPOUND QUERY ({op})")),
                ],
            });
            describe_plan_recursive(left, rows, id, my_id, depth + 1);
            describe_plan_recursive(right, rows, id, my_id, depth + 1);
        }
        Plan::Window { input, .. } => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}WINDOW FUNCTION")),
                ],
            });
            describe_plan_recursive(input, rows, id, my_id, depth + 1);
        }
        Plan::Insert(p) => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}INSERT INTO {}", p.table_name)),
                ],
            });
        }
        Plan::Update(p) => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}UPDATE {}", p.table_name)),
                ],
            });
        }
        Plan::Delete(p) => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}DELETE FROM {}", p.table_name)),
                ],
            });
        }
        Plan::SingleRow => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}SCAN CONSTANT ROW")),
                ],
            });
        }
        _ => {
            rows.push(Row {
                values: vec![
                    Value::Integer(my_id),
                    Value::Integer(parent),
                    Value::Integer(0),
                    Value::Text(format!("{indent}PLAN NODE")),
                ],
            });
        }
    }
}

#[cfg(test)]
#[path = "database_tests/mod.rs"]
mod tests;
