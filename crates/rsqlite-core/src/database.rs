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
        );
        let result = executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)?;
        if is_ddl {
            self.plan_cache.clear();
        }
        Ok(result)
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


#[cfg(test)]
#[path = "database_tests.rs"]
mod tests;
