use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;
use rsqlite_vfs::Vfs;
use sqlparser::ast::Statement;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::executor::{self, ExecResult};
use crate::planner;
use crate::types::QueryResult;

pub struct Database {
    pager: Pager,
    catalog: Catalog,
}

impl Database {
    pub fn open(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::open(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self { pager, catalog })
    }

    pub fn create(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let mut pager = Pager::create(vfs, path)?;
        let catalog = Catalog::load(&mut pager)?;
        Ok(Self { pager, catalog })
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
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            });
        }

        let plan = planner::plan_statement(&stmts[0], &self.catalog)?;
        if let planner::Plan::Pragma { ref name, ref argument } = plan {
            return executor::execute_pragma(name, argument.as_deref(), &self.pager, &self.catalog);
        }
        executor::execute(&plan, &mut self.pager, &self.catalog)
    }

    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(ExecResult { rows_affected: 0 });
        }

        let plan = planner::plan_statement(&stmts[0], &self.catalog)?;
        executor::execute_mut(&plan, &mut self.pager, &mut self.catalog)
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<SqlResult> {
        let stmts = rsqlite_parser::parse::parse_sql(sql)?;
        if stmts.is_empty() {
            return Ok(SqlResult::Execute(ExecResult { rows_affected: 0 }));
        }

        let stmt = &stmts[0];
        let plan = planner::plan_statement(stmt, &self.catalog)?;

        if let planner::Plan::Pragma { ref name, ref argument } = plan {
            return Ok(SqlResult::Query(executor::execute_pragma(
                name,
                argument.as_deref(),
                &self.pager,
                &self.catalog,
            )?));
        }

        if is_query_statement(stmt) {
            Ok(SqlResult::Query(executor::execute(
                &plan,
                &mut self.pager,
                &self.catalog,
            )?))
        } else {
            Ok(SqlResult::Execute(executor::execute_mut(
                &plan,
                &mut self.pager,
                &mut self.catalog,
            )?))
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
