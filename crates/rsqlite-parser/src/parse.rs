use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::ParseError;

pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>, ParseError> {
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select() {
        let stmts = parse_sql("SELECT * FROM users").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_create_table() {
        let stmts =
            parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_insert() {
        let stmts = parse_sql("INSERT INTO users (name) VALUES ('alice')").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_error() {
        assert!(parse_sql("SELECTT * FROM").is_err());
    }
}
