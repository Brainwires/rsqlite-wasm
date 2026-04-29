use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::ParseError;

pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>, ParseError> {
    let dialect = SQLiteDialect {};
    let preprocessed = preprocess_pragma(sql);
    let statements = Parser::parse_sql(&dialect, &preprocessed)?;
    Ok(statements)
}

fn preprocess_pragma(sql: &str) -> String {
    let trimmed = sql.trim();
    if !trimmed.to_uppercase().starts_with("PRAGMA ") {
        return sql.to_string();
    }
    let after_pragma = trimmed[7..].trim();
    if let Some(paren_start) = after_pragma.find('(') {
        if let Some(paren_end) = after_pragma.rfind(')') {
            let arg = after_pragma[paren_start + 1..paren_end].trim();
            if !arg.starts_with('\'') && !arg.starts_with('"') {
                let name = &after_pragma[..paren_start];
                let rest = if paren_end + 1 < after_pragma.len() {
                    &after_pragma[paren_end + 1..]
                } else {
                    ""
                };
                return format!("PRAGMA {name}('{arg}'){rest}");
            }
        }
    }
    sql.to_string()
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
