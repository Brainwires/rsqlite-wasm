use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::ParseError;

pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>, ParseError> {
    let dialect = SQLiteDialect {};
    let preprocessed = preprocess_pragma(sql);
    if is_vacuum(&preprocessed) {
        return Ok(vec![make_pragma_statement("__vacuum", None)]);
    }
    if let Some(stmt) = parse_trigger_statement(&preprocessed) {
        return Ok(vec![stmt]);
    }
    if let Some(stmt) = parse_detach_statement(&preprocessed) {
        return Ok(vec![stmt]);
    }
    let statements = Parser::parse_sql(&dialect, &preprocessed)?;
    Ok(statements)
}

fn parse_trigger_statement(sql: &str) -> Option<sqlparser::ast::Statement> {
    let upper = sql.trim().to_uppercase();
    if upper.starts_with("CREATE TRIGGER") || upper.starts_with("CREATE TRIGGER") {
        return parse_create_trigger(sql.trim());
    }
    if upper.starts_with("DROP TRIGGER") {
        return parse_drop_trigger(sql.trim());
    }
    None
}

fn parse_create_trigger(sql: &str) -> Option<sqlparser::ast::Statement> {
    let original_sql = sql.trim().trim_end_matches(';').trim();
    let upper = original_sql.to_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();

    let mut pos = 2; // skip "CREATE TRIGGER"
    let mut if_not_exists = false;
    if tokens.get(pos) == Some(&"IF")
        && tokens.get(pos + 1) == Some(&"NOT")
        && tokens.get(pos + 2) == Some(&"EXISTS")
    {
        if_not_exists = true;
        pos += 3;
    }

    let name = tokens.get(pos)?.to_string();
    pos += 1;

    let timing = match tokens.get(pos).copied()? {
        "BEFORE" => { pos += 1; "BEFORE" }
        "AFTER" => { pos += 1; "AFTER" }
        "INSTEAD" => {
            if tokens.get(pos + 1) == Some(&"OF") {
                pos += 2;
                "INSTEAD OF"
            } else {
                return None;
            }
        }
        _ => return None,
    };

    let event = match tokens.get(pos).copied()? {
        "INSERT" => { pos += 1; "INSERT" }
        "UPDATE" => { pos += 1; "UPDATE" }
        "DELETE" => { pos += 1; "DELETE" }
        _ => return None,
    };

    if tokens.get(pos) != Some(&"ON") {
        return None;
    }
    pos += 1;

    let table_name = tokens.get(pos)?.to_string();
    pos += 1;

    if tokens.get(pos) == Some(&"FOR") {
        if tokens.get(pos + 1) == Some(&"EACH") && tokens.get(pos + 2) == Some(&"ROW") {
            pos += 3;
        }
    }

    // Find BEGIN in the original (case-preserving) text
    let upper_sql = original_sql.to_uppercase();
    let begin_idx = find_keyword_pos(&upper_sql, pos, &tokens, "BEGIN")?;

    let when_condition = if tokens.get(pos) == Some(&"WHEN") {
        let when_start = find_word_offset(original_sql, &tokens, pos + 1)?;
        let when_end = begin_idx;
        let cond = original_sql[when_start..when_end].trim().to_string();
        Some(cond)
    } else {
        None
    };

    let body_start = begin_idx + "BEGIN".len();
    let end_idx = upper_sql.rfind("END")?;
    let body_sql = original_sql[body_start..end_idx].trim().to_string();

    let encoded = format!(
        "{}|{}|{}|{}|{}|{}|{}",
        name,
        table_name,
        timing,
        event,
        if if_not_exists { "1" } else { "0" },
        when_condition.as_deref().unwrap_or(""),
        body_sql
    );

    Some(make_pragma_statement("__create_trigger", Some(&encoded)))
}

fn find_keyword_pos(upper_sql: &str, _start_token: usize, _tokens: &[&str], keyword: &str) -> Option<usize> {
    upper_sql.find(keyword)
}

fn find_word_offset(sql: &str, tokens: &[&str], token_idx: usize) -> Option<usize> {
    let upper = sql.to_uppercase();
    let mut search_start = 0;
    for i in 0..token_idx {
        let tok = tokens.get(i)?;
        if let Some(pos) = upper[search_start..].find(tok) {
            search_start += pos + tok.len();
        }
    }
    while search_start < sql.len() && sql.as_bytes()[search_start] == b' ' {
        search_start += 1;
    }
    Some(search_start)
}

fn parse_detach_statement(sql: &str) -> Option<sqlparser::ast::Statement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("DETACH") {
        return None;
    }
    let upper_tokens: Vec<&str> = upper.split_whitespace().collect();
    let orig_tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let mut pos = 1; // skip DETACH
    if upper_tokens.get(pos) == Some(&"DATABASE") {
        pos += 1;
    }
    let schema_name = orig_tokens.get(pos)?;
    Some(make_pragma_statement("__detach", Some(schema_name)))
}

fn parse_drop_trigger(sql: &str) -> Option<sqlparser::ast::Statement> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();

    let mut pos = 2; // skip "DROP TRIGGER"
    let mut if_exists = false;
    if tokens.get(pos) == Some(&"IF") && tokens.get(pos + 1) == Some(&"EXISTS") {
        if_exists = true;
        pos += 2;
    }
    let name = tokens.get(pos)?.to_string();
    let encoded = format!("{}|{}", name, if if_exists { "1" } else { "0" });
    Some(make_pragma_statement("__drop_trigger", Some(&encoded)))
}

fn is_vacuum(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    trimmed.eq_ignore_ascii_case("VACUUM")
}

fn make_pragma_statement(name: &str, value: Option<&str>) -> sqlparser::ast::Statement {
    use sqlparser::ast::{Ident, ObjectName, Value};
    sqlparser::ast::Statement::Pragma {
        name: ObjectName::from(vec![Ident::new(name)]),
        value: value.map(|v| Value::SingleQuotedString(v.to_string())),
        is_eq: value.is_some(),
    }
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
    if let Some(eq_pos) = after_pragma.find('=') {
        let name = after_pragma[..eq_pos].trim();
        let val = after_pragma[eq_pos + 1..].trim().trim_end_matches(';');
        let val = val.trim();
        if val.eq_ignore_ascii_case("ON") || val.eq_ignore_ascii_case("YES") || val.eq_ignore_ascii_case("TRUE") {
            return format!("PRAGMA {name} = 1;");
        }
        if val.eq_ignore_ascii_case("OFF") || val.eq_ignore_ascii_case("NO") || val.eq_ignore_ascii_case("FALSE") {
            return format!("PRAGMA {name} = 0;");
        }
        if !val.starts_with('\'') && !val.starts_with('"') && val.parse::<i64>().is_err() {
            return format!("PRAGMA {name} = '{val}';");
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
