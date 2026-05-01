use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::ParseError;

pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>, ParseError> {
    let dialect = SQLiteDialect {};
    let preprocessed = preprocess_update_limit(&preprocess_bitwise_not(
        &preprocess_is_truth_family(&preprocess_pragma(sql)),
    ));
    if is_vacuum(&preprocessed) {
        return Ok(vec![make_pragma_statement("__vacuum", None)]);
    }
    if let Some(arg) = strip_keyword(&preprocessed, "REINDEX") {
        return Ok(vec![make_pragma_statement("__reindex", Some(&arg))]);
    }
    if let Some(arg) = strip_keyword(&preprocessed, "ANALYZE") {
        return Ok(vec![make_pragma_statement("__analyze", Some(&arg))]);
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
        "BEFORE" => {
            pos += 1;
            "BEFORE"
        }
        "AFTER" => {
            pos += 1;
            "AFTER"
        }
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
        "INSERT" => {
            pos += 1;
            "INSERT"
        }
        "UPDATE" => {
            pos += 1;
            "UPDATE"
        }
        "DELETE" => {
            pos += 1;
            "DELETE"
        }
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

fn find_keyword_pos(
    upper_sql: &str,
    _start_token: usize,
    _tokens: &[&str],
    keyword: &str,
) -> Option<usize> {
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

/// If `sql` starts with `keyword`, return everything after it (trimmed).
/// `REINDEX` and `ANALYZE` accept an optional table/index name, so the
/// argument may be empty.
fn strip_keyword(sql: &str, keyword: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();
    if upper == keyword {
        return Some(String::new());
    }
    let prefix = format!("{keyword} ");
    if upper.starts_with(&prefix) {
        return Some(trimmed[prefix.len()..].trim().to_string());
    }
    None
}

fn make_pragma_statement(name: &str, value: Option<&str>) -> sqlparser::ast::Statement {
    use sqlparser::ast::{Ident, ObjectName, Value};
    sqlparser::ast::Statement::Pragma {
        name: ObjectName::from(vec![Ident::new(name)]),
        value: value.map(|v| Value::SingleQuotedString(v.to_string())),
        is_eq: value.is_some(),
    }
}

/// Translate `<ident> IS [NOT] TRUE/FALSE` into the SQLite-equivalent
/// boolean form so SQLiteDialect (which rejects the syntax) accepts it:
///
/// - `x IS TRUE` → `(x IS NOT NULL AND x <> 0)`
/// - `x IS FALSE` → `(x IS NOT NULL AND x = 0)`
/// - `x IS NOT TRUE` → `(x IS NULL OR x = 0)`
/// - `x IS NOT FALSE` → `(x IS NULL OR x <> 0)`
///
/// Only matches when the LHS is a simple identifier (`col`, `t.col`,
/// `"quoted col"`). For arbitrary expressions on the left (`(a + b) IS
/// TRUE`), use the function form `is_true(...)` instead.
///
/// Skips matches inside string literals to avoid corrupting query text.
fn preprocess_is_truth_family(sql: &str) -> String {
    // Fast path: most SQL strings don't contain `IS TRUE` / `IS FALSE`, and
    // ASCII byte-walking the rare matches is incompatible with multi-byte
    // UTF-8 string literals like `'é'`. Skip the rewrite entirely when no
    // candidate keyword is present.
    let upper_haystack = sql.to_ascii_uppercase();
    if !upper_haystack.contains("IS TRUE")
        && !upper_haystack.contains("IS FALSE")
        && !upper_haystack.contains("IS NOT TRUE")
        && !upper_haystack.contains("IS NOT FALSE")
    {
        return sql.to_string();
    }

    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(n);
    let mut i = 0usize;

    while i < n {
        let ch = bytes[i] as char;
        // Skip past string literals untouched.
        if ch == '\'' || ch == '"' {
            let quote = ch;
            out.push(ch);
            i += 1;
            while i < n {
                let c = bytes[i] as char;
                out.push(c);
                i += 1;
                if c == quote {
                    // SQL doubled-quote escape: '' inside ''.
                    if i < n && (bytes[i] as char) == quote {
                        out.push(quote);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }
        // Skip past line comments.
        if ch == '-' && i + 1 < n && bytes[i + 1] as char == '-' {
            while i < n && bytes[i] as char != '\n' {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }

        // Try to match an identifier optionally followed by IS [NOT] TRUE/FALSE.
        let id_start = i;
        let id = scan_identifier(bytes, &mut i);
        if id.is_empty() {
            out.push(ch);
            i += 1;
            continue;
        }
        // Look for `IS [NOT] (TRUE|FALSE)` after optional whitespace.
        let saved_i = i;
        let mut j = i;
        skip_ws(bytes, &mut j);
        if !match_keyword(bytes, &mut j, "IS") {
            // Not the pattern; emit the identifier as-is.
            out.push_str(&id);
            continue;
        }
        skip_ws(bytes, &mut j);
        let negated = match_keyword(bytes, &mut j, "NOT");
        if negated {
            skip_ws(bytes, &mut j);
        }
        let truthy = if match_keyword(bytes, &mut j, "TRUE") {
            Some(true)
        } else if match_keyword(bytes, &mut j, "FALSE") {
            Some(false)
        } else {
            None
        };
        match truthy {
            Some(true_or_false) => {
                // IS TRUE      → (x IS NOT NULL AND x <> 0)
                // IS FALSE     → (x IS NOT NULL AND x = 0)
                // IS NOT TRUE  → (x IS NULL OR x = 0)
                // IS NOT FALSE → (x IS NULL OR x <> 0)
                let cmp = if true_or_false ^ negated { "<>" } else { "=" };
                let null_test = if negated { "IS NULL" } else { "IS NOT NULL" };
                let combiner = if negated { "OR" } else { "AND" };
                let expanded = format!("({id} {null_test} {combiner} {id} {cmp} 0)");
                let _ = id_start;
                out.push_str(&expanded);
                i = j;
            }
            None => {
                out.push_str(&id);
                i = saved_i;
            }
        }
    }

    out
}

fn scan_identifier(bytes: &[u8], i: &mut usize) -> String {
    let start = *i;
    let n = bytes.len();
    if start >= n {
        return String::new();
    }
    let first = bytes[start] as char;
    // Quoted identifier: "name" or `name`.
    if first == '"' || first == '`' {
        let quote = first;
        *i += 1;
        while *i < n {
            let c = bytes[*i] as char;
            *i += 1;
            if c == quote {
                break;
            }
        }
        // Optional `.qualifier` after a quoted identifier.
        if *i < n && bytes[*i] as char == '.' {
            *i += 1;
            scan_unquoted_id_tail(bytes, i);
        }
        return String::from_utf8_lossy(&bytes[start..*i]).into_owned();
    }
    if !first.is_ascii_alphabetic() && first != '_' {
        return String::new();
    }
    *i += 1;
    while *i < n {
        let c = bytes[*i] as char;
        if c.is_ascii_alphanumeric() || c == '_' {
            *i += 1;
        } else {
            break;
        }
    }
    // Optional `.qualifier`.
    if *i < n && bytes[*i] as char == '.' {
        *i += 1;
        scan_unquoted_id_tail(bytes, i);
    }
    String::from_utf8_lossy(&bytes[start..*i]).into_owned()
}

fn scan_unquoted_id_tail(bytes: &[u8], i: &mut usize) {
    let n = bytes.len();
    while *i < n {
        let c = bytes[*i] as char;
        if c.is_ascii_alphanumeric() || c == '_' {
            *i += 1;
        } else {
            break;
        }
    }
}

fn skip_ws(bytes: &[u8], i: &mut usize) {
    let n = bytes.len();
    while *i < n && (bytes[*i] as char).is_whitespace() {
        *i += 1;
    }
}

fn match_keyword(bytes: &[u8], i: &mut usize, kw: &str) -> bool {
    let n = bytes.len();
    let kw_bytes = kw.as_bytes();
    if *i + kw_bytes.len() > n {
        return false;
    }
    for (k, kb) in kw_bytes.iter().enumerate() {
        let c = bytes[*i + k];
        if c.to_ascii_uppercase() != *kb {
            return false;
        }
    }
    // Make sure the next char isn't an identifier continuation.
    let after = *i + kw_bytes.len();
    if after < n {
        let c = bytes[after] as char;
        if c.is_ascii_alphanumeric() || c == '_' {
            return false;
        }
    }
    *i = after;
    true
}

/// Translate prefix `~<operand>` (bitwise complement) into `__bnot(<operand>)`
/// before parsing — sqlparser's SQLiteDialect rejects `~` as a unary token.
///
/// The operand can be:
/// - a simple identifier (`col`, `t.col`, `"quoted col"`)
/// - a parenthesized expression (`~(a + 1)` → `__bnot((a + 1))`)
///
/// Other forms (function calls, longer chains) fall through and the user
/// uses the function form `__bnot(...)` directly.
///
/// Skips matches inside string literals, line comments, and `~` chars used
/// as a binary operator (the SQLite dialect doesn't expose that anyway,
/// but Postgres-style `~` regex match isn't our concern here).
fn preprocess_bitwise_not(sql: &str) -> String {
    if !sql.contains('~') {
        return sql.to_string();
    }

    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(n);
    let mut i = 0usize;

    while i < n {
        let ch = bytes[i] as char;
        if ch == '\'' || ch == '"' {
            let quote = ch;
            out.push(ch);
            i += 1;
            while i < n {
                let c = bytes[i] as char;
                out.push(c);
                i += 1;
                if c == quote {
                    if i < n && (bytes[i] as char) == quote {
                        out.push(quote);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }
        if ch == '-' && i + 1 < n && bytes[i + 1] as char == '-' {
            while i < n && bytes[i] as char != '\n' {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if ch != '~' {
            out.push(ch);
            i += 1;
            continue;
        }

        // Decide whether this `~` is in a prefix position. It's a prefix
        // iff the previous *significant* output character is the start of
        // a fresh expression — i.e. one of: empty, `(`, `,`, operator,
        // keyword separator. We approximate by walking back through any
        // trailing whitespace.
        let prev_significant = out.trim_end().chars().next_back();
        let is_prefix = match prev_significant {
            None => true,
            Some(c) => matches!(
                c,
                '(' | ',' | '+' | '-' | '*' | '/' | '%' | '=' | '<' | '>' | '!' | '|' | '&' | '~'
            ),
        } || {
            // Or the preceding token is an SQL keyword that ends an
            // expression boundary (WHERE, AND, OR, NOT, ON, BY, ...).
            let trimmed = out.trim_end();
            let last_word: String = trimmed
                .chars()
                .rev()
                .take_while(|c| c.is_ascii_alphabetic() || *c == '_')
                .collect::<String>()
                .chars()
                .rev()
                .collect();
            matches!(
                last_word.to_ascii_uppercase().as_str(),
                "WHERE"
                    | "AND"
                    | "OR"
                    | "NOT"
                    | "ON"
                    | "BY"
                    | "CASE"
                    | "WHEN"
                    | "THEN"
                    | "ELSE"
                    | "SELECT"
                    | "VALUES"
                    | "RETURNING"
                    | "HAVING"
                    | "LIMIT"
                    | "OFFSET"
            )
        };

        if !is_prefix {
            out.push(ch);
            i += 1;
            continue;
        }

        // Consume `~` and parse the operand.
        i += 1;
        skip_ws(bytes, &mut i);
        if i >= n {
            out.push('~');
            continue;
        }
        let next_ch = bytes[i] as char;
        if next_ch == '(' {
            // Find matching close paren.
            let start = i;
            let mut depth = 1;
            i += 1;
            while i < n && depth > 0 {
                let c = bytes[i] as char;
                if c == '(' {
                    depth += 1;
                } else if c == ')' {
                    depth -= 1;
                }
                i += 1;
            }
            let operand = &sql[start..i];
            out.push_str(&format!("__bnot({operand})"));
            continue;
        }
        // Identifier (possibly qualified).
        let id = scan_identifier(bytes, &mut i);
        if id.is_empty() {
            out.push('~');
            continue;
        }
        out.push_str(&format!("__bnot({id})"));
    }

    out
}

/// Translate `UPDATE t SET ... [WHERE ...] (ORDER BY ... | LIMIT n)` into
/// the rowid-IN workaround SQLite documents:
///
///   UPDATE t SET ... WHERE rowid IN (
///     SELECT rowid FROM t [WHERE ...] [ORDER BY ...] [LIMIT n]
///   )
///
/// SQLite gates UPDATE-with-LIMIT/ORDER-BY behind a compile-time flag and
/// `sqlparser`'s SQLiteDialect doesn't accept it, so without this pre-pass
/// the user has to write the rowid-IN form by hand.
///
/// Conservative: only touches single-statement inputs that start with
/// `UPDATE`. Bails on multi-table UPDATE-FROM (where rewriting requires
/// also threading the FROM clause), or any case where the table name
/// can't be cleanly extracted.
fn preprocess_update_limit(sql: &str) -> String {
    let upper = sql.to_ascii_uppercase();
    let trimmed_upper = upper.trim_start();
    if !trimmed_upper.starts_with("UPDATE ") {
        return sql.to_string();
    }
    if !upper.contains(" ORDER BY ") && !upper.contains(" LIMIT ") {
        return sql.to_string();
    }

    let kw_pos = find_top_level_keywords(sql, &["SET", "FROM", "WHERE", "ORDER", "LIMIT"]);
    let set_pos = match kw_pos.iter().find(|(k, _)| k == "SET") {
        Some((_, p)) => *p,
        None => return sql.to_string(),
    };
    if kw_pos.iter().any(|(k, _)| k == "FROM") {
        // UPDATE...FROM is a separate beast — leave it alone.
        return sql.to_string();
    }
    let where_pos = kw_pos.iter().find(|(k, _)| k == "WHERE").map(|(_, p)| *p);
    let order_pos = kw_pos.iter().find(|(k, _)| k == "ORDER").map(|(_, p)| *p);
    let limit_pos = kw_pos.iter().find(|(k, _)| k == "LIMIT").map(|(_, p)| *p);

    if order_pos.is_none() && limit_pos.is_none() {
        return sql.to_string();
    }

    // Locate UPDATE keyword start (after any leading whitespace).
    let leading_ws = sql.len() - sql.trim_start().len();
    let table_section_start = leading_ws + "UPDATE".len();
    let table_section = sql[table_section_start..set_pos].trim();
    if table_section.is_empty() {
        return sql.to_string();
    }
    // Strip an optional alias clause for the rowid subquery — keep the
    // outer UPDATE table name verbatim.
    let table_for_subquery = table_section
        .split_whitespace()
        .next()
        .unwrap_or(table_section);

    let assignments_end = where_pos
        .or(order_pos)
        .or(limit_pos)
        .unwrap_or(sql.len());
    let assignments = sql[set_pos + "SET".len()..assignments_end].trim();
    if assignments.is_empty() {
        return sql.to_string();
    }

    let where_body = where_pos.map(|wp| {
        let end = order_pos.or(limit_pos).unwrap_or(sql.len());
        sql[wp + "WHERE".len()..end].trim()
    });

    let order_body = order_pos.map(|op| {
        // Skip past "ORDER" then optional whitespace then "BY".
        let after = sql[op + "ORDER".len()..].trim_start();
        let by_skip = sql[op + "ORDER".len()..].len() - after.len();
        let body_start = if after.to_ascii_uppercase().starts_with("BY") {
            op + "ORDER".len() + by_skip + "BY".len()
        } else {
            op + "ORDER".len() + by_skip
        };
        let end = limit_pos.unwrap_or(sql.len());
        sql[body_start..end].trim()
    });

    let limit_body = limit_pos.map(|lp| {
        sql[lp + "LIMIT".len()..]
            .trim()
            .trim_end_matches(';')
            .trim()
    });

    let mut sub = format!("SELECT rowid FROM {table_for_subquery}");
    if let Some(w) = where_body {
        sub.push_str(" WHERE ");
        sub.push_str(w);
    }
    if let Some(o) = order_body {
        sub.push_str(" ORDER BY ");
        sub.push_str(o);
    }
    if let Some(l) = limit_body {
        sub.push_str(" LIMIT ");
        sub.push_str(l);
    }

    format!("UPDATE {table_section} SET {assignments} WHERE rowid IN ({sub})")
}

/// Find positions of `keywords` that appear at the top level of `sql`
/// (i.e. not inside a parenthesized subexpression and not inside a string
/// literal). Each match must be at a word boundary on both sides.
/// Keywords are matched case-insensitively.
fn find_top_level_keywords(sql: &str, keywords: &[&str]) -> Vec<(String, usize)> {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut out: Vec<(String, usize)> = Vec::new();
    let mut i = 0usize;
    let mut depth = 0i32;

    while i < n {
        let ch = bytes[i] as char;
        // Skip string literals.
        if ch == '\'' || ch == '"' {
            let quote = ch;
            i += 1;
            while i < n {
                let c = bytes[i] as char;
                i += 1;
                if c == quote {
                    if i < n && (bytes[i] as char) == quote {
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }
        // Skip line comments.
        if ch == '-' && i + 1 < n && bytes[i + 1] as char == '-' {
            while i < n && bytes[i] as char != '\n' {
                i += 1;
            }
            continue;
        }
        if ch == '(' {
            depth += 1;
            i += 1;
            continue;
        }
        if ch == ')' {
            depth -= 1;
            i += 1;
            continue;
        }
        if depth != 0 {
            i += 1;
            continue;
        }

        let prev_is_word = i > 0 && {
            let p = bytes[i - 1] as char;
            p.is_ascii_alphanumeric() || p == '_'
        };
        if !prev_is_word {
            for kw in keywords {
                let kb = kw.as_bytes();
                if i + kb.len() > n {
                    continue;
                }
                let mut ok = true;
                for (k, b) in kb.iter().enumerate() {
                    if bytes[i + k].to_ascii_uppercase() != *b {
                        ok = false;
                        break;
                    }
                }
                if !ok {
                    continue;
                }
                if i + kb.len() < n {
                    let nc = bytes[i + kb.len()] as char;
                    if nc.is_ascii_alphanumeric() || nc == '_' {
                        continue;
                    }
                }
                out.push((kw.to_string(), i));
                break;
            }
        }
        i += 1;
    }
    out
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
        if val.eq_ignore_ascii_case("ON")
            || val.eq_ignore_ascii_case("YES")
            || val.eq_ignore_ascii_case("TRUE")
        {
            return format!("PRAGMA {name} = 1;");
        }
        if val.eq_ignore_ascii_case("OFF")
            || val.eq_ignore_ascii_case("NO")
            || val.eq_ignore_ascii_case("FALSE")
        {
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

    /// Helper: assert the SQL parsed into a `__pragma` statement with the
    /// given internal name (e.g. "__vacuum", "__reindex", "__detach").
    fn assert_pseudo_pragma(sql: &str, expected_name: &str) {
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1, "expected one statement for {sql:?}");
        match &stmts[0] {
            sqlparser::ast::Statement::Pragma { name, .. } => {
                assert_eq!(
                    name.to_string().to_lowercase(),
                    expected_name,
                    "wrong pseudo-pragma for {sql:?}"
                );
            }
            other => panic!("expected pragma statement for {sql:?}, got {other:?}"),
        }
    }

    // ---- VACUUM / REINDEX / ANALYZE pre-processing ----

    #[test]
    fn parse_vacuum_bare() {
        assert_pseudo_pragma("VACUUM", "__vacuum");
        assert_pseudo_pragma("vacuum;", "__vacuum");
    }

    #[test]
    fn parse_reindex_no_arg() {
        assert_pseudo_pragma("REINDEX", "__reindex");
    }

    #[test]
    fn parse_reindex_with_target() {
        assert_pseudo_pragma("REINDEX my_index", "__reindex");
        assert_pseudo_pragma("reindex MY_TABLE;", "__reindex");
    }

    #[test]
    fn parse_analyze_no_arg() {
        assert_pseudo_pragma("ANALYZE", "__analyze");
        assert_pseudo_pragma("analyze ;", "__analyze");
    }

    #[test]
    fn parse_analyze_with_target() {
        assert_pseudo_pragma("ANALYZE users", "__analyze");
    }

    // ---- DETACH ----

    #[test]
    fn parse_detach_database() {
        assert_pseudo_pragma("DETACH DATABASE aux", "__detach");
        assert_pseudo_pragma("DETACH aux;", "__detach");
    }

    // ---- CREATE TRIGGER (pre-processed into pseudo-pragma) ----

    #[test]
    fn parse_create_trigger_after_insert() {
        assert_pseudo_pragma(
            "CREATE TRIGGER tlog AFTER INSERT ON t BEGIN SELECT 1; END",
            "__create_trigger",
        );
    }

    #[test]
    fn parse_create_trigger_before_update_for_each_row() {
        assert_pseudo_pragma(
            "CREATE TRIGGER t1 BEFORE UPDATE ON things FOR EACH ROW BEGIN SELECT 1; END",
            "__create_trigger",
        );
    }

    #[test]
    fn parse_create_trigger_if_not_exists() {
        assert_pseudo_pragma(
            "CREATE TRIGGER IF NOT EXISTS t1 AFTER DELETE ON x BEGIN SELECT 1; END",
            "__create_trigger",
        );
    }

    #[test]
    fn parse_drop_trigger() {
        assert_pseudo_pragma("DROP TRIGGER tlog", "__drop_trigger");
        assert_pseudo_pragma("DROP TRIGGER IF EXISTS tlog", "__drop_trigger");
    }

    // ---- PRAGMA preprocessing ----

    #[test]
    fn parse_pragma_paren_form() {
        // PRAGMA table_info(users) — bare identifier in parens needs quoting
        // for sqlparser; preprocess wraps it.
        let stmts = parse_sql("PRAGMA table_info(users)").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(
            &stmts[0],
            sqlparser::ast::Statement::Pragma { .. }
        ));
    }

    #[test]
    fn parse_pragma_equals_form() {
        let stmts = parse_sql("PRAGMA foreign_keys = ON").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(
            &stmts[0],
            sqlparser::ast::Statement::Pragma { .. }
        ));
    }

    #[test]
    fn parse_pragma_simple_read_form() {
        let stmts = parse_sql("PRAGMA page_size").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(
            &stmts[0],
            sqlparser::ast::Statement::Pragma { .. }
        ));
    }

    // ---- Multi-statement / generic pass-through ----

    #[test]
    fn parse_multiple_statements() {
        let stmts = parse_sql("SELECT 1; SELECT 2;").unwrap();
        assert_eq!(stmts.len(), 2);
    }
}

#[cfg(test)]
mod update_limit_tests {
    use super::*;

    #[test]
    fn rewrite_limit_only() {
        let out = preprocess_update_limit("UPDATE t SET status = 'new' LIMIT 2");
        assert!(out.contains("LIMIT 2"), "out={out}");
        assert!(out.contains("rowid IN ("), "out={out}");
    }

    #[test]
    fn rewrite_where_and_limit() {
        let out = preprocess_update_limit(
            "UPDATE t SET status = 'updated' WHERE status = 'a' LIMIT 2",
        );
        assert!(out.contains("WHERE status = 'a'"), "out={out}");
        assert!(out.contains("LIMIT 2"));
        assert!(out.contains("rowid IN ("));
    }

    #[test]
    fn no_op_without_order_by_or_limit() {
        let sql = "UPDATE t SET status = 'x' WHERE id = 1";
        let out = preprocess_update_limit(sql);
        assert_eq!(out, sql);
    }

    #[test]
    fn skips_string_literal_limit() {
        let sql = "UPDATE t SET label = 'has LIMIT in it' WHERE id = 1";
        let out = preprocess_update_limit(sql);
        assert_eq!(out, sql);
    }
}
