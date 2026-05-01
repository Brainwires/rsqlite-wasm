use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    pub fn to_string_repr(&self) -> String {
        match self {
            JsonValue::Null => "null".into(),
            JsonValue::Bool(b) => if *b { "true" } else { "false" }.into(),
            JsonValue::Number(n) => {
                if *n == (*n as i64) as f64 && n.is_finite() {
                    format!("{}", *n as i64)
                } else {
                    format!("{n}")
                }
            }
            JsonValue::String(s) => {
                let escaped = s
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t");
                format!("\"{escaped}\"")
            }
            JsonValue::Array(arr) => {
                let parts: Vec<String> = arr.iter().map(|v| v.to_string_repr()).collect();
                format!("[{}]", parts.join(","))
            }
            JsonValue::Object(obj) => {
                let parts: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| {
                        let ek = k.replace('\\', "\\\\").replace('"', "\\\"");
                        format!("\"{}\":{}", ek, v.to_string_repr())
                    })
                    .collect();
                format!("{{{}}}", parts.join(","))
            }
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            JsonValue::Null => "null",
            JsonValue::Bool(true) => "true",
            JsonValue::Bool(false) => "false",
            JsonValue::Number(_) => "real",
            JsonValue::String(_) => "text",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        }
    }

    pub fn extract_path(&self, path: &str) -> Option<&JsonValue> {
        if path == "$" {
            return Some(self);
        }
        let rest = path.strip_prefix('$')?;
        let mut current = self;
        let mut chars = rest.chars().peekable();

        while chars.peek().is_some() {
            match chars.peek() {
                Some('.') => {
                    chars.next();
                    let mut key = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == '.' || c == '[' {
                            break;
                        }
                        key.push(c);
                        chars.next();
                    }
                    if let JsonValue::Object(obj) = current {
                        current = obj.iter().find(|(k, _)| k == &key).map(|(_, v)| v)?;
                    } else {
                        return None;
                    }
                }
                Some('[') => {
                    chars.next();
                    let mut idx_str = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == ']' {
                            break;
                        }
                        idx_str.push(c);
                        chars.next();
                    }
                    if chars.peek() == Some(&']') {
                        chars.next();
                    }
                    let idx: usize = idx_str.parse().ok()?;
                    if let JsonValue::Array(arr) = current {
                        current = arr.get(idx)?;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(current)
    }
}

pub(crate) fn parse_json(s: &str) -> Result<JsonValue> {
    let trimmed = s.trim();
    let (val, rest) = parse_value(trimmed)?;
    if !rest.trim().is_empty() {
        return Err(Error::Other("malformed JSON: trailing content".into()));
    }
    Ok(val)
}

fn parse_value(s: &str) -> Result<(JsonValue, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return Err(Error::Other("unexpected end of JSON".into()));
    }
    match s.as_bytes()[0] {
        b'"' => parse_string_val(s),
        b'{' => parse_object(s),
        b'[' => parse_array(s),
        b't' | b'f' => parse_bool(s),
        b'n' => parse_null(s),
        _ => parse_number(s),
    }
}

fn parse_string_val(s: &str) -> Result<(JsonValue, &str)> {
    let (st, rest) = parse_string_raw(s)?;
    Ok((JsonValue::String(st), rest))
}

fn parse_string_raw(s: &str) -> Result<(String, &str)> {
    if !s.starts_with('"') {
        return Err(Error::Other("expected '\"'".into()));
    }
    let mut result = String::new();
    let mut chars = s[1..].char_indices();
    loop {
        match chars.next() {
            None => return Err(Error::Other("unterminated string".into())),
            Some((_, '"')) => {
                let pos = 1 + result.len() + count_escapes(&s[1..], result.len());
                let rest = &s[pos + 1..];
                return Ok((result, rest));
            }
            Some((_, '\\')) => match chars.next() {
                Some((_, '"')) => result.push('"'),
                Some((_, '\\')) => result.push('\\'),
                Some((_, '/')) => result.push('/'),
                Some((_, 'n')) => result.push('\n'),
                Some((_, 'r')) => result.push('\r'),
                Some((_, 't')) => result.push('\t'),
                Some((_, 'b')) => result.push('\u{0008}'),
                Some((_, 'f')) => result.push('\u{000C}'),
                Some((_, 'u')) => {
                    let mut hex = String::new();
                    for _ in 0..4 {
                        if let Some((_, c)) = chars.next() {
                            hex.push(c);
                        }
                    }
                    if let Ok(cp) = u32::from_str_radix(&hex, 16) {
                        if let Some(c) = char::from_u32(cp) {
                            result.push(c);
                        }
                    }
                }
                _ => {}
            },
            Some((_, c)) => result.push(c),
        }
    }
}

fn count_escapes(s: &str, result_len: usize) -> usize {
    let mut escapes = 0;
    let mut i = 0;
    let mut result_count = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() && result_count < result_len {
        if bytes[i] == b'\\' {
            escapes += 1;
            i += 1;
            if i < bytes.len() {
                if bytes[i] == b'u' {
                    escapes += 4;
                    i += 4;
                }
                i += 1;
            }
        } else {
            i += 1;
        }
        result_count += 1;
    }
    escapes
}

fn parse_object(s: &str) -> Result<(JsonValue, &str)> {
    let mut rest = s[1..].trim_start();
    let mut entries = Vec::new();
    if rest.starts_with('}') {
        return Ok((JsonValue::Object(entries), &rest[1..]));
    }
    loop {
        let (key, r) = parse_string_raw(rest)?;
        rest = r.trim_start();
        if !rest.starts_with(':') {
            return Err(Error::Other("expected ':'".into()));
        }
        rest = rest[1..].trim_start();
        let (val, r) = parse_value(rest)?;
        rest = r.trim_start();
        entries.push((key, val));
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if rest.starts_with('}') {
            rest = &rest[1..];
            break;
        } else {
            return Err(Error::Other("expected ',' or '}'".into()));
        }
    }
    Ok((JsonValue::Object(entries), rest))
}

fn parse_array(s: &str) -> Result<(JsonValue, &str)> {
    let mut rest = s[1..].trim_start();
    let mut items = Vec::new();
    if rest.starts_with(']') {
        return Ok((JsonValue::Array(items), &rest[1..]));
    }
    loop {
        let (val, r) = parse_value(rest)?;
        rest = r.trim_start();
        items.push(val);
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if rest.starts_with(']') {
            rest = &rest[1..];
            break;
        } else {
            return Err(Error::Other("expected ',' or ']'".into()));
        }
    }
    Ok((JsonValue::Array(items), rest))
}

fn parse_bool(s: &str) -> Result<(JsonValue, &str)> {
    if let Some(rest) = s.strip_prefix("true") {
        Ok((JsonValue::Bool(true), rest))
    } else if let Some(rest) = s.strip_prefix("false") {
        Ok((JsonValue::Bool(false), rest))
    } else {
        Err(Error::Other("expected 'true' or 'false'".into()))
    }
}

fn parse_null(s: &str) -> Result<(JsonValue, &str)> {
    if let Some(rest) = s.strip_prefix("null") {
        Ok((JsonValue::Null, rest))
    } else {
        Err(Error::Other("expected 'null'".into()))
    }
}

fn parse_number(s: &str) -> Result<(JsonValue, &str)> {
    let mut end = 0;
    let bytes = s.as_bytes();
    if end < bytes.len() && bytes[end] == b'-' {
        end += 1;
    }
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end < bytes.len() && bytes[end] == b'.' {
        end += 1;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end < bytes.len() && (bytes[end] == b'e' || bytes[end] == b'E') {
        end += 1;
        if end < bytes.len() && (bytes[end] == b'+' || bytes[end] == b'-') {
            end += 1;
        }
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
    }
    if end == 0 {
        return Err(Error::Other("expected number".into()));
    }
    let num_str = &s[..end];
    let n: f64 = num_str
        .parse()
        .map_err(|_| Error::Other(format!("invalid number: {num_str}")))?;
    Ok((JsonValue::Number(n), &s[end..]))
}

pub(crate) fn json_value_to_sql(jv: &JsonValue) -> Value {
    match jv {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Number(n) => {
            if *n == (*n as i64) as f64 && n.is_finite() {
                Value::Integer(*n as i64)
            } else {
                Value::Real(*n)
            }
        }
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(jv.to_string_repr()),
    }
}

fn sql_to_json_value(v: &Value) -> JsonValue {
    match v {
        Value::Null => JsonValue::Null,
        Value::Integer(n) => JsonValue::Number(*n as f64),
        Value::Real(f) => JsonValue::Number(*f),
        Value::Text(s) => {
            if let Ok(jv) = parse_json(s) {
                jv
            } else {
                JsonValue::String(s.clone())
            }
        }
        Value::Blob(_) => JsonValue::Null,
    }
}

enum PathSegment {
    Key(String),
    Index(usize),
}

fn parse_path_segments(path: &str) -> Option<Vec<PathSegment>> {
    let rest = path.strip_prefix('$')?;
    let mut segments = Vec::new();
    let mut chars = rest.chars().peekable();
    while chars.peek().is_some() {
        match chars.peek() {
            Some('.') => {
                chars.next();
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                segments.push(PathSegment::Key(key));
            }
            Some('[') => {
                chars.next();
                let mut idx_str = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        break;
                    }
                    idx_str.push(c);
                    chars.next();
                }
                if chars.peek() == Some(&']') {
                    chars.next();
                }
                let idx: usize = idx_str.parse().ok()?;
                segments.push(PathSegment::Index(idx));
            }
            _ => return None,
        }
    }
    Some(segments)
}

fn set_path(root: &mut JsonValue, path: &str, val: JsonValue, insert: bool, replace: bool) {
    let segments = match parse_path_segments(path) {
        Some(s) if !s.is_empty() => s,
        _ => return,
    };
    let (parent_segs, last) = segments.split_at(segments.len() - 1);
    let mut current = root;
    for seg in parent_segs {
        match seg {
            PathSegment::Key(k) => {
                if let JsonValue::Object(obj) = current {
                    if let Some(pos) = obj.iter().position(|(key, _)| key == k) {
                        current = &mut obj[pos].1;
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
            PathSegment::Index(i) => {
                if let JsonValue::Array(arr) = current {
                    if *i < arr.len() {
                        current = &mut arr[*i];
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
        }
    }
    match &last[0] {
        PathSegment::Key(k) => {
            if let JsonValue::Object(obj) = current {
                if let Some(pos) = obj.iter().position(|(key, _)| key == k) {
                    if replace {
                        obj[pos].1 = val;
                    }
                } else if insert {
                    obj.push((k.clone(), val));
                }
            }
        }
        PathSegment::Index(i) => {
            if let JsonValue::Array(arr) = current {
                if *i < arr.len() {
                    if replace {
                        arr[*i] = val;
                    }
                } else if insert && *i >= arr.len() {
                    arr.push(val);
                }
            }
        }
    }
}

fn remove_path(root: &mut JsonValue, path: &str) {
    let segments = match parse_path_segments(path) {
        Some(s) if !s.is_empty() => s,
        _ => return,
    };
    let (parent_segs, last) = segments.split_at(segments.len() - 1);
    let mut current = root;
    for seg in parent_segs {
        match seg {
            PathSegment::Key(k) => {
                if let JsonValue::Object(obj) = current {
                    if let Some(pos) = obj.iter().position(|(key, _)| key == k) {
                        current = &mut obj[pos].1;
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
            PathSegment::Index(i) => {
                if let JsonValue::Array(arr) = current {
                    if *i < arr.len() {
                        current = &mut arr[*i];
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
        }
    }
    match &last[0] {
        PathSegment::Key(k) => {
            if let JsonValue::Object(obj) = current {
                if let Some(pos) = obj.iter().position(|(key, _)| key == k) {
                    obj.remove(pos);
                }
            }
        }
        PathSegment::Index(i) => {
            if let JsonValue::Array(arr) = current {
                if *i < arr.len() {
                    arr.remove(*i);
                }
            }
        }
    }
}

fn json_merge_patch(target: &mut JsonValue, patch: JsonValue) {
    match patch {
        JsonValue::Object(patch_obj) => {
            if !matches!(target, JsonValue::Object(_)) {
                *target = JsonValue::Object(Vec::new());
            }
            if let JsonValue::Object(target_obj) = target {
                for (key, value) in patch_obj {
                    if matches!(value, JsonValue::Null) {
                        if let Some(pos) = target_obj.iter().position(|(k, _)| k == &key) {
                            target_obj.remove(pos);
                        }
                    } else if let Some(pos) = target_obj.iter().position(|(k, _)| k == &key) {
                        json_merge_patch(&mut target_obj[pos].1, value);
                    } else {
                        let mut new_val = JsonValue::Null;
                        json_merge_patch(&mut new_val, value);
                        target_obj.push((key, new_val));
                    }
                }
            }
        }
        other => {
            *target = other;
        }
    }
}

pub(crate) fn eval_json_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "JSON" => {
            if args.len() != 1 {
                return Err(Error::Other("json() requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => {
                    let jv = parse_json(s)?;
                    Ok(Value::Text(jv.to_string_repr()))
                }
                _ => Err(Error::Other("json() requires a TEXT argument".into())),
            }
        }
        "JSON_EXTRACT" => {
            if args.len() < 2 {
                return Err(Error::Other(
                    "json_extract() requires at least 2 arguments".into(),
                ));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => {
                    let jv = parse_json(s)?;
                    if args.len() == 2 {
                        let path = match &args[1] {
                            Value::Text(p) => p.as_str(),
                            _ => return Err(Error::Other("json_extract path must be text".into())),
                        };
                        match jv.extract_path(path) {
                            Some(v) => Ok(json_value_to_sql(v)),
                            None => Ok(Value::Null),
                        }
                    } else {
                        let mut results = Vec::new();
                        for arg in &args[1..] {
                            let path = match arg {
                                Value::Text(p) => p.as_str(),
                                _ => {
                                    return Err(Error::Other(
                                        "json_extract path must be text".into(),
                                    ));
                                }
                            };
                            match jv.extract_path(path) {
                                Some(v) => results.push(v.to_string_repr()),
                                None => results.push("null".into()),
                            }
                        }
                        Ok(Value::Text(format!("[{}]", results.join(","))))
                    }
                }
                _ => Err(Error::Other(
                    "json_extract() requires a TEXT argument".into(),
                )),
            }
        }
        "JSON_TYPE" => {
            if args.is_empty() {
                return Err(Error::Other("json_type() requires 1-2 arguments".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => {
                    let jv = parse_json(s)?;
                    if args.len() > 1 {
                        let path = match &args[1] {
                            Value::Text(p) => p.as_str(),
                            _ => return Err(Error::Other("json_type path must be text".into())),
                        };
                        match jv.extract_path(path) {
                            Some(v) => Ok(Value::Text(v.type_name().to_string())),
                            None => Ok(Value::Null),
                        }
                    } else {
                        Ok(Value::Text(jv.type_name().to_string()))
                    }
                }
                _ => Err(Error::Other("json_type() requires a TEXT argument".into())),
            }
        }
        "JSON_VALID" => {
            if args.len() != 1 {
                return Err(Error::Other("json_valid() requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Integer(if parse_json(s).is_ok() { 1 } else { 0 })),
                _ => Ok(Value::Integer(0)),
            }
        }
        "JSON_ARRAY" => {
            let items: Vec<JsonValue> = args.iter().map(sql_to_json_value).collect();
            Ok(Value::Text(JsonValue::Array(items).to_string_repr()))
        }
        "JSON_OBJECT" => {
            if args.len() % 2 != 0 {
                return Err(Error::Other(
                    "json_object() requires an even number of arguments".into(),
                ));
            }
            let mut entries = Vec::new();
            for chunk in args.chunks(2) {
                let key = match &chunk[0] {
                    Value::Text(s) => s.clone(),
                    other => crate::eval_helpers::value_to_text(other),
                };
                let val = sql_to_json_value(&chunk[1]);
                entries.push((key, val));
            }
            Ok(Value::Text(JsonValue::Object(entries).to_string_repr()))
        }
        "JSON_ARRAY_LENGTH" => {
            if args.is_empty() {
                return Err(Error::Other(
                    "json_array_length() requires 1-2 arguments".into(),
                ));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => {
                    let jv = parse_json(s)?;
                    let target = if args.len() > 1 {
                        let path = match &args[1] {
                            Value::Text(p) => p.as_str(),
                            _ => {
                                return Err(Error::Other(
                                    "json_array_length path must be text".into(),
                                ));
                            }
                        };
                        jv.extract_path(path)
                    } else {
                        Some(&jv)
                    };
                    match target {
                        Some(JsonValue::Array(arr)) => Ok(Value::Integer(arr.len() as i64)),
                        Some(_) => Ok(Value::Integer(0)),
                        None => Ok(Value::Null),
                    }
                }
                _ => Err(Error::Other("json_array_length() requires TEXT".into())),
            }
        }
        "JSON_QUOTE" => {
            if args.len() != 1 {
                return Err(Error::Other("json_quote() requires 1 argument".into()));
            }
            let jv = sql_to_json_value(&args[0]);
            Ok(Value::Text(jv.to_string_repr()))
        }
        "JSON_INSERT" | "JSON_REPLACE" | "JSON_SET" => {
            if args.len() < 3 || args.len() % 2 != 1 {
                return Err(Error::Other(format!(
                    "{name}() requires odd number of arguments >= 3"
                )));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => {
                    let mut jv = parse_json(s)?;
                    for pair in args[1..].chunks(2) {
                        let path = match &pair[0] {
                            Value::Text(p) => p.clone(),
                            _ => return Err(Error::Other(format!("{name}: path must be text"))),
                        };
                        let new_val = sql_to_json_value(&pair[1]);
                        let insert = name != "JSON_REPLACE";
                        let replace = name != "JSON_INSERT";
                        set_path(&mut jv, &path, new_val, insert, replace);
                    }
                    Ok(Value::Text(jv.to_string_repr()))
                }
                _ => Err(Error::Other(format!(
                    "{name}() requires TEXT first argument"
                ))),
            }
        }
        "JSON_REMOVE" => {
            if args.is_empty() {
                return Err(Error::Other(
                    "json_remove() requires at least 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => {
                    let mut jv = parse_json(s)?;
                    for arg in &args[1..] {
                        let path = match arg {
                            Value::Text(p) => p.as_str(),
                            _ => return Err(Error::Other("json_remove: path must be text".into())),
                        };
                        remove_path(&mut jv, path);
                    }
                    Ok(Value::Text(jv.to_string_repr()))
                }
                _ => Err(Error::Other(
                    "json_remove() requires TEXT first argument".into(),
                )),
            }
        }
        "JSON_PATCH" => {
            if args.len() != 2 {
                return Err(Error::Other("json_patch() requires 2 arguments".into()));
            }
            match (&args[0], &args[1]) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Text(base_s), Value::Text(patch_s)) => {
                    let mut base = parse_json(base_s)?;
                    let patch = parse_json(patch_s)?;
                    json_merge_patch(&mut base, patch);
                    Ok(Value::Text(base.to_string_repr()))
                }
                _ => Err(Error::Other("json_patch() requires TEXT arguments".into())),
            }
        }
        _ => Err(Error::Other(format!("unknown JSON function: {name}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_values() {
        assert_eq!(parse_json("null").unwrap(), JsonValue::Null);
        assert_eq!(parse_json("true").unwrap(), JsonValue::Bool(true));
        assert_eq!(parse_json("42").unwrap(), JsonValue::Number(42.0));
        assert_eq!(
            parse_json("\"hello\"").unwrap(),
            JsonValue::String("hello".into())
        );
    }

    #[test]
    fn parse_array_and_object() {
        let arr = parse_json("[1, 2, 3]").unwrap();
        assert!(matches!(arr, JsonValue::Array(ref v) if v.len() == 3));

        let obj = parse_json(r#"{"a": 1, "b": "hello"}"#).unwrap();
        assert!(matches!(obj, JsonValue::Object(ref v) if v.len() == 2));
    }

    #[test]
    fn extract_path() {
        let jv = parse_json(r#"{"a": {"b": [10, 20, 30]}}"#).unwrap();
        assert_eq!(jv.extract_path("$.a.b[1]"), Some(&JsonValue::Number(20.0)));
        assert_eq!(
            jv.extract_path("$.a.b"),
            Some(&JsonValue::Array(vec![
                JsonValue::Number(10.0),
                JsonValue::Number(20.0),
                JsonValue::Number(30.0)
            ]))
        );
        assert_eq!(jv.extract_path("$.nonexistent"), None);
    }

    #[test]
    fn roundtrip() {
        let input = r#"{"name":"Alice","scores":[95,87,92],"active":true}"#;
        let jv = parse_json(input).unwrap();
        let output = jv.to_string_repr();
        let jv2 = parse_json(&output).unwrap();
        assert_eq!(jv, jv2);
    }

    // ---- parse edge cases ----

    #[test]
    #[allow(clippy::approx_constant)] // arbitrary float, not PI
    fn parse_negative_and_decimal_numbers() {
        assert_eq!(parse_json("-0").unwrap(), JsonValue::Number(0.0));
        assert_eq!(parse_json("-42").unwrap(), JsonValue::Number(-42.0));
        assert_eq!(parse_json("3.14").unwrap(), JsonValue::Number(3.14));
        assert_eq!(parse_json("-2.5").unwrap(), JsonValue::Number(-2.5));
    }

    #[test]
    fn parse_scientific_notation() {
        assert_eq!(parse_json("1e3").unwrap(), JsonValue::Number(1000.0));
        assert_eq!(parse_json("1.5e2").unwrap(), JsonValue::Number(150.0));
        assert_eq!(parse_json("-2E-3").unwrap(), JsonValue::Number(-0.002));
    }

    #[test]
    fn parse_string_escapes() {
        assert_eq!(
            parse_json(r#""tab\there""#).unwrap(),
            JsonValue::String("tab\there".into())
        );
        assert_eq!(
            parse_json(r#""new\nline""#).unwrap(),
            JsonValue::String("new\nline".into())
        );
        assert_eq!(
            parse_json(r#""back\\slash""#).unwrap(),
            JsonValue::String("back\\slash".into())
        );
        assert_eq!(
            parse_json(r#""quote\"in""#).unwrap(),
            JsonValue::String("quote\"in".into())
        );
    }

    #[test]
    fn parse_unicode_escape() {
        // é = é
        let jv = parse_json(r#""café""#).unwrap();
        assert_eq!(jv, JsonValue::String("café".into()));
    }

    #[test]
    fn parse_empty_containers() {
        assert!(matches!(
            parse_json("[]").unwrap(),
            JsonValue::Array(ref v) if v.is_empty()
        ));
        assert!(matches!(
            parse_json("{}").unwrap(),
            JsonValue::Object(ref v) if v.is_empty()
        ));
    }

    #[test]
    fn parse_nested_deeply() {
        let input = r#"{"a":{"b":{"c":{"d":[1,[2,[3,[4]]]]}}}}"#;
        let jv = parse_json(input).unwrap();
        assert_eq!(
            jv.extract_path("$.a.b.c.d[0]"),
            Some(&JsonValue::Number(1.0))
        );
    }

    #[test]
    fn parse_invalid_json_errors() {
        assert!(parse_json("not json").is_err());
        assert!(parse_json("{").is_err());
        assert!(parse_json("[1,").is_err());
        assert!(parse_json(r#"{"a":}"#).is_err());
        assert!(parse_json("").is_err());
    }

    #[test]
    fn parse_trailing_content_errors() {
        // Two top-level values is malformed.
        assert!(parse_json("1 2").is_err());
        assert!(parse_json("[]extra").is_err());
    }

    #[test]
    fn parse_with_whitespace() {
        let jv = parse_json("  \n\t [ 1 , 2 , 3 ] \n  ").unwrap();
        assert!(matches!(jv, JsonValue::Array(ref v) if v.len() == 3));
    }

    // ---- extract_path edge cases ----

    #[test]
    fn extract_root_returns_self() {
        let jv = parse_json("[1,2,3]").unwrap();
        assert_eq!(jv.extract_path("$"), Some(&jv));
    }

    #[test]
    fn extract_array_out_of_bounds() {
        let jv = parse_json("[10, 20]").unwrap();
        assert_eq!(jv.extract_path("$[5]"), None);
    }

    #[test]
    fn extract_into_scalar_returns_none() {
        let jv = parse_json("42").unwrap();
        // Can't take .key from a scalar
        assert_eq!(jv.extract_path("$.foo"), None);
        // Or [0] from a scalar
        assert_eq!(jv.extract_path("$[0]"), None);
    }

    #[test]
    fn extract_chained_object_and_array() {
        let jv = parse_json(r#"{"users":[{"name":"a"},{"name":"b"}]}"#).unwrap();
        assert_eq!(
            jv.extract_path("$.users[0].name"),
            Some(&JsonValue::String("a".into()))
        );
        assert_eq!(
            jv.extract_path("$.users[1].name"),
            Some(&JsonValue::String("b".into()))
        );
    }

    // ---- to_string_repr edge cases ----

    #[test]
    fn to_string_quotes_special_chars() {
        let s = JsonValue::String("a\"b\\c\nd".into());
        // Should escape ", \, and \n
        assert_eq!(s.to_string_repr(), r#""a\"b\\c\nd""#);
    }

    #[test]
    fn to_string_integer_form() {
        // Whole-number floats render as integers (per to_string_repr logic).
        assert_eq!(JsonValue::Number(5.0).to_string_repr(), "5");
        assert_eq!(JsonValue::Number(-5.0).to_string_repr(), "-5");
        assert_eq!(JsonValue::Number(2.5).to_string_repr(), "2.5");
    }

    #[test]
    fn type_name_each_variant() {
        assert_eq!(JsonValue::Null.type_name(), "null");
        assert_eq!(JsonValue::Bool(true).type_name(), "true");
        assert_eq!(JsonValue::Bool(false).type_name(), "false");
        assert_eq!(JsonValue::Number(0.0).type_name(), "real");
        assert_eq!(JsonValue::String("x".into()).type_name(), "text");
        assert_eq!(JsonValue::Array(vec![]).type_name(), "array");
        assert_eq!(JsonValue::Object(vec![]).type_name(), "object");
    }
}
