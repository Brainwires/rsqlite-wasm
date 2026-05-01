use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::planner::{BinOp, LiteralValue, UnaryOp};

pub(crate) fn eval_scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "LENGTH" => {
            if args.is_empty() {
                return Err(Error::Other("LENGTH requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Integer(s.chars().count() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                Value::Integer(_) | Value::Real(_) => {
                    let s = value_to_text(&args[0]);
                    Ok(Value::Integer(s.len() as i64))
                }
            }
        }
        "UPPER" => {
            if args.is_empty() {
                return Err(Error::Other("UPPER requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_uppercase())),
                other => Ok(Value::Text(value_to_text(other).to_uppercase())),
            }
        }
        "LOWER" => {
            if args.is_empty() {
                return Err(Error::Other("LOWER requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(s.to_lowercase())),
                other => Ok(Value::Text(value_to_text(other).to_lowercase())),
            }
        }
        "ABS" => {
            if args.is_empty() {
                return Err(Error::Other("ABS requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(n) => Ok(Value::Integer(n.abs())),
                Value::Real(f) => Ok(Value::Real(f.abs())),
                _ => Ok(Value::Integer(0)),
            }
        }
        "TYPEOF" => {
            if args.is_empty() {
                return Err(Error::Other("TYPEOF requires 1 argument".into()));
            }
            let t = match &args[0] {
                Value::Null => "null",
                Value::Integer(_) => "integer",
                Value::Real(_) => "real",
                Value::Text(_) => "text",
                Value::Blob(_) => "blob",
            };
            Ok(Value::Text(t.to_string()))
        }
        "COALESCE" => {
            for v in args {
                if !matches!(v, Value::Null) {
                    return Ok(v.clone());
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            if args.len() < 2 {
                return Err(Error::Other("IFNULL requires 2 arguments".into()));
            }
            if matches!(args[0], Value::Null) {
                Ok(args[1].clone())
            } else {
                Ok(args[0].clone())
            }
        }
        "NULLIF" => {
            if args.len() < 2 {
                return Err(Error::Other("NULLIF requires 2 arguments".into()));
            }
            if compare(&args[0], &args[1]) == 0 {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            if args.len() < 2 {
                return Err(Error::Other("SUBSTR requires 2-3 arguments".into()));
            }
            if matches!(args[0], Value::Null) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&args[0]);
            let chars: Vec<char> = s.chars().collect();
            let start = match &args[1] {
                Value::Integer(n) => *n,
                _ => 1,
            };
            // SQLite SUBSTR is 1-indexed; negative means from end
            let (start_idx, take_len) = if start > 0 {
                let idx = (start - 1) as usize;
                let len = if args.len() > 2 {
                    match &args[2] {
                        Value::Integer(n) => *n as usize,
                        _ => chars.len(),
                    }
                } else {
                    chars.len()
                };
                (idx, len)
            } else if start == 0 {
                let len = if args.len() > 2 {
                    match &args[2] {
                        Value::Integer(n) => (*n as usize).saturating_sub(1),
                        _ => chars.len(),
                    }
                } else {
                    chars.len()
                };
                (0, len)
            } else {
                let from_end = (-start) as usize;
                let idx = chars.len().saturating_sub(from_end);
                let len = if args.len() > 2 {
                    match &args[2] {
                        Value::Integer(n) => *n as usize,
                        _ => chars.len(),
                    }
                } else {
                    chars.len()
                };
                (idx, len)
            };
            let result: String = chars.iter().skip(start_idx).take(take_len).collect();
            Ok(Value::Text(result))
        }
        "REPLACE" => {
            if args.len() < 3 {
                return Err(Error::Other("REPLACE requires 3 arguments".into()));
            }
            if matches!(args[0], Value::Null) {
                return Ok(Value::Null);
            }
            let s = value_to_text(&args[0]);
            let from = value_to_text(&args[1]);
            let to = value_to_text(&args[2]);
            Ok(Value::Text(s.replace(&from, &to)))
        }
        "INSTR" => {
            if args.len() < 2 {
                return Err(Error::Other("INSTR requires 2 arguments".into()));
            }
            if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
                return Ok(Value::Null);
            }
            let haystack = value_to_text(&args[0]);
            let needle = value_to_text(&args[1]);
            match haystack.find(&needle) {
                Some(pos) => {
                    let char_pos = haystack[..pos].chars().count() + 1;
                    Ok(Value::Integer(char_pos as i64))
                }
                None => Ok(Value::Integer(0)),
            }
        }
        "TRIM" => {
            if args.is_empty() {
                return Err(Error::Other("TRIM requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                other => Ok(Value::Text(value_to_text(other).trim().to_string())),
            }
        }
        "LTRIM" => {
            if args.is_empty() {
                return Err(Error::Other("LTRIM requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                other => Ok(Value::Text(value_to_text(other).trim_start().to_string())),
            }
        }
        "RTRIM" => {
            if args.is_empty() {
                return Err(Error::Other("RTRIM requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                other => Ok(Value::Text(value_to_text(other).trim_end().to_string())),
            }
        }
        "HEX" => {
            if args.is_empty() {
                return Err(Error::Other("HEX requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(hex))
                }
                other => {
                    let s = value_to_text(other);
                    let hex: String = s.bytes().map(|b| format!("{:02X}", b)).collect();
                    Ok(Value::Text(hex))
                }
            }
        }
        "QUOTE" => {
            if args.is_empty() {
                return Err(Error::Other("QUOTE requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Text("NULL".to_string())),
                Value::Integer(n) => Ok(Value::Text(n.to_string())),
                Value::Real(f) => Ok(Value::Text(f.to_string())),
                Value::Text(s) => {
                    let escaped = s.replace('\'', "''");
                    Ok(Value::Text(format!("'{escaped}'")))
                }
                Value::Blob(b) => {
                    let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                    Ok(Value::Text(format!("X'{hex}'")))
                }
            }
        }
        "UNICODE" => {
            if args.is_empty() {
                return Err(Error::Other("UNICODE requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => match s.chars().next() {
                    Some(c) => Ok(Value::Integer(c as i64)),
                    None => Ok(Value::Null),
                },
                other => {
                    let s = value_to_text(other);
                    match s.chars().next() {
                        Some(c) => Ok(Value::Integer(c as i64)),
                        None => Ok(Value::Null),
                    }
                }
            }
        }
        "CHAR" => {
            let mut result = String::new();
            for v in args {
                if let Value::Integer(n) = v {
                    if let Some(c) = char::from_u32(*n as u32) {
                        result.push(c);
                    }
                }
            }
            Ok(Value::Text(result))
        }
        "ZEROBLOB" => {
            if args.is_empty() {
                return Err(Error::Other("ZEROBLOB requires 1 argument".into()));
            }
            match &args[0] {
                Value::Integer(n) => Ok(Value::Blob(vec![0u8; *n as usize])),
                _ => Ok(Value::Blob(vec![])),
            }
        }
        "RANDOM" => Ok(Value::Integer(rand_i64())),
        "GLOB" => {
            if args.len() != 2 {
                return Err(Error::Other("GLOB requires 2 arguments".into()));
            }
            if matches!(args[0], Value::Null) || matches!(args[1], Value::Null) {
                return Ok(Value::Null);
            }
            let pattern = value_to_text(&args[0]);
            let value = value_to_text(&args[1]);
            Ok(Value::Integer(if glob_match(&pattern, &value) {
                1
            } else {
                0
            }))
        }
        "ROUND" => {
            if args.is_empty() {
                return Err(Error::Other("ROUND requires 1-2 arguments".into()));
            }
            if matches!(args[0], Value::Null) {
                return Ok(Value::Null);
            }
            let digits = if args.len() > 1 {
                match &args[1] {
                    Value::Integer(n) => *n as i32,
                    _ => 0,
                }
            } else {
                0
            };
            let val = match &args[0] {
                Value::Integer(n) => *n as f64,
                Value::Real(f) => *f,
                _ => 0.0,
            };
            let factor = 10f64.powi(digits);
            let rounded = (val * factor).round() / factor;
            if digits == 0 {
                Ok(Value::Real(rounded))
            } else {
                Ok(Value::Real(rounded))
            }
        }
        "LAST_INSERT_ROWID" => Ok(Value::Integer(super::executor::get_last_insert_rowid_pub())),
        "CHANGES" => Ok(Value::Integer(super::executor::get_changes_pub())),
        "TOTAL_CHANGES" => Ok(Value::Integer(super::executor::get_total_changes_pub())),
        "PRINTF" | "FORMAT" => {
            if args.is_empty() {
                return Err(Error::Other("PRINTF requires at least 1 argument".into()));
            }
            let fmt = value_to_text(&args[0]);
            let result = simple_printf(&fmt, &args[1..]);
            Ok(Value::Text(result))
        }
        "LIKELY" | "UNLIKELY" | "LIKELIHOOD" => {
            if args.is_empty() {
                return Err(Error::Other(format!("{name} requires 1 argument")));
            }
            Ok(args[0].clone())
        }
        "SIGN" => {
            if args.is_empty() {
                return Err(Error::Other("SIGN requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Integer(n) => Ok(Value::Integer(n.signum())),
                Value::Real(f) => {
                    if f.is_nan() {
                        Ok(Value::Null)
                    } else if *f > 0.0 {
                        Ok(Value::Integer(1))
                    } else if *f < 0.0 {
                        Ok(Value::Integer(-1))
                    } else {
                        Ok(Value::Integer(0))
                    }
                }
                Value::Text(s) => match s.trim().parse::<f64>() {
                    Ok(f) if f > 0.0 => Ok(Value::Integer(1)),
                    Ok(f) if f < 0.0 => Ok(Value::Integer(-1)),
                    Ok(_) => Ok(Value::Integer(0)),
                    Err(_) => Ok(Value::Null),
                },
                Value::Blob(_) => Ok(Value::Null),
            }
        }
        "SQLITE_VERSION" => Ok(Value::Text("3.42.0".to_string())),
        "SQLITE_SOURCE_ID" => Ok(Value::Text("2023-05-16 12:36:15 rsqlite-wasm".to_string())),
        "RANDOMBLOB" => {
            if args.is_empty() {
                return Err(Error::Other("RANDOMBLOB requires 1 argument".into()));
            }
            let n = match &args[0] {
                Value::Integer(n) => (*n).max(1) as usize,
                _ => 1usize,
            };
            let mut bytes = Vec::with_capacity(n);
            // Use rand_i64 to fill bytes; not cryptographically secure but
            // matches SQLite's pseudo-random behavior for RANDOMBLOB.
            while bytes.len() < n {
                let r = rand_i64().to_le_bytes();
                let need = n - bytes.len();
                bytes.extend_from_slice(&r[..need.min(8)]);
            }
            Ok(Value::Blob(bytes))
        }
        "DATE" => crate::datetime::eval_date(args),
        "TIME" => crate::datetime::eval_time(args),
        "DATETIME" => crate::datetime::eval_datetime(args),
        "JULIANDAY" => crate::datetime::eval_julianday(args),
        "UNIXEPOCH" => crate::datetime::eval_unixepoch(args),
        "STRFTIME" => crate::datetime::eval_strftime(args),
        "IIF" => {
            if args.len() != 3 {
                return Err(Error::Other("IIF requires 3 arguments".into()));
            }
            if is_truthy(&args[0]) {
                Ok(args[1].clone())
            } else {
                Ok(args[2].clone())
            }
        }
        "VEC_DISTANCE_COSINE" => {
            if args.len() != 2 {
                return Err(Error::Other(
                    "VEC_DISTANCE_COSINE requires 2 arguments".into(),
                ));
            }
            let (v1, v2) = blob_pair_to_f32(args)?;
            Ok(Value::Real(cosine_distance(&v1, &v2)))
        }
        "VEC_DISTANCE_L2" => {
            if args.len() != 2 {
                return Err(Error::Other("VEC_DISTANCE_L2 requires 2 arguments".into()));
            }
            let (v1, v2) = blob_pair_to_f32(args)?;
            Ok(Value::Real(l2_distance(&v1, &v2)))
        }
        "VEC_DISTANCE_DOT" => {
            if args.len() != 2 {
                return Err(Error::Other("VEC_DISTANCE_DOT requires 2 arguments".into()));
            }
            let (v1, v2) = blob_pair_to_f32(args)?;
            let dot: f64 = v1
                .iter()
                .zip(&v2)
                .map(|(a, b)| (*a as f64) * (*b as f64))
                .sum();
            Ok(Value::Real(-dot))
        }
        "VEC_LENGTH" => {
            if args.len() != 1 {
                return Err(Error::Other("VEC_LENGTH requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => {
                    if b.len() % 4 != 0 {
                        return Err(Error::Other(
                            "VEC_LENGTH: BLOB length must be a multiple of 4".into(),
                        ));
                    }
                    Ok(Value::Integer((b.len() / 4) as i64))
                }
                _ => Err(Error::Other("VEC_LENGTH requires a BLOB argument".into())),
            }
        }
        "VEC_NORMALIZE" => {
            if args.len() != 1 {
                return Err(Error::Other("VEC_NORMALIZE requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => {
                    let v = blob_to_f32_vec(b)?;
                    let norm: f64 = v
                        .iter()
                        .map(|x| (*x as f64) * (*x as f64))
                        .sum::<f64>()
                        .sqrt();
                    if norm == 0.0 {
                        return Ok(args[0].clone());
                    }
                    let normalized: Vec<u8> = v
                        .iter()
                        .flat_map(|x| ((*x as f64 / norm) as f32).to_le_bytes())
                        .collect();
                    Ok(Value::Blob(normalized))
                }
                _ => Err(Error::Other(
                    "VEC_NORMALIZE requires a BLOB argument".into(),
                )),
            }
        }
        "VEC_FROM_JSON" => {
            if args.len() != 1 {
                return Err(Error::Other("VEC_FROM_JSON requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => {
                    let floats = parse_json_float_array(s)?;
                    let blob: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();
                    Ok(Value::Blob(blob))
                }
                _ => Err(Error::Other(
                    "VEC_FROM_JSON requires a TEXT argument".into(),
                )),
            }
        }
        "VEC_TO_JSON" => {
            if args.len() != 1 {
                return Err(Error::Other("VEC_TO_JSON requires 1 argument".into()));
            }
            match &args[0] {
                Value::Null => Ok(Value::Null),
                Value::Blob(b) => {
                    let v = blob_to_f32_vec(b)?;
                    let parts: Vec<String> = v.iter().map(|f| format!("{f}")).collect();
                    Ok(Value::Text(format!("[{}]", parts.join(","))))
                }
                _ => Err(Error::Other("VEC_TO_JSON requires a BLOB argument".into())),
            }
        }
        "MIN" => {
            if args.is_empty() {
                return Ok(Value::Null);
            }
            let mut min = &args[0];
            for v in &args[1..] {
                if matches!(v, Value::Null) {
                    return Ok(Value::Null);
                }
                if compare(v, min) < 0 {
                    min = v;
                }
            }
            Ok(min.clone())
        }
        "MAX" => {
            if args.is_empty() {
                return Ok(Value::Null);
            }
            let mut max = &args[0];
            for v in &args[1..] {
                if matches!(v, Value::Null) {
                    return Ok(Value::Null);
                }
                if compare(v, max) > 0 {
                    max = v;
                }
            }
            Ok(max.clone())
        }
        "JSON" | "JSON_EXTRACT" | "JSON_TYPE" | "JSON_VALID" | "JSON_ARRAY" | "JSON_OBJECT"
        | "JSON_ARRAY_LENGTH" | "JSON_QUOTE" | "JSON_INSERT" | "JSON_REPLACE" | "JSON_SET"
        | "JSON_REMOVE" | "JSON_PATCH" => crate::json::eval_json_function(name, args),
        _ => Err(Error::Other(format!("unknown function: {name}"))),
    }
}

pub(crate) fn value_to_text(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Integer(n) => n.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Blob(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

pub(crate) fn like_match_with_escape(pattern: &str, value: &str, escape: Option<char>) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    like_match_inner(&pat, &val, escape)
}

pub(crate) fn like_match_inner(pattern: &[char], value: &[char], escape: Option<char>) -> bool {
    let mut pi = 0;
    let mut vi = 0;
    let mut star_pi = usize::MAX;
    let mut star_vi = 0;

    while vi < value.len() {
        // Escape handling: an escape char in the pattern forces the next
        // pattern char to be matched literally (no %/_ interpretation).
        if let Some(esc) = escape {
            if pi < pattern.len() && pattern[pi] == esc && pi + 1 < pattern.len() {
                if pattern[pi + 1].to_ascii_lowercase() == value[vi].to_ascii_lowercase() {
                    pi += 2;
                    vi += 1;
                    continue;
                } else if star_pi != usize::MAX {
                    pi = star_pi + 1;
                    star_vi += 1;
                    vi = star_vi;
                    continue;
                } else {
                    return false;
                }
            }
        }
        if pi < pattern.len() && pattern[pi] == '%' {
            star_pi = pi;
            star_vi = vi;
            pi += 1;
        } else if pi < pattern.len()
            && (pattern[pi] == '_'
                || pattern[pi].to_ascii_lowercase() == value[vi].to_ascii_lowercase())
        {
            pi += 1;
            vi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }

    // Trailing % in pattern still matches; but trailing escape+char means we
    // need a value char that doesn't exist — fall through.
    while pi < pattern.len() && pattern[pi] == '%' {
        pi += 1;
    }
    pi == pattern.len()
}

pub(crate) fn glob_match(pattern: &str, value: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    glob_match_inner(&pat, &val)
}

fn glob_match_inner(pattern: &[char], value: &[char]) -> bool {
    let mut pi = 0;
    let mut vi = 0;
    let mut star_pi = usize::MAX;
    let mut star_vi = 0;

    while vi < value.len() {
        if pi < pattern.len() && pattern[pi] == '*' {
            star_pi = pi;
            star_vi = vi;
            pi += 1;
        } else if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == value[vi]) {
            pi += 1;
            vi += 1;
        } else if pi < pattern.len() && pattern[pi] == '[' {
            if let Some((end, matched)) = match_char_class(&pattern[pi..], value[vi]) {
                if matched {
                    pi += end;
                    vi += 1;
                } else if star_pi != usize::MAX {
                    pi = star_pi + 1;
                    star_vi += 1;
                    vi = star_vi;
                } else {
                    return false;
                }
            } else if star_pi != usize::MAX {
                pi = star_pi + 1;
                star_vi += 1;
                vi = star_vi;
            } else {
                return false;
            }
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_vi += 1;
            vi = star_vi;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }
    pi == pattern.len()
}

fn match_char_class(pattern: &[char], ch: char) -> Option<(usize, bool)> {
    if pattern.is_empty() || pattern[0] != '[' {
        return None;
    }
    let mut i = 1;
    let negated = i < pattern.len() && pattern[i] == '^';
    if negated {
        i += 1;
    }
    let mut matched = false;
    while i < pattern.len() && pattern[i] != ']' {
        if i + 2 < pattern.len() && pattern[i + 1] == '-' {
            let lo = pattern[i];
            let hi = pattern[i + 2];
            if ch >= lo && ch <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if pattern[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }
    if i < pattern.len() && pattern[i] == ']' {
        Some((i + 1, matched != negated))
    } else {
        None
    }
}

pub(crate) fn eval_cast(val: Value, type_name: &str) -> Result<Value> {
    if matches!(val, Value::Null) {
        return Ok(Value::Null);
    }
    let upper = type_name.to_uppercase();
    if upper.contains("INT") {
        match &val {
            Value::Integer(_) => Ok(val),
            Value::Real(f) => Ok(Value::Integer(*f as i64)),
            Value::Text(s) => {
                let n = s.trim().parse::<i64>().unwrap_or(0);
                Ok(Value::Integer(n))
            }
            Value::Blob(b) => {
                let s = String::from_utf8_lossy(b);
                let n = s.trim().parse::<i64>().unwrap_or(0);
                Ok(Value::Integer(n))
            }
            Value::Null => Ok(Value::Null),
        }
    } else if upper.contains("REAL") || upper.contains("FLOAT") || upper.contains("DOUBLE") {
        match &val {
            Value::Real(_) => Ok(val),
            Value::Integer(n) => Ok(Value::Real(*n as f64)),
            Value::Text(s) => {
                let f = s.trim().parse::<f64>().unwrap_or(0.0);
                Ok(Value::Real(f))
            }
            _ => Ok(Value::Real(0.0)),
        }
    } else if upper.contains("TEXT") || upper.contains("CHAR") || upper.contains("CLOB") {
        Ok(Value::Text(value_to_text(&val)))
    } else if upper.contains("BLOB") {
        match val {
            Value::Blob(_) => Ok(val),
            Value::Text(s) => Ok(Value::Blob(s.into_bytes())),
            _ => Ok(Value::Blob(value_to_text(&val).into_bytes())),
        }
    } else {
        Ok(val)
    }
}

pub(crate) fn rand_i64() -> i64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;
    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    std::thread::current().id().hash(&mut h);
    h.finish() as i64
}

pub(crate) fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Null => Value::Null,
        LiteralValue::Integer(n) => Value::Integer(*n),
        LiteralValue::Real(f) => Value::Real(*f),
        LiteralValue::Text(s) => Value::Text(s.clone()),
        LiteralValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
    }
}

pub(crate) fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Integer(0) => false,
        Value::Integer(_) => true,
        Value::Real(f) => *f != 0.0,
        Value::Text(s) => !s.is_empty(),
        Value::Blob(b) => !b.is_empty(),
    }
}

pub(crate) fn eval_binop(op: BinOp, left: &Value, right: &Value) -> Result<Value> {
    // NULL propagation for most operators
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return match op {
            BinOp::And => {
                // FALSE AND NULL => FALSE, NULL AND TRUE => NULL
                if matches!(left, Value::Integer(0)) || matches!(right, Value::Integer(0)) {
                    Ok(Value::Integer(0))
                } else {
                    Ok(Value::Null)
                }
            }
            BinOp::Or => {
                // TRUE OR NULL => TRUE, NULL OR FALSE => NULL
                if is_truthy(left) || is_truthy(right) {
                    Ok(Value::Integer(1))
                } else {
                    Ok(Value::Null)
                }
            }
            BinOp::Is => {
                // NULL IS NULL = 1; NULL IS x or x IS NULL = 0
                let both_null = matches!(left, Value::Null) && matches!(right, Value::Null);
                Ok(Value::Integer(if both_null { 1 } else { 0 }))
            }
            BinOp::IsNot => {
                let both_null = matches!(left, Value::Null) && matches!(right, Value::Null);
                Ok(Value::Integer(if both_null { 0 } else { 1 }))
            }
            _ => Ok(Value::Null),
        };
    }

    match op {
        BinOp::Eq => Ok(Value::Integer(if compare(left, right) == 0 {
            1
        } else {
            0
        })),
        BinOp::NotEq => Ok(Value::Integer(if compare(left, right) != 0 {
            1
        } else {
            0
        })),
        BinOp::Lt => Ok(Value::Integer(if compare(left, right) < 0 { 1 } else { 0 })),
        BinOp::LtEq => Ok(Value::Integer(if compare(left, right) <= 0 {
            1
        } else {
            0
        })),
        BinOp::Gt => Ok(Value::Integer(if compare(left, right) > 0 { 1 } else { 0 })),
        BinOp::GtEq => Ok(Value::Integer(if compare(left, right) >= 0 {
            1
        } else {
            0
        })),
        BinOp::And => Ok(Value::Integer(if is_truthy(left) && is_truthy(right) {
            1
        } else {
            0
        })),
        BinOp::Or => Ok(Value::Integer(if is_truthy(left) || is_truthy(right) {
            1
        } else {
            0
        })),
        BinOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinOp::Div => {
            // Integer division truncates
            numeric_op(
                left,
                right,
                |a, b| if b != 0 { a / b } else { 0 },
                |a, b| a / b,
            )
        }
        BinOp::Mod => numeric_op(
            left,
            right,
            |a, b| if b != 0 { a % b } else { 0 },
            |a, b| a % b,
        ),
        BinOp::Concat => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }
            let l = value_to_text(left);
            let r = value_to_text(right);
            Ok(Value::Text(format!("{l}{r}")))
        }
        BinOp::BitAnd => Ok(Value::Integer(value_to_int(left) & value_to_int(right))),
        BinOp::BitOr => Ok(Value::Integer(value_to_int(left) | value_to_int(right))),
        BinOp::ShiftLeft => {
            let shift = value_to_int(right);
            let val = value_to_int(left);
            if shift < 0 {
                // SQLite: negative shift becomes opposite-direction shift
                Ok(Value::Integer(val.wrapping_shr((-shift) as u32 & 63)))
            } else {
                Ok(Value::Integer(val.wrapping_shl(shift as u32 & 63)))
            }
        }
        BinOp::ShiftRight => {
            let shift = value_to_int(right);
            let val = value_to_int(left);
            if shift < 0 {
                Ok(Value::Integer(val.wrapping_shl((-shift) as u32 & 63)))
            } else {
                Ok(Value::Integer(val.wrapping_shr(shift as u32 & 63)))
            }
        }
        BinOp::Is | BinOp::IsNot => {
            // NULL was already returned above, so neither side is NULL here.
            // Fall through to plain equality.
            let eq = compare(left, right) == 0;
            let result = match op {
                BinOp::Is => eq,
                _ => !eq,
            };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }
        BinOp::JsonArrow | BinOp::JsonLongArrow => {
            let json_text = value_to_text(left);
            let parsed = match crate::json::parse_json(&json_text) {
                Ok(v) => v,
                Err(_) => return Ok(Value::Null),
            };
            let extracted = match right {
                Value::Text(key) => match &parsed {
                    crate::json::JsonValue::Object(obj) => {
                        obj.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
                    }
                    _ => None,
                },
                Value::Integer(idx) => match &parsed {
                    crate::json::JsonValue::Array(arr) => {
                        let len = arr.len() as i64;
                        let i = if *idx < 0 { len + *idx } else { *idx };
                        if i >= 0 && i < len {
                            Some(arr[i as usize].clone())
                        } else {
                            None
                        }
                    }
                    _ => None,
                },
                _ => None,
            };
            match extracted {
                None => Ok(Value::Null),
                Some(jv) => match op {
                    BinOp::JsonArrow => Ok(Value::Text(jv.to_string_repr())),
                    BinOp::JsonLongArrow => Ok(crate::json::json_value_to_sql(&jv)),
                    _ => unreachable!(),
                },
            }
        }
    }
}

/// Coerce any SQLite value to i64 for bitwise operations.
/// NULL was filtered out by the caller; this only handles non-null values.
pub(crate) fn value_to_int(val: &Value) -> i64 {
    match val {
        Value::Null => 0,
        Value::Integer(n) => *n,
        Value::Real(f) => *f as i64,
        Value::Text(s) => s.trim().parse::<i64>().unwrap_or(0),
        Value::Blob(b) => {
            let s = String::from_utf8_lossy(b);
            s.trim().parse::<i64>().unwrap_or(0)
        }
    }
}

pub(crate) fn eval_unaryop(op: UnaryOp, val: &Value) -> Result<Value> {
    match (op, val) {
        (UnaryOp::Not, Value::Null) => Ok(Value::Null),
        (UnaryOp::Not, v) => Ok(Value::Integer(if is_truthy(v) { 0 } else { 1 })),
        (UnaryOp::Neg, Value::Null) => Ok(Value::Null),
        (UnaryOp::Neg, Value::Integer(n)) => Ok(Value::Integer(-n)),
        (UnaryOp::Neg, Value::Real(f)) => Ok(Value::Real(-f)),
        (UnaryOp::Neg, _) => Ok(Value::Integer(0)),
        (UnaryOp::BitNot, Value::Null) => Ok(Value::Null),
        (UnaryOp::BitNot, v) => Ok(Value::Integer(!value_to_int(v))),
    }
}

/// SQLite comparison ordering: NULL < INTEGER/REAL < TEXT < BLOB
pub(crate) fn type_order(val: &Value) -> i32 {
    match val {
        Value::Null => 0,
        Value::Integer(_) => 1,
        Value::Real(_) => 1,
        Value::Text(_) => 2,
        Value::Blob(_) => 3,
    }
}

pub(crate) fn compare(left: &Value, right: &Value) -> i32 {
    let lo = type_order(left);
    let ro = type_order(right);
    if lo != ro {
        return lo - ro;
    }

    match (left, right) {
        (Value::Null, Value::Null) => 0,
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b) as i32,
        (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).map_or(0, |o| o as i32),
        (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b).map_or(0, |o| o as i32),
        (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)).map_or(0, |o| o as i32),
        (Value::Text(a), Value::Text(b)) => a.cmp(b) as i32,
        (Value::Blob(a), Value::Blob(b)) => a.cmp(b) as i32,
        _ => 0,
    }
}

pub(crate) fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(int_op(*a, *b))),
        (Value::Real(a), Value::Real(b)) => Ok(Value::Real(float_op(*a, *b))),
        (Value::Integer(a), Value::Real(b)) => Ok(Value::Real(float_op(*a as f64, *b))),
        (Value::Real(a), Value::Integer(b)) => Ok(Value::Real(float_op(*a, *b as f64))),
        _ => Ok(Value::Integer(0)),
    }
}

fn blob_to_f32_vec(blob: &[u8]) -> Result<Vec<f32>> {
    if blob.len() % 4 != 0 {
        return Err(Error::Other(
            "vector BLOB length must be a multiple of 4 bytes".into(),
        ));
    }
    Ok(blob
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

fn blob_pair_to_f32(args: &[Value]) -> Result<(Vec<f32>, Vec<f32>)> {
    let b1 = match &args[0] {
        Value::Blob(b) => b,
        Value::Null => return Err(Error::Other("vector argument must not be NULL".into())),
        _ => {
            return Err(Error::Other(
                "vector distance requires BLOB arguments".into(),
            ));
        }
    };
    let b2 = match &args[1] {
        Value::Blob(b) => b,
        Value::Null => return Err(Error::Other("vector argument must not be NULL".into())),
        _ => {
            return Err(Error::Other(
                "vector distance requires BLOB arguments".into(),
            ));
        }
    };
    let v1 = blob_to_f32_vec(b1)?;
    let v2 = blob_to_f32_vec(b2)?;
    if v1.len() != v2.len() {
        return Err(Error::Other(format!(
            "vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }
    Ok((v1, v2))
}

fn cosine_distance(v1: &[f32], v2: &[f32]) -> f64 {
    let dot: f64 = v1
        .iter()
        .zip(v2)
        .map(|(a, b)| (*a as f64) * (*b as f64))
        .sum();
    let norm1: f64 = v1
        .iter()
        .map(|x| (*x as f64) * (*x as f64))
        .sum::<f64>()
        .sqrt();
    let norm2: f64 = v2
        .iter()
        .map(|x| (*x as f64) * (*x as f64))
        .sum::<f64>()
        .sqrt();
    if norm1 == 0.0 || norm2 == 0.0 {
        return 1.0;
    }
    1.0 - (dot / (norm1 * norm2))
}

fn l2_distance(v1: &[f32], v2: &[f32]) -> f64 {
    v1.iter()
        .zip(v2)
        .map(|(a, b)| {
            let d = (*a as f64) - (*b as f64);
            d * d
        })
        .sum::<f64>()
        .sqrt()
}

fn parse_json_float_array(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(Error::Other(
            "VEC_FROM_JSON: expected JSON array like [1.0, 2.0, ...]".into(),
        ));
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<f32>()
                .map_err(|_| Error::Other(format!("VEC_FROM_JSON: invalid float: {}", part.trim())))
        })
        .collect()
}

fn simple_printf(fmt: &str, args: &[Value]) -> String {
    let mut result = String::new();
    let mut chars = fmt.chars().peekable();
    let mut arg_idx = 0;

    while let Some(ch) = chars.next() {
        if ch == '%' {
            match chars.peek() {
                Some('%') => {
                    chars.next();
                    result.push('%');
                }
                Some('d' | 'i') => {
                    chars.next();
                    if let Some(val) = args.get(arg_idx) {
                        match val {
                            Value::Integer(n) => result.push_str(&n.to_string()),
                            Value::Real(f) => result.push_str(&(*f as i64).to_string()),
                            _ => result.push('0'),
                        }
                    }
                    arg_idx += 1;
                }
                Some('f') => {
                    chars.next();
                    if let Some(val) = args.get(arg_idx) {
                        match val {
                            Value::Real(f) => result.push_str(&format!("{:.6}", f)),
                            Value::Integer(n) => result.push_str(&format!("{:.6}", *n as f64)),
                            _ => result.push_str("0.000000"),
                        }
                    }
                    arg_idx += 1;
                }
                Some('s') => {
                    chars.next();
                    if let Some(val) = args.get(arg_idx) {
                        result.push_str(&value_to_text(val));
                    }
                    arg_idx += 1;
                }
                _ => {
                    result.push('%');
                }
            }
        } else {
            result.push(ch);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsqlite_storage::codec::Value;

    fn t(s: &str) -> Value {
        Value::Text(s.to_string())
    }
    fn i(n: i64) -> Value {
        Value::Integer(n)
    }
    fn r(f: f64) -> Value {
        Value::Real(f)
    }

    // ---- value coercion / comparison ----

    #[test]
    fn value_to_text_covers_all_types() {
        assert_eq!(value_to_text(&Value::Null), "");
        assert_eq!(value_to_text(&i(42)), "42");
        assert_eq!(value_to_text(&r(2.5)), "2.5");
        assert_eq!(value_to_text(&t("hi")), "hi");
        assert_eq!(value_to_text(&Value::Blob(vec![97, 98])), "ab");
    }

    #[test]
    fn value_to_int_parses_and_coerces() {
        assert_eq!(value_to_int(&Value::Null), 0);
        assert_eq!(value_to_int(&i(7)), 7);
        assert_eq!(value_to_int(&r(3.9)), 3); // truncates
        assert_eq!(value_to_int(&t("42")), 42);
        assert_eq!(value_to_int(&t("not a number")), 0);
        assert_eq!(value_to_int(&t("  -10  ")), -10); // trims
    }

    #[test]
    fn is_truthy_semantics() {
        assert!(!is_truthy(&Value::Null));
        assert!(!is_truthy(&i(0)));
        assert!(is_truthy(&i(1)));
        assert!(is_truthy(&i(-1)));
        assert!(!is_truthy(&r(0.0)));
        assert!(is_truthy(&r(0.5)));
        assert!(!is_truthy(&t("")));
        assert!(is_truthy(&t("any")));
        assert!(!is_truthy(&Value::Blob(vec![])));
        assert!(is_truthy(&Value::Blob(vec![0])));
    }

    #[test]
    fn type_order_ranks_consistently() {
        assert!(type_order(&Value::Null) < type_order(&i(0)));
        assert_eq!(type_order(&i(0)), type_order(&r(0.0)));
        assert!(type_order(&i(0)) < type_order(&t("")));
        assert!(type_order(&t("")) < type_order(&Value::Blob(vec![])));
    }

    #[test]
    fn compare_orders_by_type_then_value() {
        assert_eq!(compare(&Value::Null, &Value::Null), 0);
        assert!(compare(&Value::Null, &i(0)) < 0);
        assert_eq!(compare(&i(5), &i(5)), 0);
        assert!(compare(&i(5), &i(10)) < 0);
        assert!(compare(&i(5), &r(5.0)) == 0);
        assert!(compare(&t("a"), &t("b")) < 0);
        assert!(compare(&i(99), &t("a")) < 0); // numeric < text
    }

    // ---- LIKE matching ----

    #[test]
    fn like_basic_wildcards() {
        assert!(like_match_with_escape("hello", "hello", None));
        assert!(like_match_with_escape("h%", "hello", None));
        assert!(like_match_with_escape("%lo", "hello", None));
        assert!(like_match_with_escape("h_llo", "hello", None));
        assert!(!like_match_with_escape("h_llo", "ho", None));
    }

    #[test]
    fn like_case_insensitive_ascii() {
        assert!(like_match_with_escape("HELLO", "hello", None));
        assert!(like_match_with_escape("h%O", "HELLO", None));
    }

    #[test]
    fn like_escape_char() {
        // \% should match literal '%'
        assert!(like_match_with_escape(r"50\%", "50%", Some('\\')));
        assert!(!like_match_with_escape(r"50\%", "50x", Some('\\')));
        // \_ should match literal '_'
        assert!(like_match_with_escape(r"a\_b", "a_b", Some('\\')));
        assert!(!like_match_with_escape(r"a\_b", "aXb", Some('\\')));
    }

    #[test]
    fn like_empty_inputs() {
        assert!(like_match_with_escape("", "", None));
        assert!(!like_match_with_escape("a", "", None));
        assert!(!like_match_with_escape("", "a", None));
        assert!(like_match_with_escape("%", "", None));
    }

    // ---- GLOB matching ----

    #[test]
    fn glob_basic_wildcards() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h*o", "hello"));
        assert!(!glob_match("h?llo", "hllo"));
    }

    #[test]
    fn glob_is_case_sensitive() {
        // GLOB is case-sensitive (unlike LIKE).
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "HELLO"));
    }

    #[test]
    fn glob_char_class() {
        assert!(glob_match("[abc]ello", "aello"));
        assert!(glob_match("[abc]ello", "bello"));
        assert!(!glob_match("[abc]ello", "dello"));
    }

    // ---- CAST ----

    #[test]
    fn cast_integer_paths() {
        assert_eq!(eval_cast(t("42"), "INTEGER").unwrap(), i(42));
        assert_eq!(eval_cast(r(3.7), "INTEGER").unwrap(), i(3));
        assert_eq!(eval_cast(t("garbage"), "INTEGER").unwrap(), i(0));
        assert_eq!(eval_cast(Value::Null, "INTEGER").unwrap(), Value::Null);
    }

    #[test]
    #[allow(clippy::approx_constant)] // testing CAST behavior, not PI
    fn cast_real_paths() {
        assert_eq!(eval_cast(i(5), "REAL").unwrap(), r(5.0));
        assert_eq!(eval_cast(t("3.14"), "REAL").unwrap(), r(3.14));
        assert_eq!(eval_cast(t("nope"), "REAL").unwrap(), r(0.0));
    }

    #[test]
    fn cast_text_paths() {
        assert_eq!(eval_cast(i(42), "TEXT").unwrap(), t("42"));
        assert_eq!(eval_cast(r(2.5), "TEXT").unwrap(), t("2.5"));
        assert_eq!(eval_cast(Value::Null, "TEXT").unwrap(), Value::Null);
    }

    #[test]
    fn cast_blob_paths() {
        match eval_cast(t("ab"), "BLOB").unwrap() {
            Value::Blob(b) => assert_eq!(b, vec![97, 98]),
            other => panic!("expected blob, got {:?}", other),
        }
    }

    // ---- literal_to_value ----

    #[test]
    fn literal_conversions() {
        use crate::planner::LiteralValue;
        assert_eq!(literal_to_value(&LiteralValue::Null), Value::Null);
        assert_eq!(literal_to_value(&LiteralValue::Integer(7)), i(7));
        assert_eq!(literal_to_value(&LiteralValue::Real(1.5)), r(1.5));
        assert_eq!(literal_to_value(&LiteralValue::Text("x".into())), t("x"));
        assert_eq!(literal_to_value(&LiteralValue::Bool(true)), i(1));
        assert_eq!(literal_to_value(&LiteralValue::Bool(false)), i(0));
    }

    // ---- binary op edge cases ----

    #[test]
    fn binop_null_propagation() {
        // Most ops return NULL when either side is NULL.
        assert_eq!(
            eval_binop(BinOp::Add, &Value::Null, &i(5)).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binop(BinOp::Mul, &i(3), &Value::Null).unwrap(),
            Value::Null
        );
        // AND/OR are special: FALSE AND NULL = FALSE, TRUE OR NULL = TRUE.
        assert_eq!(eval_binop(BinOp::And, &i(0), &Value::Null).unwrap(), i(0));
        assert_eq!(eval_binop(BinOp::Or, &i(1), &Value::Null).unwrap(), i(1));
    }

    #[test]
    fn binop_is_and_is_not_with_null() {
        // NULL IS NULL = 1; NULL IS x = 0.
        assert_eq!(
            eval_binop(BinOp::Is, &Value::Null, &Value::Null).unwrap(),
            i(1)
        );
        assert_eq!(eval_binop(BinOp::Is, &Value::Null, &i(5)).unwrap(), i(0));
        assert_eq!(
            eval_binop(BinOp::IsNot, &Value::Null, &Value::Null).unwrap(),
            i(0)
        );
        assert_eq!(eval_binop(BinOp::IsNot, &Value::Null, &i(5)).unwrap(), i(1));
    }

    #[test]
    fn binop_arithmetic() {
        assert_eq!(eval_binop(BinOp::Add, &i(2), &i(3)).unwrap(), i(5));
        assert_eq!(eval_binop(BinOp::Sub, &i(10), &i(4)).unwrap(), i(6));
        assert_eq!(eval_binop(BinOp::Mul, &i(3), &i(7)).unwrap(), i(21));
        assert_eq!(eval_binop(BinOp::Div, &i(10), &i(3)).unwrap(), i(3));
        assert_eq!(eval_binop(BinOp::Mod, &i(10), &i(3)).unwrap(), i(1));
    }

    #[test]
    fn binop_div_by_zero_returns_zero() {
        // SQLite returns NULL for x/0 with reals; our impl currently
        // returns 0 for integer division by zero. Documenting current behavior.
        assert_eq!(eval_binop(BinOp::Div, &i(10), &i(0)).unwrap(), i(0));
    }

    #[test]
    fn binop_concat() {
        assert_eq!(
            eval_binop(BinOp::Concat, &t("foo"), &t("bar")).unwrap(),
            t("foobar")
        );
        // NULL || x = NULL.
        assert_eq!(
            eval_binop(BinOp::Concat, &Value::Null, &t("x")).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn binop_bitwise() {
        assert_eq!(
            eval_binop(BinOp::BitAnd, &i(0b1100), &i(0b1010)).unwrap(),
            i(0b1000)
        );
        assert_eq!(
            eval_binop(BinOp::BitOr, &i(0b1100), &i(0b0011)).unwrap(),
            i(0b1111)
        );
        assert_eq!(eval_binop(BinOp::ShiftLeft, &i(1), &i(3)).unwrap(), i(8));
        assert_eq!(eval_binop(BinOp::ShiftRight, &i(16), &i(2)).unwrap(), i(4));
    }

    // ---- unary ops ----

    #[test]
    fn unary_not_truthiness() {
        assert_eq!(eval_unaryop(UnaryOp::Not, &i(0)).unwrap(), i(1));
        assert_eq!(eval_unaryop(UnaryOp::Not, &i(5)).unwrap(), i(0));
        assert_eq!(
            eval_unaryop(UnaryOp::Not, &Value::Null).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn unary_neg_numeric() {
        assert_eq!(eval_unaryop(UnaryOp::Neg, &i(5)).unwrap(), i(-5));
        assert_eq!(eval_unaryop(UnaryOp::Neg, &r(2.5)).unwrap(), r(-2.5));
        assert_eq!(
            eval_unaryop(UnaryOp::Neg, &Value::Null).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn unary_bitnot() {
        assert_eq!(eval_unaryop(UnaryOp::BitNot, &i(0)).unwrap(), i(-1));
        assert_eq!(eval_unaryop(UnaryOp::BitNot, &i(-1)).unwrap(), i(0));
    }

    // ---- scalar functions ----

    #[test]
    fn scalar_length_unicode() {
        assert_eq!(eval_scalar_function("LENGTH", &[t("hello")]).unwrap(), i(5));
        // 4 chars, not 8 bytes, for "café"
        assert_eq!(eval_scalar_function("LENGTH", &[t("café")]).unwrap(), i(4));
        assert_eq!(
            eval_scalar_function("LENGTH", &[Value::Null]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn scalar_upper_lower_unicode() {
        assert_eq!(
            eval_scalar_function("UPPER", &[t("hello")]).unwrap(),
            t("HELLO")
        );
        assert_eq!(
            eval_scalar_function("LOWER", &[t("WORLD")]).unwrap(),
            t("world")
        );
        // Unicode case folding works for ascii-only here; locale-dependent
        // characters like 'İ' may not roundtrip, which is SQLite-correct.
        assert_eq!(eval_scalar_function("UPPER", &[t("ß")]).unwrap(), t("SS"));
    }

    #[test]
    fn scalar_substr_negative_start() {
        // start=-3 means "3 from the end"
        assert_eq!(
            eval_scalar_function("SUBSTR", &[t("hello"), i(-3)]).unwrap(),
            t("llo")
        );
        // start=1 is the first char (1-indexed)
        assert_eq!(
            eval_scalar_function("SUBSTR", &[t("hello"), i(1), i(2)]).unwrap(),
            t("he")
        );
        // start=0 is treated as 1 in some impls; here it returns from index 0
        assert_eq!(
            eval_scalar_function("SUBSTR", &[t("hello"), i(0), i(3)]).unwrap(),
            t("he")
        );
    }

    #[test]
    fn scalar_coalesce_picks_first_non_null() {
        assert_eq!(
            eval_scalar_function("COALESCE", &[Value::Null, Value::Null, t("c")]).unwrap(),
            t("c")
        );
        assert_eq!(
            eval_scalar_function("COALESCE", &[Value::Null, Value::Null]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn scalar_ifnull_and_nullif() {
        assert_eq!(
            eval_scalar_function("IFNULL", &[Value::Null, t("x")]).unwrap(),
            t("x")
        );
        assert_eq!(
            eval_scalar_function("IFNULL", &[i(1), t("x")]).unwrap(),
            i(1)
        );
        assert_eq!(
            eval_scalar_function("NULLIF", &[i(1), i(1)]).unwrap(),
            Value::Null
        );
        assert_eq!(eval_scalar_function("NULLIF", &[i(1), i(2)]).unwrap(), i(1));
    }

    #[test]
    fn scalar_typeof_each_type() {
        assert_eq!(
            eval_scalar_function("TYPEOF", &[Value::Null]).unwrap(),
            t("null")
        );
        assert_eq!(
            eval_scalar_function("TYPEOF", &[i(0)]).unwrap(),
            t("integer")
        );
        assert_eq!(
            eval_scalar_function("TYPEOF", &[r(0.0)]).unwrap(),
            t("real")
        );
        assert_eq!(
            eval_scalar_function("TYPEOF", &[t("x")]).unwrap(),
            t("text")
        );
        assert_eq!(
            eval_scalar_function("TYPEOF", &[Value::Blob(vec![1])]).unwrap(),
            t("blob")
        );
    }

    #[test]
    fn scalar_abs_paths() {
        assert_eq!(eval_scalar_function("ABS", &[i(-5)]).unwrap(), i(5));
        assert_eq!(eval_scalar_function("ABS", &[r(-2.5)]).unwrap(), r(2.5));
        assert_eq!(
            eval_scalar_function("ABS", &[Value::Null]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn scalar_replace_and_instr() {
        assert_eq!(
            eval_scalar_function("REPLACE", &[t("hello world"), t("world"), t("rust")]).unwrap(),
            t("hello rust")
        );
        assert_eq!(
            eval_scalar_function("INSTR", &[t("hello"), t("ll")]).unwrap(),
            i(3)
        );
        assert_eq!(
            eval_scalar_function("INSTR", &[t("hello"), t("xx")]).unwrap(),
            i(0)
        );
    }

    #[test]
    fn scalar_trim_variants() {
        assert_eq!(
            eval_scalar_function("TRIM", &[t("  hi  ")]).unwrap(),
            t("hi")
        );
        assert_eq!(
            eval_scalar_function("LTRIM", &[t("  hi  ")]).unwrap(),
            t("hi  ")
        );
        assert_eq!(
            eval_scalar_function("RTRIM", &[t("  hi  ")]).unwrap(),
            t("  hi")
        );
    }

    #[test]
    fn scalar_sign_branches() {
        assert_eq!(eval_scalar_function("SIGN", &[i(5)]).unwrap(), i(1));
        assert_eq!(eval_scalar_function("SIGN", &[i(-5)]).unwrap(), i(-1));
        assert_eq!(eval_scalar_function("SIGN", &[i(0)]).unwrap(), i(0));
        assert_eq!(
            eval_scalar_function("SIGN", &[Value::Null]).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_scalar_function("SIGN", &[t("garbage")]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn scalar_iif_picks_branch() {
        assert_eq!(
            eval_scalar_function("IIF", &[i(1), t("yes"), t("no")]).unwrap(),
            t("yes")
        );
        assert_eq!(
            eval_scalar_function("IIF", &[i(0), t("yes"), t("no")]).unwrap(),
            t("no")
        );
    }

    #[test]
    fn scalar_hex_and_quote() {
        match eval_scalar_function("HEX", &[Value::Blob(vec![0xAB, 0xCD])]).unwrap() {
            Value::Text(s) => assert_eq!(s, "ABCD"),
            other => panic!("got {:?}", other),
        }
        // QUOTE wraps text in single quotes and doubles internal quotes
        match eval_scalar_function("QUOTE", &[t("it's")]).unwrap() {
            Value::Text(s) => assert_eq!(s, "'it''s'"),
            other => panic!("got {:?}", other),
        }
    }

    #[test]
    fn scalar_zeroblob_size() {
        match eval_scalar_function("ZEROBLOB", &[i(4)]).unwrap() {
            Value::Blob(b) => assert_eq!(b, vec![0, 0, 0, 0]),
            other => panic!("got {:?}", other),
        }
    }

    #[test]
    fn scalar_random_returns_integer() {
        // Just sanity: returns *something* of integer type.
        assert!(matches!(
            eval_scalar_function("RANDOM", &[]).unwrap(),
            Value::Integer(_)
        ));
    }

    #[test]
    fn scalar_randomblob_length() {
        match eval_scalar_function("RANDOMBLOB", &[i(8)]).unwrap() {
            Value::Blob(b) => assert_eq!(b.len(), 8),
            other => panic!("got {:?}", other),
        }
    }

    #[test]
    fn scalar_likelihood_passes_through() {
        assert_eq!(
            eval_scalar_function("LIKELIHOOD", &[i(42), r(0.5)]).unwrap(),
            i(42)
        );
        assert_eq!(eval_scalar_function("LIKELY", &[t("x")]).unwrap(), t("x"));
        assert_eq!(eval_scalar_function("UNLIKELY", &[t("y")]).unwrap(), t("y"));
    }

    #[test]
    fn scalar_unknown_function_errors() {
        assert!(eval_scalar_function("NOPE_FN", &[]).is_err());
    }
}
