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
            let result: String = chars
                .iter()
                .skip(start_idx)
                .take(take_len)
                .collect();
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
            Ok(Value::Integer(if glob_match(&pattern, &value) { 1 } else { 0 }))
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
        "LAST_INSERT_ROWID" => {
            Ok(Value::Integer(super::executor::get_last_insert_rowid_pub()))
        }
        "CHANGES" => {
            Ok(Value::Integer(super::executor::get_changes_pub()))
        }
        "TOTAL_CHANGES" => {
            Ok(Value::Integer(super::executor::get_total_changes_pub()))
        }
        "PRINTF" | "FORMAT" => {
            if args.is_empty() {
                return Err(Error::Other("PRINTF requires at least 1 argument".into()));
            }
            let fmt = value_to_text(&args[0]);
            let result = simple_printf(&fmt, &args[1..]);
            Ok(Value::Text(result))
        }
        "LIKELY" | "UNLIKELY" => {
            if args.is_empty() {
                return Err(Error::Other(format!("{name} requires 1 argument")));
            }
            Ok(args[0].clone())
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
                return Err(Error::Other("VEC_DISTANCE_COSINE requires 2 arguments".into()));
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
            let dot: f64 = v1.iter().zip(&v2).map(|(a, b)| (*a as f64) * (*b as f64)).sum();
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
                        return Err(Error::Other("VEC_LENGTH: BLOB length must be a multiple of 4".into()));
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
                    let norm: f64 = v.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
                    if norm == 0.0 {
                        return Ok(args[0].clone());
                    }
                    let normalized: Vec<u8> = v.iter()
                        .flat_map(|x| ((*x as f64 / norm) as f32).to_le_bytes())
                        .collect();
                    Ok(Value::Blob(normalized))
                }
                _ => Err(Error::Other("VEC_NORMALIZE requires a BLOB argument".into())),
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
                _ => Err(Error::Other("VEC_FROM_JSON requires a TEXT argument".into())),
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

pub(crate) fn like_match(pattern: &str, value: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let val: Vec<char> = value.chars().collect();
    like_match_inner(&pat, &val)
}

pub(crate) fn like_match_inner(pattern: &[char], value: &[char]) -> bool {
    let mut pi = 0;
    let mut vi = 0;
    let mut star_pi = usize::MAX;
    let mut star_vi = 0;

    while vi < value.len() {
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
        } else if pi < pattern.len()
            && (pattern[pi] == '?' || pattern[pi] == value[vi])
        {
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
            _ => Ok(Value::Null),
        };
    }

    match op {
        BinOp::Eq => Ok(Value::Integer(if compare(left, right) == 0 { 1 } else { 0 })),
        BinOp::NotEq => Ok(Value::Integer(if compare(left, right) != 0 { 1 } else { 0 })),
        BinOp::Lt => Ok(Value::Integer(if compare(left, right) < 0 { 1 } else { 0 })),
        BinOp::LtEq => Ok(Value::Integer(if compare(left, right) <= 0 { 1 } else { 0 })),
        BinOp::Gt => Ok(Value::Integer(if compare(left, right) > 0 { 1 } else { 0 })),
        BinOp::GtEq => Ok(Value::Integer(if compare(left, right) >= 0 { 1 } else { 0 })),
        BinOp::And => Ok(Value::Integer(
            if is_truthy(left) && is_truthy(right) {
                1
            } else {
                0
            },
        )),
        BinOp::Or => Ok(Value::Integer(
            if is_truthy(left) || is_truthy(right) {
                1
            } else {
                0
            },
        )),
        BinOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinOp::Div => {
            // Integer division truncates
            numeric_op(left, right, |a, b| if b != 0 { a / b } else { 0 }, |a, b| a / b)
        }
        BinOp::Mod => {
            numeric_op(left, right, |a, b| if b != 0 { a % b } else { 0 }, |a, b| a % b)
        }
        BinOp::Concat => {
            if matches!(left, Value::Null) || matches!(right, Value::Null) {
                return Ok(Value::Null);
            }
            let l = value_to_text(left);
            let r = value_to_text(right);
            Ok(Value::Text(format!("{l}{r}")))
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
        return Err(Error::Other("vector BLOB length must be a multiple of 4 bytes".into()));
    }
    Ok(blob.chunks_exact(4).map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]])).collect())
}

fn blob_pair_to_f32(args: &[Value]) -> Result<(Vec<f32>, Vec<f32>)> {
    let b1 = match &args[0] {
        Value::Blob(b) => b,
        Value::Null => return Err(Error::Other("vector argument must not be NULL".into())),
        _ => return Err(Error::Other("vector distance requires BLOB arguments".into())),
    };
    let b2 = match &args[1] {
        Value::Blob(b) => b,
        Value::Null => return Err(Error::Other("vector argument must not be NULL".into())),
        _ => return Err(Error::Other("vector distance requires BLOB arguments".into())),
    };
    let v1 = blob_to_f32_vec(b1)?;
    let v2 = blob_to_f32_vec(b2)?;
    if v1.len() != v2.len() {
        return Err(Error::Other(format!(
            "vector dimension mismatch: {} vs {}", v1.len(), v2.len()
        )));
    }
    Ok((v1, v2))
}

fn cosine_distance(v1: &[f32], v2: &[f32]) -> f64 {
    let dot: f64 = v1.iter().zip(v2).map(|(a, b)| (*a as f64) * (*b as f64)).sum();
    let norm1: f64 = v1.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
    let norm2: f64 = v2.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
    if norm1 == 0.0 || norm2 == 0.0 {
        return 1.0;
    }
    1.0 - (dot / (norm1 * norm2))
}

fn l2_distance(v1: &[f32], v2: &[f32]) -> f64 {
    v1.iter().zip(v2).map(|(a, b)| {
        let d = (*a as f64) - (*b as f64);
        d * d
    }).sum::<f64>().sqrt()
}

fn parse_json_float_array(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(Error::Other("VEC_FROM_JSON: expected JSON array like [1.0, 2.0, ...]".into()));
    }
    let inner = &trimmed[1..trimmed.len()-1];
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner.split(',')
        .map(|part| {
            part.trim().parse::<f32>()
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
