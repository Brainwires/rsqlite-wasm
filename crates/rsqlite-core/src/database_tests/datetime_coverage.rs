// Datetime function coverage driven through the public SQL eval path
// (SELECT strftime/julianday/date/time/datetime(...)). Existing inline tests
// in datetime.rs only cover %s/%% and a couple of modifiers; these exercise
// the additional strftime specifiers, julianday variants, date arithmetic
// modifiers, and NULL propagation.

use super::*;
use crate::types::Value;

fn db() -> Database {
    let vfs = rsqlite_vfs::memory::MemoryVfs::new();
    Database::create(&vfs, "test.db").unwrap()
}

fn text(db: &mut Database, sql: &str) -> String {
    let r = db.query(sql).unwrap();
    match &r.rows[0].values[0] {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text from `{sql}`, got {other:?}"),
    }
}

// 2024-03-15 is a Friday and 2024 is a leap year.
const D: &str = "2024-03-15 10:30:45";

#[test]
fn strftime_day_of_year_j() {
    let mut db = db();
    // March 15: 31(Jan)+29(Feb leap)+15 = 75, zero-padded to 3 digits.
    assert_eq!(text(&mut db, &format!("SELECT strftime('%j', '{D}')")), "075");
}

#[test]
fn strftime_weekday_w() {
    let mut db = db();
    // Friday == 5 (0=Sunday).
    assert_eq!(text(&mut db, &format!("SELECT strftime('%w', '{D}')")), "5");
}

#[test]
fn strftime_week_number_capital_w() {
    let mut db = db();
    // week = (doy + 6 - dow) / 7 = (75 + 6 - 5) / 7 = 10.
    assert_eq!(text(&mut db, &format!("SELECT strftime('%W', '{D}')")), "10");
}

#[test]
fn strftime_julianday_capital_j() {
    let mut db = db();
    let s = text(&mut db, &format!("SELECT strftime('%J', '{D}')"));
    // Formatted to 10 decimal places; value near the Julian Day for 2024-03-15.
    let jd: f64 = s.parse().expect("parseable float");
    assert!(
        (jd - 2460384.9380).abs() < 0.01,
        "unexpected julianday {jd} from {s}"
    );
}

#[test]
fn strftime_fractional_seconds_f() {
    let mut db = db();
    // %f renders SS.000 in this implementation.
    assert_eq!(
        text(&mut db, &format!("SELECT strftime('%f', '{D}')")),
        "45.000"
    );
}

#[test]
fn strftime_unknown_specifier_passthrough() {
    let mut db = db();
    // Unknown specifier %Q is echoed verbatim as "%Q".
    assert_eq!(
        text(&mut db, &format!("SELECT strftime('x%Qy', '{D}')")),
        "x%Qy"
    );
}

#[test]
fn julianday_with_value() {
    let mut db = db();
    let r = db.query(&format!("SELECT julianday('{D}')")).unwrap();
    let jd = match &r.rows[0].values[0] {
        Value::Real(f) => *f,
        other => panic!("expected Real, got {other:?}"),
    };
    assert!((jd - 2460384.9380).abs() < 0.01, "got {jd}");
}

#[test]
fn julianday_with_date_modifier() {
    let mut db = db();
    // Adding one day increases the Julian Day by exactly 1.0.
    let base = match db
        .query(&format!("SELECT julianday('{D}')"))
        .unwrap()
        .rows[0]
        .values[0]
    {
        Value::Real(f) => f,
        ref other => panic!("{other:?}"),
    };
    let plus = match db
        .query(&format!("SELECT julianday('{D}', '+1 days')"))
        .unwrap()
        .rows[0]
        .values[0]
    {
        Value::Real(f) => f,
        ref other => panic!("{other:?}"),
    };
    assert!((plus - base - 1.0).abs() < 1e-6, "base={base} plus={plus}");
}

#[test]
fn julianday_null_returns_null() {
    let mut db = db();
    let r = db.query("SELECT julianday(NULL)").unwrap();
    assert_eq!(r.rows[0].values[0], Value::Null);
}

#[test]
fn date_plus_years() {
    let mut db = db();
    assert_eq!(
        text(&mut db, &format!("SELECT date('{D}', '+2 years')")),
        "2026-03-15"
    );
}

#[test]
fn datetime_plus_hours() {
    let mut db = db();
    assert_eq!(
        text(&mut db, &format!("SELECT datetime('{D}', '+3 hours')")),
        "2024-03-15 13:30:45"
    );
}

#[test]
fn time_plus_minutes() {
    let mut db = db();
    assert_eq!(
        text(&mut db, &format!("SELECT time('{D}', '+15 minutes')")),
        "10:45:45"
    );
}

#[test]
fn datetime_plus_seconds() {
    let mut db = db();
    assert_eq!(
        text(&mut db, &format!("SELECT datetime('{D}', '+20 seconds')")),
        "2024-03-15 10:31:05"
    );
}

#[test]
fn datetime_start_of_day() {
    let mut db = db();
    assert_eq!(
        text(&mut db, &format!("SELECT datetime('{D}', 'start of day')")),
        "2024-03-15 00:00:00"
    );
}

#[test]
fn time_only_input() {
    let mut db = db();
    // A bare HH:MM:SS input is accepted and time() echoes it back.
    assert_eq!(text(&mut db, "SELECT time('10:30:45')"), "10:30:45");
}

#[test]
fn null_input_returns_null_for_each_function() {
    let mut db = db();
    for f in ["date", "time", "datetime", "julianday", "strftime"] {
        let sql = if f == "strftime" {
            "SELECT strftime('%Y', NULL)".to_string()
        } else {
            format!("SELECT {f}(NULL)")
        };
        let r = db.query(&sql).unwrap();
        assert_eq!(r.rows[0].values[0], Value::Null, "function {f}");
    }
}
