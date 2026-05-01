use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::eval_helpers::value_to_text;

const JULIAN_EPOCH_OFFSET: f64 = 2440587.5;
const SECONDS_PER_DAY: f64 = 86400.0;

#[derive(Clone, Debug)]
struct DateTime {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
}

impl DateTime {
    fn now() -> Self {
        let secs = current_unix_timestamp();
        Self::from_unix(secs)
    }

    fn from_unix(secs: f64) -> Self {
        let total_secs = secs as i64;
        let mut days = total_secs.div_euclid(86400);
        let day_secs = total_secs.rem_euclid(86400);

        let hour = (day_secs / 3600) as u32;
        let minute = ((day_secs % 3600) / 60) as u32;
        let second = (day_secs % 60) as u32;

        days += 719468;
        let era = if days >= 0 { days } else { days - 146096 } / 146097;
        let doe = (days - era * 146097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe as i32 + (era as i32) * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = if m <= 2 { y + 1 } else { y };

        Self {
            year,
            month: m,
            day: d,
            hour,
            minute,
            second,
        }
    }

    fn to_unix(&self) -> f64 {
        let (y, m) = if self.month <= 2 {
            (self.year as i64 - 1, self.month as i64 + 9)
        } else {
            (self.year as i64, self.month as i64 - 3)
        };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * m as u64 + 2) / 5 + self.day as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era as i64 * 146097 + doe as i64 - 719468;
        (days * 86400 + self.hour as i64 * 3600 + self.minute as i64 * 60 + self.second as i64)
            as f64
    }

    fn to_julianday(&self) -> f64 {
        self.to_unix() / SECONDS_PER_DAY + JULIAN_EPOCH_OFFSET
    }

    fn from_julianday(jd: f64) -> Self {
        let unix = (jd - JULIAN_EPOCH_OFFSET) * SECONDS_PER_DAY;
        Self::from_unix(unix)
    }

    fn format_date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    fn format_time(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.hour, self.minute, self.second)
    }

    fn format_datetime(&self) -> String {
        format!("{} {}", self.format_date(), self.format_time())
    }

    fn is_leap_year(&self) -> bool {
        (self.year % 4 == 0 && self.year % 100 != 0) || (self.year % 400 == 0)
    }

    fn days_in_month(&self) -> u32 {
        match self.month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if self.is_leap_year() {
                    29
                } else {
                    28
                }
            }
            _ => 30,
        }
    }

    fn day_of_week(&self) -> u32 {
        let unix = self.to_unix() as i64;
        let days = unix.div_euclid(86400);
        ((days + 4) % 7) as u32
    }

    fn day_of_year(&self) -> u32 {
        let month_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
        let mut doy = month_days[self.month as usize - 1] + self.day;
        if self.month > 2 && self.is_leap_year() {
            doy += 1;
        }
        doy
    }

    fn add_days(&mut self, days: i64) {
        let unix = self.to_unix() + days as f64 * 86400.0;
        *self = Self::from_unix(unix);
    }

    fn add_hours(&mut self, hours: i64) {
        let unix = self.to_unix() + hours as f64 * 3600.0;
        *self = Self::from_unix(unix);
    }

    fn add_minutes(&mut self, minutes: i64) {
        let unix = self.to_unix() + minutes as f64 * 60.0;
        *self = Self::from_unix(unix);
    }

    fn add_seconds(&mut self, seconds: i64) {
        let unix = self.to_unix() + seconds as f64;
        *self = Self::from_unix(unix);
    }

    fn add_months(&mut self, months: i32) {
        let total_months = (self.year * 12 + self.month as i32 - 1) + months;
        self.year = total_months.div_euclid(12);
        self.month = (total_months.rem_euclid(12) + 1) as u32;
        let max_day = self.days_in_month();
        if self.day > max_day {
            self.day = max_day;
        }
    }

    fn add_years(&mut self, years: i32) {
        self.year += years;
        let max_day = self.days_in_month();
        if self.day > max_day {
            self.day = max_day;
        }
    }
}

fn parse_timevalue(args: &[Value]) -> Result<Option<DateTime>> {
    if args.is_empty() {
        return Ok(Some(DateTime::now()));
    }

    let val = &args[0];
    match val {
        Value::Null => Ok(None),
        Value::Text(s) => {
            if s.eq_ignore_ascii_case("now") {
                Ok(Some(DateTime::now()))
            } else if let Some(dt) = parse_iso_datetime(s) {
                Ok(Some(dt))
            } else {
                Ok(None)
            }
        }
        Value::Integer(n) => Ok(Some(DateTime::from_unix(*n as f64))),
        Value::Real(f) => {
            if *f > 100000.0 {
                Ok(Some(DateTime::from_unix(*f)))
            } else {
                Ok(Some(DateTime::from_julianday(*f)))
            }
        }
        _ => Ok(None),
    }
}

fn parse_iso_datetime(s: &str) -> Option<DateTime> {
    let s = s.trim();

    if s.len() == 10 && s.as_bytes()[4] == b'-' && s.as_bytes()[7] == b'-' {
        let year = s[0..4].parse::<i32>().ok()?;
        let month = s[5..7].parse::<u32>().ok()?;
        let day = s[8..10].parse::<u32>().ok()?;
        if month >= 1 && month <= 12 && day >= 1 && day <= 31 {
            return Some(DateTime {
                year,
                month,
                day,
                hour: 0,
                minute: 0,
                second: 0,
            });
        }
    }

    if s.len() >= 19 {
        let sep = s.as_bytes()[10];
        if (sep == b' ' || sep == b'T') && s.as_bytes()[4] == b'-' {
            let year = s[0..4].parse::<i32>().ok()?;
            let month = s[5..7].parse::<u32>().ok()?;
            let day = s[8..10].parse::<u32>().ok()?;
            let hour = s[11..13].parse::<u32>().ok()?;
            let minute = s[14..16].parse::<u32>().ok()?;
            let second = s[17..19].parse::<u32>().ok()?;
            if month >= 1
                && month <= 12
                && day >= 1
                && day <= 31
                && hour <= 23
                && minute <= 59
                && second <= 59
            {
                return Some(DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                });
            }
        }
    }

    if s.len() == 8 && s.as_bytes()[2] == b':' && s.as_bytes()[5] == b':' {
        let hour = s[0..2].parse::<u32>().ok()?;
        let minute = s[3..5].parse::<u32>().ok()?;
        let second = s[6..8].parse::<u32>().ok()?;
        if hour <= 23 && minute <= 59 && second <= 59 {
            return Some(DateTime {
                year: 2000,
                month: 1,
                day: 1,
                hour,
                minute,
                second,
            });
        }
    }

    None
}

fn apply_modifiers(dt: &mut DateTime, args: &[Value]) {
    for arg in args {
        let s = value_to_text(arg);
        let s = s.trim();

        if s.eq_ignore_ascii_case("start of month") {
            dt.day = 1;
            dt.hour = 0;
            dt.minute = 0;
            dt.second = 0;
        } else if s.eq_ignore_ascii_case("start of year") {
            dt.month = 1;
            dt.day = 1;
            dt.hour = 0;
            dt.minute = 0;
            dt.second = 0;
        } else if s.eq_ignore_ascii_case("start of day") {
            dt.hour = 0;
            dt.minute = 0;
            dt.second = 0;
        } else if let Some(n) = parse_modifier_number(s, "days") {
            dt.add_days(n);
        } else if let Some(n) = parse_modifier_number(s, "day") {
            dt.add_days(n);
        } else if let Some(n) = parse_modifier_number(s, "hours") {
            dt.add_hours(n);
        } else if let Some(n) = parse_modifier_number(s, "hour") {
            dt.add_hours(n);
        } else if let Some(n) = parse_modifier_number(s, "minutes") {
            dt.add_minutes(n);
        } else if let Some(n) = parse_modifier_number(s, "minute") {
            dt.add_minutes(n);
        } else if let Some(n) = parse_modifier_number(s, "seconds") {
            dt.add_seconds(n);
        } else if let Some(n) = parse_modifier_number(s, "second") {
            dt.add_seconds(n);
        } else if let Some(n) = parse_modifier_number(s, "months") {
            dt.add_months(n as i32);
        } else if let Some(n) = parse_modifier_number(s, "month") {
            dt.add_months(n as i32);
        } else if let Some(n) = parse_modifier_number(s, "years") {
            dt.add_years(n as i32);
        } else if let Some(n) = parse_modifier_number(s, "year") {
            dt.add_years(n as i32);
        } else if s.eq_ignore_ascii_case("unixepoch") {
            // Already handled at parse time for integers
        }
    }
}

fn parse_modifier_number(s: &str, suffix: &str) -> Option<i64> {
    let s = s.trim();
    if !s.ends_with(suffix) {
        return None;
    }
    let num_part = s[..s.len() - suffix.len()].trim();
    num_part.parse::<i64>().ok()
}

fn strftime_format(fmt: &str, dt: &DateTime) -> String {
    let mut result = String::new();
    let mut chars = fmt.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            match chars.next() {
                Some('Y') => result.push_str(&format!("{:04}", dt.year)),
                Some('m') => result.push_str(&format!("{:02}", dt.month)),
                Some('d') => result.push_str(&format!("{:02}", dt.day)),
                Some('H') => result.push_str(&format!("{:02}", dt.hour)),
                Some('M') => result.push_str(&format!("{:02}", dt.minute)),
                Some('S') => result.push_str(&format!("{:02}", dt.second)),
                Some('s') => result.push_str(&format!("{}", dt.to_unix() as i64)),
                Some('j') => result.push_str(&format!("{:03}", dt.day_of_year())),
                Some('w') => result.push_str(&format!("{}", dt.day_of_week())),
                Some('W') => {
                    let doy = dt.day_of_year();
                    let dow = dt.day_of_week();
                    let week = (doy + 6 - dow) / 7;
                    result.push_str(&format!("{:02}", week));
                }
                Some('J') => result.push_str(&format!("{:.10}", dt.to_julianday())),
                Some('f') => result.push_str(&format!("{:02}.000", dt.second)),
                Some('%') => result.push('%'),
                Some(c) => {
                    result.push('%');
                    result.push(c);
                }
                None => result.push('%'),
            }
        } else {
            result.push(ch);
        }
    }

    result
}

fn current_unix_timestamp() -> f64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }

    #[cfg(target_arch = "wasm32")]
    {
        0.0
    }
}

pub(crate) fn eval_date(args: &[Value]) -> Result<Value> {
    let dt = parse_timevalue(args)?;
    match dt {
        None => Ok(Value::Null),
        Some(mut dt) => {
            if args.len() > 1 {
                apply_modifiers(&mut dt, &args[1..]);
            }
            Ok(Value::Text(dt.format_date()))
        }
    }
}

pub(crate) fn eval_time(args: &[Value]) -> Result<Value> {
    let dt = parse_timevalue(args)?;
    match dt {
        None => Ok(Value::Null),
        Some(mut dt) => {
            if args.len() > 1 {
                apply_modifiers(&mut dt, &args[1..]);
            }
            Ok(Value::Text(dt.format_time()))
        }
    }
}

pub(crate) fn eval_datetime(args: &[Value]) -> Result<Value> {
    let dt = parse_timevalue(args)?;
    match dt {
        None => Ok(Value::Null),
        Some(mut dt) => {
            if args.len() > 1 {
                apply_modifiers(&mut dt, &args[1..]);
            }
            Ok(Value::Text(dt.format_datetime()))
        }
    }
}

pub(crate) fn eval_julianday(args: &[Value]) -> Result<Value> {
    let dt = parse_timevalue(args)?;
    match dt {
        None => Ok(Value::Null),
        Some(mut dt) => {
            if args.len() > 1 {
                apply_modifiers(&mut dt, &args[1..]);
            }
            Ok(Value::Real(dt.to_julianday()))
        }
    }
}

pub(crate) fn eval_unixepoch(args: &[Value]) -> Result<Value> {
    let dt = parse_timevalue(args)?;
    match dt {
        None => Ok(Value::Null),
        Some(mut dt) => {
            if args.len() > 1 {
                apply_modifiers(&mut dt, &args[1..]);
            }
            Ok(Value::Integer(dt.to_unix() as i64))
        }
    }
}

pub(crate) fn eval_strftime(args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        return Err(Error::Other("STRFTIME requires at least 1 argument".into()));
    }
    let fmt = value_to_text(&args[0]);
    let dt = if args.len() > 1 {
        parse_timevalue(&args[1..])?
    } else {
        Some(DateTime::now())
    };
    match dt {
        None => Ok(Value::Null),
        Some(mut dt) => {
            if args.len() > 2 {
                apply_modifiers(&mut dt, &args[2..]);
            }
            Ok(Value::Text(strftime_format(&fmt, &dt)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_string() {
        let dt = parse_iso_datetime("2024-03-15").unwrap();
        assert_eq!(dt.year, 2024);
        assert_eq!(dt.month, 3);
        assert_eq!(dt.day, 15);
    }

    #[test]
    fn parse_datetime_string() {
        let dt = parse_iso_datetime("2024-03-15 10:30:45").unwrap();
        assert_eq!(dt.year, 2024);
        assert_eq!(dt.month, 3);
        assert_eq!(dt.day, 15);
        assert_eq!(dt.hour, 10);
        assert_eq!(dt.minute, 30);
        assert_eq!(dt.second, 45);
    }

    #[test]
    fn unix_roundtrip() {
        let dt = DateTime {
            year: 2024,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
        };
        let unix = dt.to_unix();
        let dt2 = DateTime::from_unix(unix);
        assert_eq!(dt2.year, 2024);
        assert_eq!(dt2.month, 1);
        assert_eq!(dt2.day, 1);
    }

    #[test]
    fn format_date() {
        let dt = DateTime {
            year: 2024,
            month: 3,
            day: 5,
            hour: 14,
            minute: 30,
            second: 0,
        };
        assert_eq!(dt.format_date(), "2024-03-05");
        assert_eq!(dt.format_time(), "14:30:00");
        assert_eq!(dt.format_datetime(), "2024-03-05 14:30:00");
    }

    #[test]
    fn add_days() {
        let mut dt = DateTime {
            year: 2024,
            month: 2,
            day: 28,
            hour: 0,
            minute: 0,
            second: 0,
        };
        dt.add_days(1);
        assert_eq!(dt.month, 2);
        assert_eq!(dt.day, 29); // 2024 is a leap year
        dt.add_days(1);
        assert_eq!(dt.month, 3);
        assert_eq!(dt.day, 1);
    }

    #[test]
    fn add_months() {
        let mut dt = DateTime {
            year: 2024,
            month: 1,
            day: 31,
            hour: 0,
            minute: 0,
            second: 0,
        };
        dt.add_months(1);
        assert_eq!(dt.month, 2);
        assert_eq!(dt.day, 29); // clamped to Feb 29 in leap year
    }

    #[test]
    fn strftime_basic() {
        let dt = DateTime {
            year: 2024,
            month: 3,
            day: 15,
            hour: 10,
            minute: 30,
            second: 45,
        };
        assert_eq!(strftime_format("%Y-%m-%d", &dt), "2024-03-15");
        assert_eq!(strftime_format("%H:%M:%S", &dt), "10:30:45");
    }

    #[test]
    fn eval_date_literal() {
        let r = eval_date(&[Value::Text("2024-06-15".into())]).unwrap();
        assert_eq!(r, Value::Text("2024-06-15".into()));
    }

    #[test]
    fn eval_date_with_modifier() {
        let r = eval_date(&[
            Value::Text("2024-06-15".into()),
            Value::Text("+3 days".into()),
        ])
        .unwrap();
        assert_eq!(r, Value::Text("2024-06-18".into()));
    }

    #[test]
    fn eval_date_start_of_month() {
        let r = eval_date(&[
            Value::Text("2024-06-15".into()),
            Value::Text("start of month".into()),
        ])
        .unwrap();
        assert_eq!(r, Value::Text("2024-06-01".into()));
    }
}
