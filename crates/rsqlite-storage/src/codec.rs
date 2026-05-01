use crate::error::{Result, StorageError};
use crate::varint;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(n) => write!(f, "{n}"),
            Value::Real(n) => write!(f, "{n}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Blob(b) => write!(f, "X'{}'", hex_encode(b)),
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

#[derive(Debug, Clone)]
pub struct Record {
    pub values: Vec<Value>,
}

impl Record {
    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.is_empty() {
            return Ok(Record { values: vec![] });
        }

        let (header_size, header_size_len) = varint::read_varint(payload);
        let header_size = header_size as usize;

        if header_size > payload.len() {
            return Err(StorageError::Corrupt(format!(
                "record header size {header_size} exceeds payload length {}",
                payload.len()
            )));
        }

        // Read serial types from the header
        let mut serial_types = Vec::new();
        let mut pos = header_size_len;
        while pos < header_size {
            let (serial_type, len) = varint::read_varint(&payload[pos..]);
            serial_types.push(serial_type);
            pos += len;
        }

        // Decode values from the body
        let mut values = Vec::with_capacity(serial_types.len());
        let mut body_pos = header_size;

        for &serial_type in &serial_types {
            let (value, size) = decode_value(serial_type, &payload[body_pos..])?;
            values.push(value);
            body_pos += size;
        }

        Ok(Record { values })
    }

    pub fn encode(&self) -> Vec<u8> {
        let serial_types: Vec<u64> = self.values.iter().map(serial_type_for).collect();

        // Calculate header size
        let mut header_content_size = 0usize;
        for &st in &serial_types {
            header_content_size += varint::varint_len(st);
        }

        let header_size_varint_len = varint::varint_len((header_content_size + 1) as u64);
        // The header size includes itself
        let mut total_header_size = header_content_size + header_size_varint_len;
        // Recheck in case the varint length changes
        if varint::varint_len(total_header_size as u64) != header_size_varint_len {
            total_header_size = header_content_size + varint::varint_len(total_header_size as u64);
        }

        let mut body_size = 0usize;
        for (st, val) in serial_types.iter().zip(self.values.iter()) {
            body_size += value_byte_size(*st, val);
        }

        let mut buf = Vec::with_capacity(total_header_size + body_size);

        // Write header size
        let mut tmp = [0u8; 9];
        let n = varint::write_varint(total_header_size as u64, &mut tmp);
        buf.extend_from_slice(&tmp[..n]);

        // Write serial types
        for &st in &serial_types {
            let n = varint::write_varint(st, &mut tmp);
            buf.extend_from_slice(&tmp[..n]);
        }

        // Write values
        for (st, val) in serial_types.iter().zip(self.values.iter()) {
            encode_value(*st, val, &mut buf);
        }

        buf
    }
}

fn serial_type_for(value: &Value) -> u64 {
    match value {
        Value::Null => 0,
        Value::Integer(n) => {
            let n = *n;
            if n == 0 {
                8
            } else if n == 1 {
                9
            } else if n >= -128 && n <= 127 {
                1
            } else if n >= -32768 && n <= 32767 {
                2
            } else if n >= -8388608 && n <= 8388607 {
                3
            } else if n >= -2147483648 && n <= 2147483647 {
                4
            } else if n >= -140737488355328 && n <= 140737488355327 {
                5
            } else {
                6
            }
        }
        Value::Real(_) => 7,
        Value::Text(s) => (s.len() as u64) * 2 + 13,
        Value::Blob(b) => (b.len() as u64) * 2 + 12,
    }
}

fn value_byte_size(serial_type: u64, _value: &Value) -> usize {
    match serial_type {
        0 | 8 | 9 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        7 => 8,
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize,
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize,
        _ => 0,
    }
}

fn content_size_for_serial_type(serial_type: u64) -> usize {
    match serial_type {
        0 | 8 | 9 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        7 => 8,
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize,
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize,
        _ => 0,
    }
}

fn decode_value(serial_type: u64, data: &[u8]) -> Result<(Value, usize)> {
    match serial_type {
        0 => Ok((Value::Null, 0)),
        1 => {
            let v = data[0] as i8 as i64;
            Ok((Value::Integer(v), 1))
        }
        2 => {
            let v = i16::from_be_bytes([data[0], data[1]]) as i64;
            Ok((Value::Integer(v), 2))
        }
        3 => {
            // 3-byte signed integer (big-endian)
            let sign_extend = if data[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let v = i32::from_be_bytes([sign_extend, data[0], data[1], data[2]]) as i64;
            Ok((Value::Integer(v), 3))
        }
        4 => {
            let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as i64;
            Ok((Value::Integer(v), 4))
        }
        5 => {
            // 6-byte signed integer (big-endian)
            let sign_extend = if data[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let v = i64::from_be_bytes([
                sign_extend,
                sign_extend,
                data[0],
                data[1],
                data[2],
                data[3],
                data[4],
                data[5],
            ]);
            Ok((Value::Integer(v), 6))
        }
        6 => {
            let v = i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Ok((Value::Integer(v), 8))
        }
        7 => {
            let bits = u64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Ok((Value::Real(f64::from_bits(bits)), 8))
        }
        8 => Ok((Value::Integer(0), 0)),
        9 => Ok((Value::Integer(1), 0)),
        n if n >= 12 && n % 2 == 0 => {
            let size = content_size_for_serial_type(n);
            let blob = data[..size].to_vec();
            Ok((Value::Blob(blob), size))
        }
        n if n >= 13 && n % 2 == 1 => {
            let size = content_size_for_serial_type(n);
            let text = String::from_utf8_lossy(&data[..size]).into_owned();
            Ok((Value::Text(text), size))
        }
        _ => Err(StorageError::Corrupt(format!(
            "unknown serial type: {serial_type}"
        ))),
    }
}

fn encode_value(serial_type: u64, value: &Value, buf: &mut Vec<u8>) {
    match serial_type {
        0 | 8 | 9 => {}
        1 => {
            if let Value::Integer(n) = value {
                buf.push(*n as u8);
            }
        }
        2 => {
            if let Value::Integer(n) = value {
                buf.extend_from_slice(&(*n as i16).to_be_bytes());
            }
        }
        3 => {
            if let Value::Integer(n) = value {
                let bytes = (*n as i32).to_be_bytes();
                buf.extend_from_slice(&bytes[1..4]);
            }
        }
        4 => {
            if let Value::Integer(n) = value {
                buf.extend_from_slice(&(*n as i32).to_be_bytes());
            }
        }
        5 => {
            if let Value::Integer(n) = value {
                let bytes = n.to_be_bytes();
                buf.extend_from_slice(&bytes[2..8]);
            }
        }
        6 => {
            if let Value::Integer(n) = value {
                buf.extend_from_slice(&n.to_be_bytes());
            }
        }
        7 => {
            if let Value::Real(n) = value {
                buf.extend_from_slice(&n.to_bits().to_be_bytes());
            }
        }
        n if n >= 12 && n % 2 == 0 => {
            if let Value::Blob(b) = value {
                buf.extend_from_slice(b);
            }
        }
        n if n >= 13 && n % 2 == 1 => {
            if let Value::Text(s) = value {
                buf.extend_from_slice(s.as_bytes());
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(values: &[Value]) {
        let record = Record {
            values: values.to_vec(),
        };
        let encoded = record.encode();
        let decoded = Record::decode(&encoded).unwrap();
        assert_eq!(
            record.values.len(),
            decoded.values.len(),
            "value count mismatch"
        );
        for (i, (orig, dec)) in record.values.iter().zip(decoded.values.iter()).enumerate() {
            assert_eq!(orig, dec, "value mismatch at index {i}");
        }
    }

    #[test]
    fn empty_record() {
        round_trip(&[]);
    }

    #[test]
    fn null_value() {
        round_trip(&[Value::Null]);
    }

    #[test]
    fn integer_zero_and_one() {
        round_trip(&[Value::Integer(0), Value::Integer(1)]);
    }

    #[test]
    fn small_integers() {
        round_trip(&[Value::Integer(42), Value::Integer(-1), Value::Integer(127)]);
    }

    #[test]
    fn medium_integers() {
        round_trip(&[Value::Integer(1000), Value::Integer(-30000)]);
    }

    #[test]
    fn large_integers() {
        round_trip(&[
            Value::Integer(i32::MAX as i64),
            Value::Integer(i32::MIN as i64),
            Value::Integer(i64::MAX),
            Value::Integer(i64::MIN),
        ]);
    }

    #[test]
    #[allow(clippy::approx_constant)] // arbitrary float values, not π
    fn real_values() {
        round_trip(&[Value::Real(3.14), Value::Real(-0.0), Value::Real(f64::MAX)]);
    }

    #[test]
    fn text_values() {
        round_trip(&[
            Value::Text(String::new()),
            Value::Text("hello".to_string()),
            Value::Text("hello world, this is a longer string!".to_string()),
        ]);
    }

    #[test]
    fn blob_values() {
        round_trip(&[
            Value::Blob(vec![]),
            Value::Blob(vec![0x01, 0x02, 0x03]),
            Value::Blob(vec![0xFF; 100]),
        ]);
    }

    #[test]
    #[allow(clippy::approx_constant)] // arbitrary float values, not e
    fn mixed_types() {
        round_trip(&[
            Value::Integer(1),
            Value::Text("hello".to_string()),
            Value::Null,
            Value::Real(2.718),
            Value::Blob(vec![0xDE, 0xAD]),
            Value::Integer(0),
        ]);
    }

    #[test]
    fn sqlite_schema_like_record() {
        // sqlite_schema rows look like: (type TEXT, name TEXT, tbl_name TEXT, rootpage INT, sql TEXT)
        round_trip(&[
            Value::Text("table".to_string()),
            Value::Text("users".to_string()),
            Value::Text("users".to_string()),
            Value::Integer(2),
            Value::Text(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)".to_string(),
            ),
        ]);
    }
}
