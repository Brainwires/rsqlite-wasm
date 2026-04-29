/// SQLite variable-length integer encoding.
///
/// Bytes 0..7: high bit is continuation flag, low 7 bits are data.
/// Byte 8 (if reached): all 8 bits are data.
/// Encodes values 0..2^64-1 in 1..9 bytes.

/// Decode a varint from `data`. Returns `(value, bytes_consumed)`.
/// Panics if `data` is empty.
pub fn read_varint(data: &[u8]) -> (u64, usize) {
    let mut value: u64 = 0;

    for i in 0..8 {
        if i >= data.len() {
            return (value, i);
        }
        let byte = data[i];
        value = (value << 7) | (byte & 0x7F) as u64;
        if byte & 0x80 == 0 {
            return (value, i + 1);
        }
    }

    // 9th byte: all 8 bits are data
    if data.len() > 8 {
        value = (value << 8) | data[8] as u64;
        (value, 9)
    } else {
        (value, data.len())
    }
}

/// Encode a varint into `buf`. Returns the number of bytes written (1..9).
/// `buf` must be at least 9 bytes.
pub fn write_varint(value: u64, buf: &mut [u8]) -> usize {
    debug_assert!(buf.len() >= 9);

    if value <= 0x7F {
        buf[0] = value as u8;
        return 1;
    }

    // Determine how many bytes we need.
    let len = varint_len(value);

    if len == 9 {
        // 9-byte encoding: first 8 bytes carry 7 bits each (56 bits), 9th byte carries 8 bits.
        let hi = value >> 8;
        for i in 0..8 {
            let shift = 7 * (7 - i);
            let byte = ((hi >> shift) & 0x7F) as u8;
            buf[i] = byte | 0x80;
        }
        buf[8] = value as u8;
    } else {
        for i in (0..len).rev() {
            if i == len - 1 {
                buf[i] = (value & 0x7F) as u8; // last byte: no continuation
            } else {
                let shift = 7 * (len - 1 - i);
                buf[i] = ((value >> shift) & 0x7F) as u8 | 0x80;
            }
        }
    }

    len
}

/// Returns the number of bytes needed to encode `value` as a varint.
pub fn varint_len(value: u64) -> usize {
    match value {
        0..=0x7F => 1,
        0..=0x3FFF => 2,
        0..=0x1F_FFFF => 3,
        0..=0x0FFF_FFFF => 4,
        0..=0x07_FFFF_FFFF => 5,
        0..=0x03FF_FFFF_FFFF => 6,
        0..=0x01_FFFF_FFFF_FFFF => 7,
        0..=0x00FF_FFFF_FFFF_FFFF => 8,
        _ => 9,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: u64) {
        let mut buf = [0u8; 9];
        let written = write_varint(value, &mut buf);
        let (decoded, consumed) = read_varint(&buf[..written]);
        assert_eq!(
            decoded, value,
            "round-trip failed for {value}: wrote {written} bytes, decoded {decoded}"
        );
        assert_eq!(consumed, written, "consumed bytes mismatch for {value}");
    }

    #[test]
    fn single_byte_values() {
        for v in 0..=127u64 {
            round_trip(v);
            let mut buf = [0u8; 9];
            assert_eq!(write_varint(v, &mut buf), 1);
        }
    }

    #[test]
    fn two_byte_values() {
        round_trip(128);
        round_trip(0x3FFF);
        round_trip(255);
        round_trip(1000);
    }

    #[test]
    fn multi_byte_boundaries() {
        // Test at each size boundary
        let boundaries = [
            (0x7F, 1),
            (0x80, 2),
            (0x3FFF, 2),
            (0x4000, 3),
            (0x1F_FFFF, 3),
            (0x20_0000, 4),
            (0x0FFF_FFFF, 4),
            (0x1000_0000, 5),
            (0x07_FFFF_FFFF, 5),
            (0x08_0000_0000, 6),
            (0x03FF_FFFF_FFFF, 6),
            (0x0400_0000_0000, 7),
            (0x01_FFFF_FFFF_FFFF, 7),
            (0x02_0000_0000_0000, 8),
            (0x00FF_FFFF_FFFF_FFFF, 8),
            (0x0100_0000_0000_0000, 9),
        ];
        for (value, expected_len) in boundaries {
            let mut buf = [0u8; 9];
            let len = write_varint(value, &mut buf);
            assert_eq!(
                len, expected_len,
                "varint_len mismatch for {value:#x}: got {len}, expected {expected_len}"
            );
            round_trip(value);
        }
    }

    #[test]
    fn max_value() {
        round_trip(u64::MAX);
        let mut buf = [0u8; 9];
        assert_eq!(write_varint(u64::MAX, &mut buf), 9);
    }

    #[test]
    fn zero() {
        round_trip(0);
        let mut buf = [0u8; 9];
        let len = write_varint(0, &mut buf);
        assert_eq!(len, 1);
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn varint_len_matches_write() {
        let values = [
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            2097151,
            268435455,
            u32::MAX as u64,
            u64::MAX / 2,
            u64::MAX,
        ];
        for v in values {
            let expected = varint_len(v);
            let mut buf = [0u8; 9];
            let actual = write_varint(v, &mut buf);
            assert_eq!(
                actual, expected,
                "varint_len({v}) = {expected} but write_varint wrote {actual} bytes"
            );
        }
    }

    #[test]
    fn read_varint_from_longer_buffer() {
        let mut buf = [0u8; 20];
        let len = write_varint(12345, &mut buf);
        buf[len] = 0xFF; // junk after the varint
        let (value, consumed) = read_varint(&buf);
        assert_eq!(value, 12345);
        assert_eq!(consumed, len);
    }

    #[test]
    fn sqlite_specific_values() {
        // Values commonly seen in SQLite databases
        round_trip(1); // boolean true
        round_trip(13); // serial type for empty text
        round_trip(12); // serial type for empty blob
        round_trip(4096); // page size
        round_trip(100); // header size on page 1
    }
}
