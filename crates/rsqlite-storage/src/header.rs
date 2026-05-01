use crate::error::{Result, StorageError};

pub const HEADER_SIZE: usize = 100;
pub const MAGIC: &[u8; 16] = b"SQLite format 3\0";
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextEncoding {
    Utf8 = 1,
    Utf16Le = 2,
    Utf16Be = 3,
}

impl TextEncoding {
    fn from_u32(v: u32) -> Result<Self> {
        match v {
            1 => Ok(Self::Utf8),
            2 => Ok(Self::Utf16Le),
            3 => Ok(Self::Utf16Be),
            _ => Err(StorageError::InvalidHeader(format!(
                "invalid text encoding: {v}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    pub page_size: u32,
    pub write_version: u8,
    pub read_version: u8,
    pub reserved_bytes: u8,
    pub max_embedded_payload_fraction: u8,
    pub min_embedded_payload_fraction: u8,
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub database_size: u32,
    pub first_freelist_page: u32,
    pub freelist_count: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub default_cache_size: u32,
    pub largest_root_btree_page: u32,
    pub text_encoding: TextEncoding,
    pub user_version: u32,
    pub incremental_vacuum: u32,
    pub application_id: u32,
    pub version_valid_for: u32,
    pub sqlite_version: u32,
}

impl DatabaseHeader {
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(StorageError::InvalidHeader(format!(
                "header too short: {} bytes (need {HEADER_SIZE})",
                data.len()
            )));
        }

        if &data[0..16] != MAGIC {
            return Err(StorageError::InvalidHeader(
                "invalid magic string".to_string(),
            ));
        }

        let raw_page_size = u16::from_be_bytes([data[16], data[17]]) as u32;
        let page_size = if raw_page_size == 1 {
            65536
        } else {
            raw_page_size
        };

        if !page_size.is_power_of_two() || page_size < 512 || page_size > 65536 {
            return Err(StorageError::InvalidHeader(format!(
                "invalid page size: {page_size}"
            )));
        }

        let write_version = data[18];
        let read_version = data[19];
        let reserved_bytes = data[20];
        let max_embedded_payload_fraction = data[21];
        let min_embedded_payload_fraction = data[22];
        let leaf_payload_fraction = data[23];

        if max_embedded_payload_fraction != 64 {
            return Err(StorageError::InvalidHeader(format!(
                "max embedded payload fraction must be 64, got {max_embedded_payload_fraction}"
            )));
        }
        if min_embedded_payload_fraction != 32 {
            return Err(StorageError::InvalidHeader(format!(
                "min embedded payload fraction must be 32, got {min_embedded_payload_fraction}"
            )));
        }
        if leaf_payload_fraction != 32 {
            return Err(StorageError::InvalidHeader(format!(
                "leaf payload fraction must be 32, got {leaf_payload_fraction}"
            )));
        }

        let text_encoding = TextEncoding::from_u32(read_be_u32(data, 56))?;

        Ok(Self {
            page_size,
            write_version,
            read_version,
            reserved_bytes,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter: read_be_u32(data, 24),
            database_size: read_be_u32(data, 28),
            first_freelist_page: read_be_u32(data, 32),
            freelist_count: read_be_u32(data, 36),
            schema_cookie: read_be_u32(data, 40),
            schema_format: read_be_u32(data, 44),
            default_cache_size: read_be_u32(data, 48),
            largest_root_btree_page: read_be_u32(data, 52),
            text_encoding,
            user_version: read_be_u32(data, 60),
            incremental_vacuum: read_be_u32(data, 64),
            application_id: read_be_u32(data, 68),
            // bytes 72..92 are reserved (zeroes)
            version_valid_for: read_be_u32(data, 92),
            sqlite_version: read_be_u32(data, 96),
        })
    }

    /// Serialize this header into a 100-byte buffer.
    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= HEADER_SIZE);

        buf[0..16].copy_from_slice(MAGIC);

        let raw_page_size: u16 = if self.page_size == 65536 {
            1
        } else {
            self.page_size as u16
        };
        buf[16..18].copy_from_slice(&raw_page_size.to_be_bytes());

        buf[18] = self.write_version;
        buf[19] = self.read_version;
        buf[20] = self.reserved_bytes;
        buf[21] = self.max_embedded_payload_fraction;
        buf[22] = self.min_embedded_payload_fraction;
        buf[23] = self.leaf_payload_fraction;

        write_be_u32(buf, 24, self.file_change_counter);
        write_be_u32(buf, 28, self.database_size);
        write_be_u32(buf, 32, self.first_freelist_page);
        write_be_u32(buf, 36, self.freelist_count);
        write_be_u32(buf, 40, self.schema_cookie);
        write_be_u32(buf, 44, self.schema_format);
        write_be_u32(buf, 48, self.default_cache_size);
        write_be_u32(buf, 52, self.largest_root_btree_page);
        write_be_u32(buf, 56, self.text_encoding as u32);
        write_be_u32(buf, 60, self.user_version);
        write_be_u32(buf, 64, self.incremental_vacuum);
        write_be_u32(buf, 68, self.application_id);

        // Reserved bytes 72..92 must be zero
        buf[72..92].fill(0);

        write_be_u32(buf, 92, self.version_valid_for);
        write_be_u32(buf, 96, self.sqlite_version);
    }

    /// Create a default header for a new database.
    pub fn new_default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            write_version: 1, // legacy journal mode
            read_version: 1,
            reserved_bytes: 0,
            max_embedded_payload_fraction: 64,
            min_embedded_payload_fraction: 32,
            leaf_payload_fraction: 32,
            file_change_counter: 0,
            database_size: 1, // at least 1 page (the header page)
            first_freelist_page: 0,
            freelist_count: 0,
            schema_cookie: 0,
            schema_format: 4,
            default_cache_size: 0,
            largest_root_btree_page: 0,
            text_encoding: TextEncoding::Utf8,
            user_version: 0,
            incremental_vacuum: 0,
            application_id: 0,
            version_valid_for: 0,
            sqlite_version: 0,
        }
    }

    pub fn usable_size(&self) -> u32 {
        self.page_size - self.reserved_bytes as u32
    }
}

fn read_be_u32(data: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

fn write_be_u32(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_valid_header() -> [u8; 100] {
        let mut buf = [0u8; 100];
        let header = DatabaseHeader::new_default();
        header.write(&mut buf);
        buf
    }

    #[test]
    fn round_trip_default_header() {
        let original = DatabaseHeader::new_default();
        let mut buf = [0u8; 100];
        original.write(&mut buf);
        let parsed = DatabaseHeader::parse(&buf).unwrap();

        assert_eq!(parsed.page_size, 4096);
        assert_eq!(parsed.write_version, 1);
        assert_eq!(parsed.read_version, 1);
        assert_eq!(parsed.reserved_bytes, 0);
        assert_eq!(parsed.text_encoding, TextEncoding::Utf8);
        assert_eq!(parsed.schema_format, 4);
        assert_eq!(parsed.database_size, 1);
    }

    #[test]
    fn magic_string() {
        let buf = make_valid_header();
        assert_eq!(&buf[0..16], MAGIC);
    }

    #[test]
    fn invalid_magic_rejected() {
        let mut buf = make_valid_header();
        buf[0] = b'X';
        assert!(DatabaseHeader::parse(&buf).is_err());
    }

    #[test]
    fn too_short_rejected() {
        let buf = [0u8; 50];
        assert!(DatabaseHeader::parse(&buf).is_err());
    }

    #[test]
    fn page_size_65536() {
        let mut header = DatabaseHeader::new_default();
        header.page_size = 65536;
        let mut buf = [0u8; 100];
        header.write(&mut buf);

        // Page size 65536 is encoded as 1
        assert_eq!(u16::from_be_bytes([buf[16], buf[17]]), 1);

        let parsed = DatabaseHeader::parse(&buf).unwrap();
        assert_eq!(parsed.page_size, 65536);
    }

    #[test]
    fn various_page_sizes() {
        for &size in &[512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let mut header = DatabaseHeader::new_default();
            header.page_size = size;
            let mut buf = [0u8; 100];
            header.write(&mut buf);
            let parsed = DatabaseHeader::parse(&buf).unwrap();
            assert_eq!(parsed.page_size, size);
        }
    }

    #[test]
    fn invalid_page_size_rejected() {
        let mut buf = make_valid_header();
        // Set page size to 3 (not a power of two, not valid)
        buf[16] = 0;
        buf[17] = 3;
        assert!(DatabaseHeader::parse(&buf).is_err());
    }

    #[test]
    fn text_encodings() {
        for (enc, expected) in [
            (TextEncoding::Utf8, 1u32),
            (TextEncoding::Utf16Le, 2),
            (TextEncoding::Utf16Be, 3),
        ] {
            let mut header = DatabaseHeader::new_default();
            header.text_encoding = enc;
            let mut buf = [0u8; 100];
            header.write(&mut buf);
            assert_eq!(read_be_u32(&buf, 56), expected);
            let parsed = DatabaseHeader::parse(&buf).unwrap();
            assert_eq!(parsed.text_encoding, enc);
        }
    }

    #[test]
    fn usable_size() {
        let mut header = DatabaseHeader::new_default();
        assert_eq!(header.usable_size(), 4096);
        header.reserved_bytes = 8;
        assert_eq!(header.usable_size(), 4088);
    }

    #[test]
    fn round_trip_with_all_fields_set() {
        let original = DatabaseHeader {
            page_size: 8192,
            write_version: 2,
            read_version: 2,
            reserved_bytes: 0,
            max_embedded_payload_fraction: 64,
            min_embedded_payload_fraction: 32,
            leaf_payload_fraction: 32,
            file_change_counter: 42,
            database_size: 100,
            first_freelist_page: 5,
            freelist_count: 3,
            schema_cookie: 7,
            schema_format: 4,
            default_cache_size: 2000,
            largest_root_btree_page: 10,
            text_encoding: TextEncoding::Utf8,
            user_version: 1,
            incremental_vacuum: 0,
            application_id: 0x12345678,
            version_valid_for: 42,
            sqlite_version: 3039004,
        };

        let mut buf = [0u8; 100];
        original.write(&mut buf);
        let parsed = DatabaseHeader::parse(&buf).unwrap();

        assert_eq!(parsed.page_size, 8192);
        assert_eq!(parsed.write_version, 2);
        assert_eq!(parsed.file_change_counter, 42);
        assert_eq!(parsed.database_size, 100);
        assert_eq!(parsed.first_freelist_page, 5);
        assert_eq!(parsed.freelist_count, 3);
        assert_eq!(parsed.schema_cookie, 7);
        assert_eq!(parsed.default_cache_size, 2000);
        assert_eq!(parsed.largest_root_btree_page, 10);
        assert_eq!(parsed.user_version, 1);
        assert_eq!(parsed.application_id, 0x12345678);
        assert_eq!(parsed.sqlite_version, 3039004);
    }

    #[test]
    fn parse_real_sqlite_header() {
        // Create a database with sqlite3 and try to read its header.
        // This test verifies format compatibility.
        // If sqlite3 is not available, the test is skipped.
        let test_db = "/tmp/rsqlite_test_header.db";
        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO test VALUES (1, 'hello');")
            .status();

        match status {
            Ok(s) if s.success() => {
                let data = std::fs::read(test_db).unwrap();
                let header = DatabaseHeader::parse(&data).unwrap();
                assert!(header.page_size >= 512);
                assert!(header.page_size <= 65536);
                assert!(header.page_size.is_power_of_two());
                assert_eq!(header.text_encoding, TextEncoding::Utf8);
                assert!(header.schema_format >= 1 && header.schema_format <= 4);
                assert!(header.database_size >= 1);
                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping real header test");
            }
        }
    }
}
