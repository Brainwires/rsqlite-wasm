//! Transparent file-sharding VFS wrapper.
//!
//! Browsers cap individual storage-file sizes (OPFS and IndexedDB both have
//! per-file limits, often ≤ 4 GB). `MultiplexVfs` wraps any inner [`Vfs`] and
//! presents a single logical file whose bytes are spread across many fixed-size
//! backing files. With the default 1 GB chunk size and the three-digit suffix
//! used here, a logical file can grow to ~1 TB before the suffix would need to
//! widen.
//!
//! # Naming
//!
//! A logical path `db.sqlite` maps to physical paths `db.sqlite.000`,
//! `db.sqlite.001`, … on the inner VFS.
//!
//! # Backward compatibility
//!
//! If a non-sharded file exists at the logical path (`db.sqlite` with no
//! `.000` suffix), it is treated as **legacy mode**: shard 0 keeps the bare
//! name, additional shards use the suffix scheme. This lets databases created
//! with an older single-file VFS keep opening — and grow into multiplex mode
//! transparently when they exceed `chunk_size`.
//!
//! # Locking
//!
//! Lock state is recorded on shard 0; later shards do not participate. The
//! engine treats the logical file as one coordination point.

use crate::{LockType, OpenFlags, Result, SyncFlags, Vfs, VfsError, VfsFile};

/// Default chunk size: 1 GB. With three suffix digits, this lets a logical
/// database reach ~1 TB before the suffix scheme runs out.
pub const DEFAULT_CHUNK_SIZE: u64 = 1 << 30;

const SUFFIX_DIGITS: usize = 3;

fn shard_name(base: &str, idx: usize, legacy: bool) -> String {
    if idx == 0 && legacy {
        base.to_string()
    } else {
        format!("{}.{:0width$}", base, idx, width = SUFFIX_DIGITS)
    }
}

/// A [`Vfs`] that shards each logical file across multiple inner files.
pub struct MultiplexVfs {
    inner: Box<dyn Vfs>,
    chunk_size: u64,
}

impl MultiplexVfs {
    /// Wrap `inner` with the default 1 GB chunk size.
    pub fn new(inner: Box<dyn Vfs>) -> Self {
        Self::with_chunk_size(inner, DEFAULT_CHUNK_SIZE)
    }

    /// Wrap `inner` with a custom chunk size. Panics if `chunk_size == 0`.
    pub fn with_chunk_size(inner: Box<dyn Vfs>, chunk_size: u64) -> Self {
        assert!(chunk_size > 0, "chunk_size must be > 0");
        Self { inner, chunk_size }
    }

    /// Probe the inner VFS to determine whether `base` exists, in what form,
    /// and how many *populated* shards it has. Returns
    /// `(legacy_mode, shard_count)`.
    ///
    /// Shard 0 always counts if it exists (a freshly-created DB has an empty
    /// shard 0). Higher-index shards only count if they exist *and* contain
    /// data — this lets backends pre-register empty shard files (e.g. OPFS's
    /// async `SyncAccessHandle` registration) without inflating the apparent
    /// logical size of the database.
    fn discover_shards(&self, base: &str) -> Result<(bool, usize)> {
        let probe = OpenFlags {
            create: false,
            read_write: false,
            delete_on_close: false,
        };
        let first_multi = shard_name(base, 0, false);
        if self.inner.exists(&first_multi)? {
            let mut count = 1usize;
            loop {
                let next = shard_name(base, count, false);
                if !self.inner.exists(&next)? {
                    return Ok((false, count));
                }
                let f = self.inner.open(&next, probe.clone())?;
                let sz = f.file_size()?;
                drop(f);
                if sz == 0 {
                    return Ok((false, count));
                }
                count += 1;
            }
        }
        if self.inner.exists(base)? {
            let mut count = 1usize;
            loop {
                let next = shard_name(base, count, true);
                if !self.inner.exists(&next)? {
                    return Ok((true, count));
                }
                let f = self.inner.open(&next, probe.clone())?;
                let sz = f.file_size()?;
                drop(f);
                if sz == 0 {
                    return Ok((true, count));
                }
                count += 1;
            }
        }
        Ok((false, 0))
    }
}

impl Vfs for MultiplexVfs {
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
        let (legacy, existing_count) = self.discover_shards(path)?;
        let mut shards: Vec<Box<dyn VfsFile>> = Vec::with_capacity(existing_count.max(1));

        if existing_count == 0 {
            if !flags.create {
                return Err(VfsError::NotFound(path.to_string()));
            }
            // New databases always start in pure multiplex mode (`.000`).
            let name = shard_name(path, 0, false);
            let shard = self.inner.open(&name, flags.clone())?;
            shards.push(shard);
        } else {
            let mut existing_flags = flags.clone();
            existing_flags.create = false;
            existing_flags.delete_on_close = false;
            for i in 0..existing_count {
                let name = shard_name(path, i, legacy);
                let shard = self.inner.open(&name, existing_flags.clone())?;
                shards.push(shard);
            }
        }

        let delete_on_close = flags.delete_on_close;
        Ok(Box::new(MultiplexFile {
            base_path: path.to_string(),
            chunk_size: self.chunk_size,
            inner: self.inner.clone_box(),
            flags,
            shards,
            legacy,
            lock: LockType::None,
            delete_on_close,
        }))
    }

    fn delete(&self, path: &str) -> Result<()> {
        let (legacy, count) = self.discover_shards(path)?;
        for i in 0..count {
            let name = shard_name(path, i, legacy);
            self.inner.delete(&name)?;
        }
        Ok(())
    }

    fn exists(&self, path: &str) -> Result<bool> {
        let (_, count) = self.discover_shards(path)?;
        Ok(count > 0)
    }

    fn clone_box(&self) -> Box<dyn Vfs> {
        Box::new(MultiplexVfs {
            inner: self.inner.clone_box(),
            chunk_size: self.chunk_size,
        })
    }
}

/// A logical file backed by one or more inner VFS files.
pub struct MultiplexFile {
    base_path: String,
    chunk_size: u64,
    inner: Box<dyn Vfs>,
    flags: OpenFlags,
    shards: Vec<Box<dyn VfsFile>>,
    legacy: bool,
    lock: LockType,
    delete_on_close: bool,
}

impl MultiplexFile {
    fn shard_name_for(&self, idx: usize) -> String {
        shard_name(&self.base_path, idx, self.legacy)
    }

    /// Make sure shards `0..=idx` are open, creating new ones via the inner
    /// VFS when the logical file grows. Intermediate shards (and the
    /// previous tail when it sat below `chunk_size`) are zero-padded to
    /// `chunk_size` so reads across sparse holes see zeros instead of EOF.
    fn ensure_shard(&mut self, idx: usize) -> Result<()> {
        if !self.shards.is_empty() && self.shards.len() <= idx {
            let last = self.shards.len() - 1;
            let cur = self.shards[last].file_size()?;
            if cur < self.chunk_size {
                self.shards[last].truncate(self.chunk_size)?;
            }
        }
        while self.shards.len() <= idx {
            let i = self.shards.len();
            let name = self.shard_name_for(i);
            let mut flags = self.flags.clone();
            flags.create = true;
            // Shard files persist as long as the logical file exists; the
            // logical-level delete_on_close runs in our Drop instead.
            flags.delete_on_close = false;
            let mut shard = self.inner.open(&name, flags)?;
            if i < idx {
                shard.truncate(self.chunk_size)?;
            }
            self.shards.push(shard);
        }
        Ok(())
    }
}

impl VfsFile for MultiplexFile {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let mut total = 0usize;
        let mut cur_offset = offset;
        let mut remaining: &mut [u8] = buf;

        while !remaining.is_empty() {
            let shard_idx = (cur_offset / self.chunk_size) as usize;
            if shard_idx >= self.shards.len() {
                break;
            }
            let intra = cur_offset % self.chunk_size;
            let max_in_shard = self.chunk_size - intra;
            let to_read = (remaining.len() as u64).min(max_in_shard) as usize;

            let n = self.shards[shard_idx].read(intra, &mut remaining[..to_read])?;
            if n == 0 {
                break;
            }
            total += n;
            cur_offset += n as u64;
            let tmp = std::mem::take(&mut remaining);
            remaining = &mut tmp[n..];
        }
        Ok(total)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let mut cur_offset = offset;
        let mut remaining = data;

        while !remaining.is_empty() {
            let shard_idx = (cur_offset / self.chunk_size) as usize;
            self.ensure_shard(shard_idx)?;
            let intra = cur_offset % self.chunk_size;
            let max_in_shard = self.chunk_size - intra;
            let to_write = (remaining.len() as u64).min(max_in_shard) as usize;

            self.shards[shard_idx].write(intra, &remaining[..to_write])?;
            cur_offset += to_write as u64;
            remaining = &remaining[to_write..];
        }
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        if self.shards.is_empty() {
            return Ok(0);
        }
        let last_idx = self.shards.len() - 1;
        let last_size = self.shards[last_idx].file_size()?;
        Ok((last_idx as u64) * self.chunk_size + last_size)
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        if size == 0 {
            // Zero every open shard; pop higher ones from our Vec but keep
            // the inner files alive (some backends — OPFS — can't recreate
            // file handles synchronously, so we don't delete on truncate).
            for shard in self.shards.iter_mut() {
                shard.truncate(0)?;
            }
            while self.shards.len() > 1 {
                self.shards.pop();
            }
            return Ok(());
        }

        let target_idx = ((size - 1) / self.chunk_size) as usize;
        let last_size = size - (target_idx as u64) * self.chunk_size;

        self.ensure_shard(target_idx)?;

        // Zero shards strictly above the target; pop them from Vec but keep
        // inner handles registered (see comment above).
        while self.shards.len() > target_idx + 1 {
            let i = self.shards.len() - 1;
            self.shards[i].truncate(0)?;
            self.shards.pop();
        }

        // Pad full shards below the target to chunk_size (handles grow).
        for i in 0..target_idx {
            let cur = self.shards[i].file_size()?;
            if cur != self.chunk_size {
                self.shards[i].truncate(self.chunk_size)?;
            }
        }

        self.shards[target_idx].truncate(last_size)?;
        Ok(())
    }

    fn sync(&mut self, flags: SyncFlags) -> Result<()> {
        for shard in self.shards.iter_mut() {
            shard.sync(flags)?;
        }
        Ok(())
    }

    fn lock(&mut self, lock_type: LockType) -> Result<()> {
        if let Some(first) = self.shards.first_mut() {
            first.lock(lock_type)?;
        }
        self.lock = lock_type;
        Ok(())
    }

    fn unlock(&mut self, lock_type: LockType) -> Result<()> {
        if let Some(first) = self.shards.first_mut() {
            first.unlock(lock_type)?;
        }
        self.lock = lock_type;
        Ok(())
    }
}

impl Drop for MultiplexFile {
    fn drop(&mut self) {
        if self.delete_on_close {
            // Release shard handles before deleting on backends that require it.
            let count = self.shards.len();
            self.shards.clear();
            for i in 0..count {
                let name = shard_name(&self.base_path, i, self.legacy);
                let _ = self.inner.delete(&name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryVfs;

    fn vfs(chunk: u64) -> MultiplexVfs {
        MultiplexVfs::with_chunk_size(Box::new(MemoryVfs::new()), chunk)
    }

    fn create_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            read_write: true,
            delete_on_close: false,
        }
    }

    #[test]
    fn small_write_fits_one_shard() {
        let v = vfs(1024);
        let mut f = v.open("a.db", create_flags()).unwrap();
        f.write(0, b"hello").unwrap();
        assert_eq!(f.file_size().unwrap(), 5);
        let mut buf = [0u8; 5];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn write_spans_shard_boundary() {
        let v = vfs(16);
        let mut f = v.open("a.db", create_flags()).unwrap();
        // 30 bytes → fills shard 0 (16) and shard 1 (14).
        let data: Vec<u8> = (0..30u8).collect();
        f.write(0, &data).unwrap();
        assert_eq!(f.file_size().unwrap(), 30);

        let mut buf = vec![0u8; 30];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 30);
        assert_eq!(buf, data);
    }

    #[test]
    fn read_spans_three_shards() {
        let v = vfs(8);
        let mut f = v.open("a.db", create_flags()).unwrap();
        let data: Vec<u8> = (0..24u8).collect();
        f.write(0, &data).unwrap();
        // Read across all three shards starting from offset 4.
        let mut buf = vec![0u8; 16];
        let n = f.read(4, &mut buf).unwrap();
        assert_eq!(n, 16);
        assert_eq!(buf, data[4..20]);
    }

    #[test]
    fn write_at_offset_creates_intermediate_shards() {
        let v = vfs(8);
        let mut f = v.open("a.db", create_flags()).unwrap();
        f.write(20, b"xy").unwrap();
        assert_eq!(f.file_size().unwrap(), 22);
        let mut buf = vec![0u8; 22];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 22);
        assert_eq!(&buf[..20], &[0u8; 20]);
        assert_eq!(&buf[20..], b"xy");
    }

    #[test]
    fn truncate_drops_higher_shards() {
        let v = vfs(8);
        let mut f = v.open("a.db", create_flags()).unwrap();
        let data: Vec<u8> = (0..40u8).collect();
        f.write(0, &data).unwrap();
        assert_eq!(f.file_size().unwrap(), 40);
        f.truncate(10).unwrap();
        assert_eq!(f.file_size().unwrap(), 10);
        let mut buf = vec![0u8; 10];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 10);
        assert_eq!(buf, data[..10]);
        // Reading past end returns 0.
        let mut tail = vec![0u8; 5];
        let n = f.read(20, &mut tail).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn truncate_to_zero_keeps_shard_zero() {
        let v = vfs(8);
        let mut f = v.open("a.db", create_flags()).unwrap();
        f.write(0, &[1u8; 30]).unwrap();
        f.truncate(0).unwrap();
        assert_eq!(f.file_size().unwrap(), 0);
    }

    #[test]
    fn truncate_grow_zero_fills() {
        let v = vfs(8);
        let mut f = v.open("a.db", create_flags()).unwrap();
        f.write(0, b"abc").unwrap();
        f.truncate(20).unwrap();
        assert_eq!(f.file_size().unwrap(), 20);
        let mut buf = vec![0u8; 20];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 20);
        assert_eq!(&buf[..3], b"abc");
        assert!(buf[3..].iter().all(|b| *b == 0));
    }

    #[test]
    fn reopen_finds_all_shards() {
        let inner = MemoryVfs::new();
        // Write through one MultiplexVfs.
        {
            let v = MultiplexVfs::with_chunk_size(Box::new(inner.clone()), 8);
            let mut f = v.open("a.db", create_flags()).unwrap();
            let data: Vec<u8> = (0..30u8).collect();
            f.write(0, &data).unwrap();
            assert_eq!(f.file_size().unwrap(), 30);
        }
        // Reopen with a fresh wrapper around the same inner VFS.
        let v2 = MultiplexVfs::with_chunk_size(Box::new(inner), 8);
        let f2 = v2.open("a.db", create_flags()).unwrap();
        assert_eq!(f2.file_size().unwrap(), 30);
        let mut buf = vec![0u8; 30];
        let n = f2.read(0, &mut buf).unwrap();
        assert_eq!(n, 30);
        assert_eq!(buf, (0..30u8).collect::<Vec<_>>());
    }

    #[test]
    fn legacy_single_file_opens_and_grows() {
        let inner = MemoryVfs::new();
        // Pre-create a legacy non-sharded file.
        {
            let mut f = inner.open("a.db", create_flags()).unwrap();
            f.write(0, b"legacy bytes").unwrap();
        }
        let v = MultiplexVfs::with_chunk_size(Box::new(inner.clone()), 16);
        let mut f = v.open("a.db", create_flags()).unwrap();
        assert_eq!(f.file_size().unwrap(), 12);
        // Grow past the boundary; this should create `a.db.001`.
        let payload: Vec<u8> = (0..40u8).collect();
        f.write(12, &payload).unwrap();
        assert_eq!(f.file_size().unwrap(), 52);
        // Verify the legacy bare path AND .001 both exist on the inner VFS.
        assert!(inner.exists("a.db").unwrap());
        assert!(inner.exists("a.db.001").unwrap());
        // Round-trip read.
        let mut buf = vec![0u8; 52];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 52);
        assert_eq!(&buf[..12], b"legacy bytes");
        assert_eq!(&buf[12..], payload.as_slice());
    }

    #[test]
    fn delete_removes_all_shards() {
        let inner = MemoryVfs::new();
        let v = MultiplexVfs::with_chunk_size(Box::new(inner.clone()), 8);
        {
            let mut f = v.open("a.db", create_flags()).unwrap();
            f.write(0, &(0..30u8).collect::<Vec<_>>()).unwrap();
        }
        assert!(inner.exists("a.db.000").unwrap());
        assert!(inner.exists("a.db.001").unwrap());
        assert!(inner.exists("a.db.002").unwrap());
        v.delete("a.db").unwrap();
        assert!(!inner.exists("a.db.000").unwrap());
        assert!(!inner.exists("a.db.001").unwrap());
        assert!(!inner.exists("a.db.002").unwrap());
    }

    #[test]
    fn open_without_create_errors_on_missing() {
        let v = vfs(16);
        let res = v.open(
            "missing.db",
            OpenFlags {
                create: false,
                read_write: true,
                delete_on_close: false,
            },
        );
        assert!(res.is_err());
    }

    #[test]
    fn exists_reflects_shard_presence() {
        let v = vfs(16);
        assert!(!v.exists("a.db").unwrap());
        v.open("a.db", create_flags()).unwrap();
        assert!(v.exists("a.db").unwrap());
    }

    #[test]
    fn pre_registered_empty_shards_are_ignored() {
        // Simulate the OPFS pattern: the wrapper has eagerly created empty
        // shard files to pre-register their handles. MultiplexVfs should
        // treat those headroom shards as past-end, not part of the logical
        // file's size.
        let inner = MemoryVfs::new();
        for i in 0..8 {
            inner
                .open(&format!("a.db.{:03}", i), create_flags())
                .unwrap();
        }
        // Write 5 bytes through shard 0.
        {
            let mut f = inner.open("a.db.000", create_flags()).unwrap();
            f.write(0, b"hello").unwrap();
        }
        let v = MultiplexVfs::with_chunk_size(Box::new(inner), 16);
        let f = v.open("a.db", create_flags()).unwrap();
        assert_eq!(f.file_size().unwrap(), 5);
        let mut buf = [0u8; 5];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn growth_into_pre_registered_shard_works() {
        // If higher-index shards are pre-registered as empty, growing into
        // them via writes should succeed without inner.open(create=true).
        let inner = MemoryVfs::new();
        for i in 0..4 {
            inner
                .open(&format!("a.db.{:03}", i), create_flags())
                .unwrap();
        }
        let v = MultiplexVfs::with_chunk_size(Box::new(inner.clone()), 8);
        let mut f = v.open("a.db", create_flags()).unwrap();
        // Write 24 bytes — fills shards 0, 1, 2.
        let data: Vec<u8> = (0..24u8).collect();
        f.write(0, &data).unwrap();
        assert_eq!(f.file_size().unwrap(), 24);
        let mut buf = vec![0u8; 24];
        let n = f.read(0, &mut buf).unwrap();
        assert_eq!(n, 24);
        assert_eq!(buf, data);
        // The pre-registered shard 3 was untouched, still empty in the inner VFS.
        let f3 = inner
            .open(
                "a.db.003",
                OpenFlags {
                    create: false,
                    read_write: false,
                    delete_on_close: false,
                },
            )
            .unwrap();
        assert_eq!(f3.file_size().unwrap(), 0);
    }
}
