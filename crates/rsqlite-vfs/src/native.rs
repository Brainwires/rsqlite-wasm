use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

use crate::{LockType, OpenFlags, Result, SyncFlags, Vfs, VfsFile};

pub struct NativeVfs;

impl NativeVfs {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NativeVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for NativeVfs {
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
        let file = OpenOptions::new()
            .read(true)
            .write(flags.read_write)
            .create(flags.create)
            .open(path)?;
        Ok(Box::new(NativeFile {
            file: Mutex::new(file),
            path: path.to_string(),
            delete_on_close: flags.delete_on_close,
            lock: LockType::None,
        }))
    }

    fn delete(&self, path: &str) -> Result<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn exists(&self, path: &str) -> Result<bool> {
        Ok(std::path::Path::new(path).exists())
    }

    fn clone_box(&self) -> Box<dyn Vfs> {
        Box::new(NativeVfs)
    }
}

pub struct NativeFile {
    file: Mutex<File>,
    path: String,
    delete_on_close: bool,
    lock: LockType,
}

impl VfsFile for NativeFile {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        let n = file.read(buf)?;
        Ok(n)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        let file = self.file.lock().unwrap();
        Ok(file.metadata()?.len())
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.set_len(size)?;
        Ok(())
    }

    fn sync(&mut self, flags: SyncFlags) -> Result<()> {
        let file = self.file.lock().unwrap();
        if flags.full {
            file.sync_all()?;
        } else {
            file.sync_data()?;
        }
        Ok(())
    }

    fn lock(&mut self, lock_type: LockType) -> Result<()> {
        // TODO: implement file-level locking via flock/fcntl
        self.lock = lock_type;
        Ok(())
    }

    fn unlock(&mut self, lock_type: LockType) -> Result<()> {
        self.lock = lock_type;
        Ok(())
    }
}

impl Drop for NativeFile {
    fn drop(&mut self) {
        if self.delete_on_close {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Allocate a unique tempfile path under the system temp dir. We avoid
    /// pulling `tempfile` as a dep — a counter-suffixed path under
    /// std::env::temp_dir() is enough for these tests, and we clean up via
    /// delete_on_close (or explicit delete).
    fn unique_path(stem: &str) -> String {
        static N: AtomicUsize = AtomicUsize::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir()
            .join(format!(
                "rsqlite_vfs_test_{}_{}_{}.db",
                std::process::id(),
                stem,
                n
            ))
            .to_string_lossy()
            .into_owned()
    }

    fn create_flags() -> OpenFlags {
        OpenFlags {
            create: true,
            read_write: true,
            delete_on_close: true,
        }
    }

    #[test]
    fn open_creates_new_file() {
        let vfs = NativeVfs::new();
        let path = unique_path("create");
        assert!(!vfs.exists(&path).unwrap());

        let file = vfs.open(&path, create_flags()).unwrap();
        assert!(vfs.exists(&path).unwrap());
        assert_eq!(file.file_size().unwrap(), 0);
    }

    #[test]
    fn open_without_create_errors_for_missing() {
        let vfs = NativeVfs::new();
        let path = unique_path("missing");
        let res = vfs.open(
            &path,
            OpenFlags {
                create: false,
                read_write: true,
                delete_on_close: false,
            },
        );
        assert!(res.is_err());
    }

    #[test]
    fn write_then_read_roundtrip() {
        let vfs = NativeVfs::new();
        let path = unique_path("rw");
        let mut file = vfs.open(&path, create_flags()).unwrap();

        let data = b"hello, vfs world";
        file.write(0, data).unwrap();

        let mut buf = vec![0u8; data.len()];
        let n = file.read(0, &mut buf).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
    }

    #[test]
    fn write_at_offset_creates_sparse_region() {
        let vfs = NativeVfs::new();
        let path = unique_path("offset");
        let mut file = vfs.open(&path, create_flags()).unwrap();

        // Write 4 bytes at offset 100; file should grow to 104 bytes.
        file.write(100, b"data").unwrap();
        assert_eq!(file.file_size().unwrap(), 104);

        // The hole reads as zeros.
        let mut buf = vec![0xFFu8; 4];
        file.read(0, &mut buf).unwrap();
        assert_eq!(&buf, &[0u8; 4]);
    }

    #[test]
    fn truncate_shrinks_file() {
        let vfs = NativeVfs::new();
        let path = unique_path("truncate");
        let mut file = vfs.open(&path, create_flags()).unwrap();

        file.write(0, &[1u8; 100]).unwrap();
        assert_eq!(file.file_size().unwrap(), 100);

        file.truncate(40).unwrap();
        assert_eq!(file.file_size().unwrap(), 40);
    }

    #[test]
    fn sync_succeeds_with_both_flag_modes() {
        let vfs = NativeVfs::new();
        let path = unique_path("sync");
        let mut file = vfs.open(&path, create_flags()).unwrap();

        file.write(0, b"x").unwrap();
        file.sync(SyncFlags { full: false }).unwrap();
        file.sync(SyncFlags { full: true }).unwrap();
    }

    #[test]
    fn delete_removes_file() {
        let vfs = NativeVfs::new();
        let path = unique_path("delete");
        // Create with delete_on_close = false so we can explicitly delete.
        let file = vfs
            .open(
                &path,
                OpenFlags {
                    create: true,
                    read_write: true,
                    delete_on_close: false,
                },
            )
            .unwrap();
        drop(file);
        assert!(vfs.exists(&path).unwrap());

        vfs.delete(&path).unwrap();
        assert!(!vfs.exists(&path).unwrap());
    }

    #[test]
    fn delete_on_close_removes_after_drop() {
        let vfs = NativeVfs::new();
        let path = unique_path("doc");
        let file = vfs.open(&path, create_flags()).unwrap();
        assert!(vfs.exists(&path).unwrap());
        drop(file);
        assert!(!vfs.exists(&path).unwrap());
    }

    #[test]
    fn lock_unlock_set_state() {
        // Locking is currently a stub; verify the no-op path doesn't error.
        let vfs = NativeVfs::new();
        let path = unique_path("lock");
        let mut file = vfs.open(&path, create_flags()).unwrap();
        file.lock(LockType::Shared).unwrap();
        file.unlock(LockType::None).unwrap();
    }

    #[test]
    fn clone_box_makes_usable_vfs() {
        let vfs = NativeVfs::new();
        let cloned = vfs.clone_box();
        let path = unique_path("clone");
        let _file = cloned.open(&path, create_flags()).unwrap();
        assert!(cloned.exists(&path).unwrap());
    }
}
