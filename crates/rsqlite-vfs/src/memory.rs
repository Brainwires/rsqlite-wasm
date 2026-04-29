use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{LockType, OpenFlags, Result, SyncFlags, Vfs, VfsError, VfsFile};

type FileStore = Arc<Mutex<HashMap<String, Vec<u8>>>>;

#[derive(Clone)]
pub struct MemoryVfs {
    files: FileStore,
}

impl MemoryVfs {
    pub fn new() -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MemoryVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for MemoryVfs {
    fn open(&self, path: &str, flags: OpenFlags) -> Result<Box<dyn VfsFile>> {
        let mut store = self.files.lock().unwrap();
        if !store.contains_key(path) {
            if flags.create {
                store.insert(path.to_string(), Vec::new());
            } else {
                return Err(VfsError::NotFound(path.to_string()));
            }
        }
        Ok(Box::new(MemoryFile {
            path: path.to_string(),
            store: self.files.clone(),
            delete_on_close: flags.delete_on_close,
            lock: LockType::None,
        }))
    }

    fn delete(&self, path: &str) -> Result<()> {
        let mut store = self.files.lock().unwrap();
        store.remove(path);
        Ok(())
    }

    fn exists(&self, path: &str) -> Result<bool> {
        let store = self.files.lock().unwrap();
        Ok(store.contains_key(path))
    }
}

pub struct MemoryFile {
    path: String,
    store: FileStore,
    delete_on_close: bool,
    lock: LockType,
}

impl VfsFile for MemoryFile {
    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let store = self.store.lock().unwrap();
        let data = store
            .get(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;

        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0);
        }

        let available = data.len() - offset;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&data[offset..offset + to_read]);
        Ok(to_read)
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let mut store = self.store.lock().unwrap();
        let file_data = store
            .get_mut(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;

        let offset = offset as usize;
        let needed = offset + data.len();
        if needed > file_data.len() {
            file_data.resize(needed, 0);
        }
        file_data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn file_size(&self) -> Result<u64> {
        let store = self.store.lock().unwrap();
        let data = store
            .get(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;
        Ok(data.len() as u64)
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        let mut store = self.store.lock().unwrap();
        let data = store
            .get_mut(&self.path)
            .ok_or_else(|| VfsError::NotFound(self.path.clone()))?;
        data.resize(size as usize, 0);
        Ok(())
    }

    fn sync(&mut self, _flags: SyncFlags) -> Result<()> {
        Ok(())
    }

    fn lock(&mut self, lock_type: LockType) -> Result<()> {
        self.lock = lock_type;
        Ok(())
    }

    fn unlock(&mut self, lock_type: LockType) -> Result<()> {
        self.lock = lock_type;
        Ok(())
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        if self.delete_on_close {
            let mut store = self.store.lock().unwrap();
            store.remove(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_read_write() {
        let vfs = MemoryVfs::new();
        let mut file = vfs.open("test.db", OpenFlags::default()).unwrap();

        file.write(0, b"hello world").unwrap();
        assert_eq!(file.file_size().unwrap(), 11);

        let mut buf = [0u8; 5];
        let n = file.read(0, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        let n = file.read(6, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn read_past_end_returns_zero() {
        let vfs = MemoryVfs::new();
        let file = vfs.open("test.db", OpenFlags::default()).unwrap();

        let mut buf = [0u8; 10];
        let n = file.read(0, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn write_extends_file() {
        let vfs = MemoryVfs::new();
        let mut file = vfs.open("test.db", OpenFlags::default()).unwrap();

        file.write(10, b"data").unwrap();
        assert_eq!(file.file_size().unwrap(), 14);

        let mut buf = [0u8; 4];
        let n = file.read(0, &mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, &[0, 0, 0, 0]);
    }

    #[test]
    fn open_nonexistent_without_create_fails() {
        let vfs = MemoryVfs::new();
        let flags = OpenFlags {
            create: false,
            read_write: true,
            delete_on_close: false,
        };
        assert!(vfs.open("missing.db", flags).is_err());
    }

    #[test]
    fn delete_on_close() {
        let vfs = MemoryVfs::new();
        let flags = OpenFlags {
            create: true,
            read_write: true,
            delete_on_close: true,
        };
        {
            let mut file = vfs.open("temp.db", flags).unwrap();
            file.write(0, b"temp data").unwrap();
            assert!(vfs.exists("temp.db").unwrap());
        }
        assert!(!vfs.exists("temp.db").unwrap());
    }

    #[test]
    fn truncate() {
        let vfs = MemoryVfs::new();
        let mut file = vfs.open("test.db", OpenFlags::default()).unwrap();
        file.write(0, b"hello world").unwrap();
        file.truncate(5).unwrap();
        assert_eq!(file.file_size().unwrap(), 5);
    }

    #[test]
    fn exists_and_delete() {
        let vfs = MemoryVfs::new();
        vfs.open("test.db", OpenFlags::default()).unwrap();
        assert!(vfs.exists("test.db").unwrap());
        vfs.delete("test.db").unwrap();
        assert!(!vfs.exists("test.db").unwrap());
    }
}
