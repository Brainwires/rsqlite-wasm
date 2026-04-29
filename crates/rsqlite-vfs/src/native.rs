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
