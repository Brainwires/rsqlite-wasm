use std::collections::{HashMap, HashSet};
use std::num::NonZero;

use lru::LruCache;
use rsqlite_vfs::{OpenFlags, SyncFlags, Vfs, VfsFile};

use crate::error::{Result, StorageError};
use crate::header::{DatabaseHeader, HEADER_SIZE};

const DEFAULT_CACHE_SIZE: usize = 256;

pub struct Pager {
    file: Box<dyn VfsFile>,
    pub header: DatabaseHeader,
    cache: LruCache<u32, Page>,
    dirty: HashSet<u32>,
    page_count: u32,
    in_transaction: bool,
    journal: HashMap<u32, Vec<u8>>,
    saved_page_count: u32,
    savepoints: Vec<SavepointState>,
}

struct SavepointState {
    name: String,
    page_snapshots: HashMap<u32, Vec<u8>>,
    page_count: u32,
}

#[derive(Clone)]
pub struct Page {
    pub number: u32,
    pub data: Vec<u8>,
}

impl Pager {
    pub fn open(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let flags = OpenFlags {
            create: false,
            read_write: true,
            delete_on_close: false,
        };
        let file = vfs.open(path, flags)?;
        let file_size = file.file_size()?;

        if file_size < HEADER_SIZE as u64 {
            return Err(StorageError::InvalidHeader(format!(
                "file too small: {file_size} bytes"
            )));
        }

        let mut header_buf = [0u8; HEADER_SIZE];
        file.read(0, &mut header_buf)?;
        let header = DatabaseHeader::parse(&header_buf)?;

        let page_count = if header.database_size > 0 {
            header.database_size
        } else {
            (file_size / header.page_size as u64) as u32
        };

        Ok(Self {
            file,
            header,
            cache: LruCache::new(NonZero::new(DEFAULT_CACHE_SIZE).unwrap()),
            dirty: HashSet::new(),
            page_count,
            in_transaction: false,
            journal: HashMap::new(),
            saved_page_count: page_count,
            savepoints: Vec::new(),
        })
    }

    pub fn create(vfs: &dyn Vfs, path: &str) -> Result<Self> {
        let flags = OpenFlags {
            create: true,
            read_write: true,
            delete_on_close: false,
        };
        let mut file = vfs.open(path, flags)?;
        let header = DatabaseHeader::new_default();

        let mut page1 = vec![0u8; header.page_size as usize];
        header.write(&mut page1);

        // Page 1 is a leaf table B-tree page for sqlite_schema.
        // B-tree header starts at offset 100 (after the database header).
        let btree_offset = HEADER_SIZE;
        page1[btree_offset] = 0x0D; // leaf table B-tree page
        let usable = header.usable_size() as u16;
        // First free block: 0 (none)
        page1[btree_offset + 1] = 0;
        page1[btree_offset + 2] = 0;
        // Number of cells: 0
        page1[btree_offset + 3] = 0;
        page1[btree_offset + 4] = 0;
        // Cell content offset (0 means 65536 for usable_size, otherwise points to start of content)
        let cell_content_start = usable;
        page1[btree_offset + 5] = (cell_content_start >> 8) as u8;
        page1[btree_offset + 6] = cell_content_start as u8;
        // Fragmented free bytes: 0
        page1[btree_offset + 7] = 0;

        file.write(0, &page1)?;
        file.sync(SyncFlags { full: true })?;

        Ok(Self {
            file,
            header,
            cache: LruCache::new(NonZero::new(DEFAULT_CACHE_SIZE).unwrap()),
            dirty: HashSet::new(),
            page_count: 1,
            in_transaction: false,
            journal: HashMap::new(),
            saved_page_count: 1,
            savepoints: Vec::new(),
        })
    }

    /// Read a page. Pages are 1-indexed (page 1 is the first page).
    pub fn get_page(&mut self, page_num: u32) -> Result<&Page> {
        if page_num < 1 || page_num > self.page_count {
            return Err(StorageError::PageOutOfRange(page_num, self.page_count));
        }

        if !self.cache.contains(&page_num) {
            let page = self.read_page_from_disk(page_num)?;
            self.cache.put(page_num, page);
        }

        Ok(self.cache.get(&page_num).unwrap())
    }

    /// Get a mutable reference to a page, marking it dirty.
    /// If in a transaction, saves the original page to the journal before first modification.
    pub fn get_page_mut(&mut self, page_num: u32) -> Result<&mut Page> {
        if page_num < 1 || page_num > self.page_count {
            return Err(StorageError::PageOutOfRange(page_num, self.page_count));
        }

        if !self.cache.contains(&page_num) {
            let page = self.read_page_from_disk(page_num)?;
            self.cache.put(page_num, page);
        }

        if self.in_transaction && !self.journal.contains_key(&page_num) {
            let original = self.cache.get(&page_num).unwrap().data.clone();
            self.journal.insert(page_num, original);
        }

        self.dirty.insert(page_num);
        Ok(self.cache.get_mut(&page_num).unwrap())
    }

    /// Allocate a new page at the end of the database.
    pub fn allocate_page(&mut self) -> Result<u32> {
        self.page_count += 1;
        let page_num = self.page_count;
        let page = Page {
            number: page_num,
            data: vec![0u8; self.header.page_size as usize],
        };
        self.cache.put(page_num, page);
        self.dirty.insert(page_num);
        Ok(page_num)
    }

    /// Flush all dirty pages to disk.
    pub fn flush(&mut self) -> Result<()> {
        let dirty_pages: Vec<u32> = self.dirty.drain().collect();
        for page_num in dirty_pages {
            if let Some(page) = self.cache.get(&page_num) {
                let offset = (page_num as u64 - 1) * self.header.page_size as u64;
                self.file.write(offset, &page.data)?;
            }
        }

        // Update header on page 1
        self.header.database_size = self.page_count;
        let mut header_buf = [0u8; HEADER_SIZE];
        self.header.write(&mut header_buf);
        self.file.write(0, &header_buf)?;

        self.file.sync(SyncFlags { full: false })?;
        Ok(())
    }

    pub fn page_size(&self) -> u32 {
        self.header.page_size
    }

    pub fn usable_size(&self) -> u32 {
        self.header.usable_size()
    }

    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    pub fn begin_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            return Err(StorageError::Other(
                "transaction already active".to_string(),
            ));
        }
        self.in_transaction = true;
        self.journal.clear();
        self.saved_page_count = self.page_count;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(StorageError::Other("no active transaction".to_string()));
        }
        self.flush()?;
        self.journal.clear();
        self.in_transaction = false;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if !self.in_transaction {
            return Err(StorageError::Other("no active transaction".to_string()));
        }

        for (page_num, original_data) in self.journal.drain() {
            if let Some(page) = self.cache.get_mut(&page_num) {
                page.data = original_data;
            }
        }

        self.page_count = self.saved_page_count;
        self.dirty.clear();
        self.in_transaction = false;
        self.savepoints.clear();
        Ok(())
    }

    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        if !self.in_transaction {
            self.begin_transaction()?;
        }
        let mut page_snapshots = HashMap::new();
        for &page_num in self.dirty.iter() {
            if let Some(page) = self.cache.peek(&page_num) {
                page_snapshots.insert(page_num, page.data.clone());
            }
        }
        self.savepoints.push(SavepointState {
            name: name.to_string(),
            page_snapshots,
            page_count: self.page_count,
        });
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self.savepoints.iter().rposition(|s| s.name.eq_ignore_ascii_case(name));
        match pos {
            Some(i) => { self.savepoints.truncate(i); Ok(()) }
            None => Err(StorageError::Other(format!("no such savepoint: {name}"))),
        }
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self.savepoints.iter().rposition(|s| s.name.eq_ignore_ascii_case(name));
        let pos = match pos {
            Some(i) => i,
            None => return Err(StorageError::Other(format!("no such savepoint: {name}"))),
        };

        let sp = &self.savepoints[pos];
        let snapshots = sp.page_snapshots.clone();
        let page_count_at_savepoint = sp.page_count;

        for (&page_num, snap_data) in &snapshots {
            if let Some(page) = self.cache.get_mut(&page_num) {
                page.data = snap_data.clone();
            }
        }

        for &page_num in self.dirty.clone().iter() {
            if page_num > page_count_at_savepoint {
                self.dirty.remove(&page_num);
            } else if !snapshots.contains_key(&page_num) {
                if let Some(original) = self.journal.get(&page_num) {
                    if let Some(page) = self.cache.get_mut(&page_num) {
                        page.data = original.clone();
                    }
                }
                self.dirty.remove(&page_num);
            }
        }

        self.page_count = page_count_at_savepoint;
        self.savepoints.truncate(pos + 1);
        Ok(())
    }

    fn read_page_from_disk(&self, page_num: u32) -> Result<Page> {
        let page_size = self.header.page_size as usize;
        let offset = (page_num as u64 - 1) * page_size as u64;
        let mut data = vec![0u8; page_size];
        self.file.read(offset, &mut data)?;
        Ok(Page {
            number: page_num,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsqlite_vfs::memory::MemoryVfs;

    #[test]
    fn create_and_reopen() {
        let vfs = MemoryVfs::new();
        {
            let mut pager = Pager::create(&vfs, "test.db").unwrap();
            assert_eq!(pager.page_count(), 1);
            assert_eq!(pager.page_size(), 4096);

            let page = pager.get_page(1).unwrap();
            assert_eq!(page.data.len(), 4096);
            // Check B-tree header at offset 100
            assert_eq!(page.data[100], 0x0D); // leaf table B-tree
        }

        {
            let mut pager = Pager::open(&vfs, "test.db").unwrap();
            assert_eq!(pager.page_count(), 1);
            assert_eq!(pager.page_size(), 4096);
            let page = pager.get_page(1).unwrap();
            assert_eq!(page.data[100], 0x0D);
        }
    }

    #[test]
    fn allocate_and_flush() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();

        let pg2 = pager.allocate_page().unwrap();
        assert_eq!(pg2, 2);
        assert_eq!(pager.page_count(), 2);

        {
            let page = pager.get_page_mut(2).unwrap();
            page.data[0] = 0xAB;
        }

        pager.flush().unwrap();

        // Reopen and verify
        let mut pager2 = Pager::open(&vfs, "test.db").unwrap();
        assert_eq!(pager2.page_count(), 2);
        let page = pager2.get_page(2).unwrap();
        assert_eq!(page.data[0], 0xAB);
    }

    #[test]
    fn page_out_of_range() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();
        assert!(pager.get_page(0).is_err());
        assert!(pager.get_page(2).is_err());
    }
}
