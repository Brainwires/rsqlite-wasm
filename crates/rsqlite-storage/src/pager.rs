use std::collections::{HashMap, HashSet};

use lru::LruCache;
use rsqlite_vfs::{OpenFlags, SyncFlags, Vfs, VfsFile};

use crate::error::{Result, StorageError};
use crate::header::{DatabaseHeader, HEADER_SIZE};

const DEFAULT_CACHE_SIZE: usize = 256;

pub struct Pager {
    file: Box<dyn VfsFile>,
    pub header: DatabaseHeader,
    cache: LruCache<u32, Page>,
    /// Soft upper bound on resident clean pages. The cache itself is unbounded
    /// so it never auto-evicts a dirty (unflushed) page; [`Pager::cache_insert`]
    /// enforces this budget by evicting only clean pages.
    cache_cap: usize,
    dirty: HashSet<u32>,
    page_count: u32,
    /// Pages that have been freed and are available for reuse, kept in memory
    /// and serialized to SQLite freelist trunk pages on flush. Reusing freed
    /// pages (instead of always growing the file) is what keeps a DELETE/UPDATE
    /// rebuild from leaving thousands of orphaned "never used" pages that
    /// `PRAGMA integrity_check` rejects.
    free_list: Vec<u32>,
    in_transaction: bool,
    journal: HashMap<u32, Vec<u8>>,
    saved_page_count: u32,
    saved_free_list: Vec<u32>,
    savepoints: Vec<SavepointState>,
}

struct SavepointState {
    name: String,
    page_snapshots: HashMap<u32, Vec<u8>>,
    page_count: u32,
    free_list: Vec<u32>,
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

        let mut pager = Self {
            file,
            header,
            cache: LruCache::unbounded(),
            cache_cap: DEFAULT_CACHE_SIZE,
            dirty: HashSet::new(),
            page_count,
            free_list: Vec::new(),
            in_transaction: false,
            journal: HashMap::new(),
            saved_page_count: page_count,
            saved_free_list: Vec::new(),
            savepoints: Vec::new(),
        };
        pager.load_free_list()?;
        pager.saved_free_list = pager.free_list.clone();
        Ok(pager)
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
            cache: LruCache::unbounded(),
            cache_cap: DEFAULT_CACHE_SIZE,
            dirty: HashSet::new(),
            page_count: 1,
            free_list: Vec::new(),
            in_transaction: false,
            journal: HashMap::new(),
            saved_page_count: 1,
            saved_free_list: Vec::new(),
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
            self.cache_insert(page_num, page);
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
            self.cache_insert(page_num, page);
        }

        if self.in_transaction && !self.journal.contains_key(&page_num) {
            let original = self.cache.get(&page_num).unwrap().data.clone();
            self.journal.insert(page_num, original);
        }

        self.dirty.insert(page_num);
        Ok(self.cache.get_mut(&page_num).unwrap())
    }

    /// Allocate a page, reusing a previously freed page when one is available
    /// and otherwise growing the file by one page. The returned page is zeroed
    /// and marked dirty, ready to be initialized by the caller.
    pub fn allocate_page(&mut self) -> Result<u32> {
        let page_size = self.header.page_size as usize;
        if let Some(page_num) = self.free_list.pop() {
            // Reuse a freed page. Zero it so stale contents never leak.
            let page = Page {
                number: page_num,
                data: vec![0u8; page_size],
            };
            self.cache_insert(page_num, page);
            self.dirty.insert(page_num);
            return Ok(page_num);
        }
        self.page_count += 1;
        let page_num = self.page_count;
        let page = Page {
            number: page_num,
            data: vec![0u8; page_size],
        };
        self.cache_insert(page_num, page);
        self.dirty.insert(page_num);
        Ok(page_num)
    }

    /// Return `page_num` to the freelist so a later [`Pager::allocate_page`]
    /// can reuse it. The page stays inside the file's page span; on flush the
    /// freelist is serialized into SQLite freelist trunk pages so the file
    /// remains valid (no unreferenced "never used" pages). Page 1 is never
    /// freed.
    pub fn free_page(&mut self, page_num: u32) {
        if page_num <= 1 || page_num > self.page_count {
            return;
        }
        if !self.free_list.contains(&page_num) {
            self.free_list.push(page_num);
        }
    }

    /// Insert a page into the cache while honoring the cache size budget.
    ///
    /// The `lru` cache would otherwise evict its least-recently-used entry
    /// unconditionally — including dirty pages whose contents have never been
    /// written to disk. Dropping such a page silently loses data: a later read
    /// re-fetches a zeroed (never-written) or stale page, surfacing as
    /// `Corrupt("invalid B-tree page type: 0x00")` during large DELETE/UPDATE
    /// rebuilds that allocate far more than `DEFAULT_CACHE_SIZE` pages without
    /// an intervening flush.
    ///
    /// To stay correct we PIN dirty pages: before inserting, we evict only
    /// least-recently-used *clean* pages down to the budget. Dirty pages remain
    /// resident until a flush/commit clears the dirty set, even if that pushes
    /// the cache temporarily over its soft capacity (bounded by the working set
    /// of a single statement). This avoids writing uncommitted pages to disk
    /// mid-transaction, keeping rollback semantics intact.
    fn cache_insert(&mut self, page_num: u32, page: Page) {
        let cap = self.cache_cap;
        // Reserve room for the page we are about to insert.
        while self.cache.len() >= cap {
            // Find the least-recently-used CLEAN page to evict. `iter()` yields
            // most-recently-used first, so the last clean match is the LRU one.
            let victim = self
                .cache
                .iter()
                .rev()
                .map(|(k, _)| *k)
                .find(|k| !self.dirty.contains(k));
            match victim {
                Some(v) => {
                    self.cache.pop(&v);
                }
                // Every resident page is dirty — pinning them all is the price
                // of correctness; grow past the soft cap.
                None => break,
            }
        }
        self.cache.put(page_num, page);
    }

    /// Flush all dirty pages to disk.
    pub fn flush(&mut self) -> Result<()> {
        // Serialize the freelist into trunk pages first; this dirties the trunk
        // pages and updates the header's freelist fields so the on-disk file is
        // a valid SQLite database (every page is either reachable or on the
        // freelist — no "never used" pages).
        self.write_free_list()?;

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

    /// Read the SQLite freelist (a chain of trunk pages starting at the header's
    /// `first_freelist_page`) into the in-memory `free_list`.
    fn load_free_list(&mut self) -> Result<()> {
        self.free_list.clear();
        let mut trunk = self.header.first_freelist_page;
        let mut guard = 0u32;
        let max_pages = self.page_count;
        while trunk != 0 {
            guard += 1;
            if guard > max_pages.saturating_add(1) {
                return Err(StorageError::Corrupt("freelist trunk cycle".into()));
            }
            // Trunk pages live within the file's page span.
            self.free_list.push(trunk);
            let data = self.get_page(trunk)?.data.clone();
            let next = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            let leaf_count = u32::from_be_bytes([data[4], data[5], data[6], data[7]]) as usize;
            for i in 0..leaf_count {
                let p = 8 + i * 4;
                let leaf = u32::from_be_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
                if leaf != 0 {
                    self.free_list.push(leaf);
                }
            }
            trunk = next;
        }
        Ok(())
    }

    /// Serialize the in-memory `free_list` into SQLite freelist trunk pages and
    /// update the header's freelist pointers. Some of the free pages are spent
    /// as trunk pages; the remainder are recorded as leaves.
    fn write_free_list(&mut self) -> Result<()> {
        if self.free_list.is_empty() {
            self.header.first_freelist_page = 0;
            self.header.freelist_count = 0;
            return Ok(());
        }

        let usable = self.usable_size() as usize;
        let leaves_per_trunk = (usable - 8) / 4;
        debug_assert!(leaves_per_trunk > 0);

        // Total pages on the freelist (trunks + leaves). SQLite counts both.
        let total = self.free_list.len() as u32;

        // Lay the free pages out as: a chain of trunk pages, each followed by up
        // to `leaves_per_trunk` leaf pages. Walk the list assigning roles.
        let pages = std::mem::take(&mut self.free_list);
        let mut idx = 0usize;
        let mut trunks: Vec<(u32, Vec<u32>)> = Vec::new();
        while idx < pages.len() {
            let trunk = pages[idx];
            idx += 1;
            let end = (idx + leaves_per_trunk).min(pages.len());
            let leaves = pages[idx..end].to_vec();
            idx = end;
            trunks.push((trunk, leaves));
        }

        let first_trunk = trunks[0].0;
        for i in 0..trunks.len() {
            let next = if i + 1 < trunks.len() {
                trunks[i + 1].0
            } else {
                0
            };
            let (trunk_page, leaves) = &trunks[i];
            let trunk_page = *trunk_page;
            let leaves = leaves.clone();
            let data = &mut self.get_page_mut(trunk_page)?.data;
            data.fill(0);
            data[0..4].copy_from_slice(&next.to_be_bytes());
            data[4..8].copy_from_slice(&(leaves.len() as u32).to_be_bytes());
            for (j, leaf) in leaves.iter().enumerate() {
                let p = 8 + j * 4;
                data[p..p + 4].copy_from_slice(&leaf.to_be_bytes());
            }
        }

        // Restore the in-memory list (flush must not consume it permanently:
        // freed pages remain reusable after a flush).
        self.free_list = pages;

        self.header.first_freelist_page = first_trunk;
        self.header.freelist_count = total;
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
        self.saved_free_list = self.free_list.clone();
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
        self.free_list = self.saved_free_list.clone();
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
            free_list: self.free_list.clone(),
        });
        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self
            .savepoints
            .iter()
            .rposition(|s| s.name.eq_ignore_ascii_case(name));
        match pos {
            Some(i) => {
                self.savepoints.truncate(i);
                Ok(())
            }
            None => Err(StorageError::Other(format!("no such savepoint: {name}"))),
        }
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        let pos = self
            .savepoints
            .iter()
            .rposition(|s| s.name.eq_ignore_ascii_case(name));
        let pos = match pos {
            Some(i) => i,
            None => return Err(StorageError::Other(format!("no such savepoint: {name}"))),
        };

        let sp = &self.savepoints[pos];
        let snapshots = sp.page_snapshots.clone();
        let page_count_at_savepoint = sp.page_count;
        let free_list_at_savepoint = sp.free_list.clone();

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
        self.free_list = free_list_at_savepoint;
        self.savepoints.truncate(pos + 1);
        Ok(())
    }

    pub fn replace_content(&mut self, data: &[u8]) -> Result<()> {
        self.file.truncate(0)?;
        self.file.write(0, data)?;
        self.file.sync(SyncFlags { full: true })?;

        let mut header_buf = [0u8; HEADER_SIZE];
        header_buf.copy_from_slice(&data[..HEADER_SIZE]);
        self.header = DatabaseHeader::parse(&header_buf)?;
        self.page_count = if self.header.database_size > 0 {
            self.header.database_size
        } else {
            (data.len() as u64 / self.header.page_size as u64) as u32
        };
        self.cache.clear();
        self.dirty.clear();
        self.journal.clear();
        self.saved_page_count = self.page_count;
        self.load_free_list()?;
        self.saved_free_list = self.free_list.clone();
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
