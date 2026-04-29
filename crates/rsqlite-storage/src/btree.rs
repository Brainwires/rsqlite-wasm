use crate::codec::{Record, Value};
use crate::error::{Result, StorageError};
use crate::header::HEADER_SIZE;
use crate::pager::Pager;
use crate::varint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D,
}

impl PageType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0A => Ok(Self::LeafIndex),
            0x0D => Ok(Self::LeafTable),
            _ => Err(StorageError::Corrupt(format!(
                "invalid B-tree page type: {v:#04x}"
            ))),
        }
    }

    pub fn is_leaf(self) -> bool {
        matches!(self, Self::LeafTable | Self::LeafIndex)
    }

    pub fn is_table(self) -> bool {
        matches!(self, Self::InteriorTable | Self::LeafTable)
    }
}

#[derive(Debug)]
pub struct BTreePageHeader {
    pub page_type: PageType,
    pub first_freeblock: u16,
    pub cell_count: u16,
    pub cell_content_offset: u32,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: Option<u32>,
}

impl BTreePageHeader {
    pub fn header_size(&self) -> usize {
        if self.page_type.is_leaf() {
            8
        } else {
            12
        }
    }
}

/// Parse the B-tree page header from raw page data.
/// `offset` is where the B-tree header starts (100 for page 1, 0 for others).
pub fn parse_btree_header(data: &[u8], offset: usize) -> Result<BTreePageHeader> {
    let page_type = PageType::from_u8(data[offset])?;
    let first_freeblock = u16::from_be_bytes([data[offset + 1], data[offset + 2]]);
    let cell_count = u16::from_be_bytes([data[offset + 3], data[offset + 4]]);
    let raw_cell_content = u16::from_be_bytes([data[offset + 5], data[offset + 6]]);
    let cell_content_offset = if raw_cell_content == 0 {
        65536u32
    } else {
        raw_cell_content as u32
    };
    let fragmented_free_bytes = data[offset + 7];

    let right_most_pointer = if !page_type.is_leaf() {
        Some(u32::from_be_bytes([
            data[offset + 8],
            data[offset + 9],
            data[offset + 10],
            data[offset + 11],
        ]))
    } else {
        None
    };

    Ok(BTreePageHeader {
        page_type,
        first_freeblock,
        cell_count,
        cell_content_offset,
        fragmented_free_bytes,
        right_most_pointer,
    })
}

/// Read the cell pointer array from a B-tree page.
fn read_cell_pointers(data: &[u8], offset: usize, count: u16) -> Vec<u16> {
    let mut pointers = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let pos = offset + i * 2;
        let ptr = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pointers.push(ptr);
    }
    pointers
}

/// Parsed cell from a leaf table B-tree page.
#[derive(Debug)]
pub struct TableLeafCell {
    pub rowid: i64,
    pub payload: Vec<u8>,
}

/// Parsed cell from an interior table B-tree page.
#[derive(Debug)]
pub struct TableInteriorCell {
    pub left_child_page: u32,
    pub rowid: i64,
}

fn parse_table_leaf_cell(data: &[u8], offset: usize, usable_size: u32) -> Result<TableLeafCell> {
    let (payload_size, n1) = varint::read_varint(&data[offset..]);
    let (rowid, n2) = varint::read_varint(&data[offset + n1..]);
    let payload_start = offset + n1 + n2;
    let payload_size = payload_size as usize;

    // Check for overflow
    let max_local = max_local_payload_leaf(usable_size) as usize;
    let local_size = if payload_size <= max_local {
        payload_size
    } else {
        let min_local = min_local_payload(usable_size) as usize;
        let mut local = min_local + (payload_size - min_local) % (usable_size as usize - 4);
        if local > max_local {
            local = min_local;
        }
        local
    };

    if local_size == payload_size {
        // No overflow
        let payload = data[payload_start..payload_start + payload_size].to_vec();
        Ok(TableLeafCell {
            rowid: rowid as i64,
            payload,
        })
    } else {
        // Has overflow — for now, just read the local part
        // TODO: follow overflow page chain
        let payload = data[payload_start..payload_start + local_size].to_vec();
        Ok(TableLeafCell {
            rowid: rowid as i64,
            payload,
        })
    }
}

fn parse_table_interior_cell(data: &[u8], offset: usize) -> TableInteriorCell {
    let left_child = u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]);
    let (rowid, _) = varint::read_varint(&data[offset + 4..]);
    TableInteriorCell {
        left_child_page: left_child,
        rowid: rowid as i64,
    }
}

fn max_local_payload_leaf(usable_size: u32) -> u32 {
    usable_size - 35
}

fn min_local_payload(usable_size: u32) -> u32 {
    (usable_size - 12) * 32 / 255 - 23
}

/// A cursor for traversing a table B-tree (keyed by rowid).
pub struct BTreeCursor<'a> {
    pager: &'a mut Pager,
    root_page: u32,
    /// Stack of (page_number, child_index) representing current position.
    /// For interior pages, child_index tracks which child subtree we've visited:
    ///   0..cell_count-1 = left child of cell[i], cell_count = rightmost pointer.
    /// For leaf pages, child_index is the cell index.
    stack: Vec<(u32, usize)>,
    state: CursorState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    Invalid,
    Valid,
    AtEnd,
}

/// A row returned by the cursor.
#[derive(Debug)]
pub struct CursorRow {
    pub rowid: i64,
    pub record: Record,
}

impl<'a> BTreeCursor<'a> {
    pub fn new(pager: &'a mut Pager, root_page: u32) -> Self {
        Self {
            pager,
            root_page,
            stack: Vec::new(),
            state: CursorState::Invalid,
        }
    }

    /// Move to the first row in the table.
    pub fn first(&mut self) -> Result<bool> {
        self.stack.clear();
        self.state = CursorState::Invalid;
        self.descend_to_leftmost(self.root_page)?;
        self.check_valid()
    }

    /// Move to the next row. Returns false if at end.
    pub fn next(&mut self) -> Result<bool> {
        if self.state != CursorState::Valid {
            return Ok(false);
        }

        // Advance the leaf cell index
        if let Some(entry) = self.stack.last_mut() {
            entry.1 += 1;
        }

        loop {
            let (page_num, idx) = match self.stack.last().copied() {
                Some(entry) => entry,
                None => {
                    self.state = CursorState::AtEnd;
                    return Ok(false);
                }
            };

            let page = self.pager.get_page(page_num)?.data.clone();
            let offset = btree_header_offset(page_num);
            let header = parse_btree_header(&page, offset)?;

            if header.page_type.is_leaf() {
                if idx < header.cell_count as usize {
                    self.state = CursorState::Valid;
                    return Ok(true);
                }
                // Leaf exhausted, pop and advance parent
                self.stack.pop();
                if let Some(entry) = self.stack.last_mut() {
                    entry.1 += 1;
                }
            } else {
                // Interior page: idx is the child we need to visit next.
                // Children are: cell[0].left, cell[1].left, ..., cell[N-1].left, rightmost
                let total_children = header.cell_count as usize + 1;

                if idx < total_children {
                    let child_page = self.get_child_page(&page, offset, &header, idx)?;
                    self.descend_to_leftmost(child_page)?;
                    return self.check_valid();
                }
                // All children visited, pop and advance parent
                self.stack.pop();
                if let Some(entry) = self.stack.last_mut() {
                    entry.1 += 1;
                }
            }
        }
    }

    /// Get the current row (rowid + decoded record).
    pub fn current(&mut self) -> Result<CursorRow> {
        if self.state != CursorState::Valid {
            return Err(StorageError::Other("cursor not positioned".to_string()));
        }

        let &(page_num, cell_idx) = self.stack.last().unwrap();
        let page = self.pager.get_page(page_num)?.data.clone();
        let offset = btree_header_offset(page_num);
        let header = parse_btree_header(&page, offset)?;
        let usable = self.pager.usable_size();

        let pointers =
            read_cell_pointers(&page, offset + header.header_size(), header.cell_count);
        let cell_offset = pointers[cell_idx] as usize;
        let cell = parse_table_leaf_cell(&page, cell_offset, usable)?;

        let record = Record::decode(&cell.payload)?;
        Ok(CursorRow {
            rowid: cell.rowid,
            record,
        })
    }

    /// Collect all rows from the cursor (resets to first).
    pub fn collect_all(&mut self) -> Result<Vec<CursorRow>> {
        let mut rows = Vec::new();
        let mut has_row = self.first()?;
        while has_row {
            rows.push(self.current()?);
            has_row = self.next()?;
        }
        Ok(rows)
    }

    fn get_child_page(
        &mut self,
        page_data: &[u8],
        offset: usize,
        header: &BTreePageHeader,
        child_idx: usize,
    ) -> Result<u32> {
        let cell_count = header.cell_count as usize;
        if child_idx < cell_count {
            let pointers =
                read_cell_pointers(page_data, offset + header.header_size(), header.cell_count);
            let cell_offset = pointers[child_idx] as usize;
            let cell = parse_table_interior_cell(page_data, cell_offset);
            Ok(cell.left_child_page)
        } else {
            header.right_most_pointer.ok_or_else(|| {
                StorageError::Corrupt("interior page missing rightmost pointer".to_string())
            })
        }
    }

    fn descend_to_leftmost(&mut self, page_num: u32) -> Result<()> {
        let mut current_page = page_num;
        loop {
            let page_data = self.pager.get_page(current_page)?.data.clone();
            let offset = btree_header_offset(current_page);
            let header = parse_btree_header(&page_data, offset)?;

            if header.page_type.is_leaf() {
                self.stack.push((current_page, 0));
                return Ok(());
            }

            // Interior page: push with child_idx=0 (first child = left pointer of cell[0])
            self.stack.push((current_page, 0));

            let child = self.get_child_page(&page_data, offset, &header, 0)?;
            current_page = child;
        }
    }

    fn check_valid(&mut self) -> Result<bool> {
        if let Some(&(page_num, cell_idx)) = self.stack.last() {
            let page_data = self.pager.get_page(page_num)?.data.clone();
            let offset = btree_header_offset(page_num);
            let header = parse_btree_header(&page_data, offset)?;
            if header.page_type.is_leaf() && cell_idx < header.cell_count as usize {
                self.state = CursorState::Valid;
                return Ok(true);
            }
        }
        self.state = CursorState::AtEnd;
        Ok(false)
    }
}

fn btree_header_offset(page_num: u32) -> usize {
    if page_num == 1 {
        HEADER_SIZE
    } else {
        0
    }
}

/// Read all rows from the sqlite_schema table (page 1).
pub fn read_schema(pager: &mut Pager) -> Result<Vec<SchemaEntry>> {
    let mut cursor = BTreeCursor::new(pager, 1);
    let rows = cursor.collect_all()?;

    let mut entries = Vec::new();
    for row in rows {
        if row.record.values.len() < 5 {
            continue;
        }
        let entry_type = match &row.record.values[0] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let name = match &row.record.values[1] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let tbl_name = match &row.record.values[2] {
            Value::Text(s) => s.clone(),
            _ => continue,
        };
        let rootpage = match &row.record.values[3] {
            Value::Integer(n) => *n as u32,
            _ => 0,
        };
        let sql = match &row.record.values[4] {
            Value::Text(s) => Some(s.clone()),
            Value::Null => None,
            _ => None,
        };

        entries.push(SchemaEntry {
            entry_type,
            name,
            tbl_name,
            rootpage,
            sql,
        });
    }

    Ok(entries)
}

#[derive(Debug, Clone)]
pub struct SchemaEntry {
    pub entry_type: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u32,
    pub sql: Option<String>,
}

// ── Write operations ──

/// Build a leaf table cell (payload_size varint + rowid varint + payload).
fn build_table_leaf_cell(rowid: i64, payload: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(payload.len() + 18);
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(payload.len() as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    let n = varint::write_varint(rowid as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell.extend_from_slice(payload);
    cell
}

/// Build an interior table cell (4-byte left child + rowid varint).
fn build_table_interior_cell(left_child: u32, rowid: i64) -> Vec<u8> {
    let mut cell = Vec::with_capacity(13);
    cell.extend_from_slice(&left_child.to_be_bytes());
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(rowid as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell
}

pub fn write_btree_header(data: &mut [u8], offset: usize, header: &BTreePageHeader) {
    data[offset] = header.page_type as u8;
    data[offset + 1..offset + 3].copy_from_slice(&header.first_freeblock.to_be_bytes());
    data[offset + 3..offset + 5].copy_from_slice(&header.cell_count.to_be_bytes());
    let raw_offset = if header.cell_content_offset >= 65536 {
        0u16
    } else {
        header.cell_content_offset as u16
    };
    data[offset + 5..offset + 7].copy_from_slice(&raw_offset.to_be_bytes());
    data[offset + 7] = header.fragmented_free_bytes;
    if let Some(right) = header.right_most_pointer {
        data[offset + 8..offset + 12].copy_from_slice(&right.to_be_bytes());
    }
}

fn write_cell_pointers(data: &mut [u8], offset: usize, pointers: &[u16]) {
    for (i, ptr) in pointers.iter().enumerate() {
        let pos = offset + i * 2;
        data[pos..pos + 2].copy_from_slice(&ptr.to_be_bytes());
    }
}

/// Initialize a page as an empty leaf table B-tree.
fn init_leaf_page(data: &mut [u8], page_num: u32) {
    let offset = btree_header_offset(page_num);
    let usable = data.len() as u32;
    data[offset] = PageType::LeafTable as u8;
    data[offset + 1] = 0;
    data[offset + 2] = 0; // first freeblock
    data[offset + 3] = 0;
    data[offset + 4] = 0; // cell count
    let content_offset = usable as u16;
    data[offset + 5] = (content_offset >> 8) as u8;
    data[offset + 6] = content_offset as u8;
    data[offset + 7] = 0; // fragmented free bytes
}

/// Initialize a page as an empty interior table B-tree.
fn init_interior_page(data: &mut [u8], page_num: u32, right_child: u32) {
    let offset = btree_header_offset(page_num);
    let usable = data.len() as u32;
    data[offset] = PageType::InteriorTable as u8;
    data[offset + 1] = 0;
    data[offset + 2] = 0;
    data[offset + 3] = 0;
    data[offset + 4] = 0;
    let content_offset = usable as u16;
    data[offset + 5] = (content_offset >> 8) as u8;
    data[offset + 6] = content_offset as u8;
    data[offset + 7] = 0;
    data[offset + 8..offset + 12].copy_from_slice(&right_child.to_be_bytes());
}

/// Insert a row into a table B-tree. Returns the new root page number
/// (may change if the root splits).
pub fn btree_insert(pager: &mut Pager, root_page: u32, rowid: i64, record: &Record) -> Result<u32> {
    let payload = record.encode();
    let cell = build_table_leaf_cell(rowid, &payload);
    insert_into_page(pager, root_page, rowid, &cell)
}

/// Recursive insert: finds the correct leaf, inserts, splits if needed.
/// Returns the new root page (same unless root split).
fn insert_into_page(pager: &mut Pager, page_num: u32, rowid: i64, cell: &[u8]) -> Result<u32> {
    let page_data = pager.get_page(page_num)?.data.clone();
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(&page_data, offset)?;

    if header.page_type.is_leaf() {
        // Insert into this leaf page
        let result = try_insert_cell_into_leaf(pager, page_num, rowid, cell)?;
        match result {
            InsertResult::Ok => Ok(page_num),
            InsertResult::Split {
                new_page,
                median_rowid,
            } => {
                // Leaf was the root — create a new root
                let new_root = pager.allocate_page()?;
                {
                    let root_data = &mut pager.get_page_mut(new_root)?.data;
                    init_interior_page(root_data, new_root, new_page);
                }
                // Insert the median cell pointing to the old page
                let interior_cell = build_table_interior_cell(page_num, median_rowid);
                insert_cell_into_interior(pager, new_root, &interior_cell)?;
                Ok(new_root)
            }
        }
    } else {
        // Interior page: find the right child to descend into
        let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);
        let mut child_page = header.right_most_pointer.unwrap();

        for i in 0..header.cell_count as usize {
            let cell_offset = pointers[i] as usize;
            let ic = parse_table_interior_cell(&page_data, cell_offset);
            if rowid <= ic.rowid {
                child_page = ic.left_child_page;
                break;
            }
        }

        let child_data = pager.get_page(child_page)?.data.clone();
        let child_offset = btree_header_offset(child_page);
        let child_header = parse_btree_header(&child_data, child_offset)?;

        if child_header.page_type.is_leaf() {
            let result = try_insert_cell_into_leaf(pager, child_page, rowid, cell)?;
            match result {
                InsertResult::Ok => Ok(page_num),
                InsertResult::Split {
                    new_page,
                    median_rowid,
                } => {
                    let interior_cell = build_table_interior_cell(child_page, median_rowid);
                    // Try to insert into current interior page
                    let int_result = try_insert_cell_into_interior(pager, page_num, &interior_cell, new_page)?;
                    match int_result {
                        InsertResult::Ok => Ok(page_num),
                        InsertResult::Split {
                            new_page: new_int_page,
                            median_rowid: med,
                        } => {
                            // Interior split — create new root
                            let new_root = pager.allocate_page()?;
                            {
                                let root_data = &mut pager.get_page_mut(new_root)?.data;
                                init_interior_page(root_data, new_root, new_int_page);
                            }
                            let root_cell = build_table_interior_cell(page_num, med);
                            insert_cell_into_interior(pager, new_root, &root_cell)?;
                            Ok(new_root)
                        }
                    }
                }
            }
        } else {
            // Recursively insert into interior child
            let new_child_root = insert_into_page(pager, child_page, rowid, cell)?;
            if new_child_root != child_page {
                // Child root changed — need to update the pointer in the current page
                update_child_pointer(pager, page_num, child_page, new_child_root)?;
            }
            Ok(page_num)
        }
    }
}

enum InsertResult {
    Ok,
    Split { new_page: u32, median_rowid: i64 },
}

fn try_insert_cell_into_leaf(
    pager: &mut Pager,
    page_num: u32,
    rowid: i64,
    cell: &[u8],
) -> Result<InsertResult> {
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(data, offset)?;

    let ptr_area_start = offset + header.header_size();
    let ptr_area_end = ptr_area_start + header.cell_count as usize * 2;
    let content_start = header.cell_content_offset as usize;

    // Space needed: 2 bytes for pointer + cell bytes
    let space_needed = 2 + cell.len();
    let free_space = content_start - ptr_area_end;

    if space_needed <= free_space {
        // Enough room — insert in sorted position
        let pointers = read_cell_pointers(data, ptr_area_start, header.cell_count);

        // Find insert position (keep sorted by rowid)
        let mut insert_pos = pointers.len();
        for (i, &ptr) in pointers.iter().enumerate() {
            let (_, n1) = varint::read_varint(&data[ptr as usize..]);
            let (existing_rowid, _) = varint::read_varint(&data[ptr as usize + n1..]);
            if rowid <= existing_rowid as i64 {
                insert_pos = i;
                break;
            }
        }

        // Write cell content at the bottom of the content area
        let new_content_start = content_start - cell.len();
        data[new_content_start..new_content_start + cell.len()].copy_from_slice(cell);

        // Shift existing pointers to make room
        let mut new_pointers = Vec::with_capacity(pointers.len() + 1);
        for (i, &ptr) in pointers.iter().enumerate() {
            if i == insert_pos {
                new_pointers.push(new_content_start as u16);
            }
            new_pointers.push(ptr);
        }
        if insert_pos == pointers.len() {
            new_pointers.push(new_content_start as u16);
        }

        // Update header
        let new_cell_count = header.cell_count + 1;
        data[offset + 3..offset + 5].copy_from_slice(&new_cell_count.to_be_bytes());
        let content_u16 = new_content_start as u16;
        data[offset + 5..offset + 7].copy_from_slice(&content_u16.to_be_bytes());

        // Write updated pointer array
        write_cell_pointers(data, ptr_area_start, &new_pointers);

        Ok(InsertResult::Ok)
    } else {
        // Need to split
        split_leaf(pager, page_num, rowid, cell)
    }
}

fn split_leaf(
    pager: &mut Pager,
    page_num: u32,
    new_rowid: i64,
    new_cell: &[u8],
) -> Result<InsertResult> {
    let usable = pager.usable_size();

    // Collect all existing cells + the new one
    let page_data = pager.get_page(page_num)?.data.clone();
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(&page_data, offset)?;
    let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);

    let mut cells: Vec<(i64, Vec<u8>)> = Vec::new();
    for &ptr in &pointers {
        let cell_start = ptr as usize;
        let c = parse_table_leaf_cell(&page_data, cell_start, usable)?;
        let raw_cell = build_table_leaf_cell(c.rowid, &c.payload);
        cells.push((c.rowid, raw_cell));
    }
    cells.push((new_rowid, new_cell.to_vec()));
    cells.sort_by_key(|(rowid, _)| *rowid);

    // Split roughly in half
    let mid = cells.len() / 2;
    let left_cells = &cells[..mid];
    let right_cells = &cells[mid..];
    let median_rowid = left_cells.last().map(|(r, _)| *r).unwrap_or(0);

    // Rewrite left page (page_num) with left_cells
    rewrite_leaf_page(pager, page_num, left_cells)?;

    // Create new right page
    let new_page = pager.allocate_page()?;
    {
        let data = &mut pager.get_page_mut(new_page)?.data;
        init_leaf_page(data, new_page);
    }
    rewrite_leaf_page(pager, new_page, right_cells)?;

    Ok(InsertResult::Split {
        new_page,
        median_rowid,
    })
}

fn rewrite_leaf_page(pager: &mut Pager, page_num: u32, cells: &[(i64, Vec<u8>)]) -> Result<()> {
    let page_size = pager.page_size() as usize;
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);

    // Clear page (preserve DB header if page 1)
    let clear_start = offset;
    data[clear_start..page_size].fill(0);

    init_leaf_page(data, page_num);

    let ptr_area_start = offset + 8; // leaf header is 8 bytes
    let mut content_end = page_size;
    let mut pointers = Vec::with_capacity(cells.len());

    for (_, cell_data) in cells {
        content_end -= cell_data.len();
        data[content_end..content_end + cell_data.len()].copy_from_slice(cell_data);
        pointers.push(content_end as u16);
    }

    // Write header
    let cell_count = cells.len() as u16;
    data[offset + 3..offset + 5].copy_from_slice(&cell_count.to_be_bytes());
    let content_u16 = content_end as u16;
    data[offset + 5..offset + 7].copy_from_slice(&content_u16.to_be_bytes());

    // Write pointers
    write_cell_pointers(data, ptr_area_start, &pointers);

    Ok(())
}

fn insert_cell_into_interior(pager: &mut Pager, page_num: u32, cell: &[u8]) -> Result<()> {
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(data, offset)?;

    let ptr_area_start = offset + header.header_size();
    let content_start = header.cell_content_offset as usize;

    let new_content_start = content_start - cell.len();
    data[new_content_start..new_content_start + cell.len()].copy_from_slice(cell);

    // Add pointer at the end of the pointer array
    let ptr_pos = ptr_area_start + header.cell_count as usize * 2;
    let ptr_val = new_content_start as u16;
    data[ptr_pos..ptr_pos + 2].copy_from_slice(&ptr_val.to_be_bytes());

    // Update header
    let new_count = header.cell_count + 1;
    data[offset + 3..offset + 5].copy_from_slice(&new_count.to_be_bytes());
    let cu16 = new_content_start as u16;
    data[offset + 5..offset + 7].copy_from_slice(&cu16.to_be_bytes());

    Ok(())
}

fn try_insert_cell_into_interior(
    pager: &mut Pager,
    page_num: u32,
    cell: &[u8],
    new_right_child: u32,
) -> Result<InsertResult> {
    let page_size = pager.page_size() as usize;
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(data, offset)?;

    let ptr_area_start = offset + header.header_size();
    let ptr_area_end = ptr_area_start + header.cell_count as usize * 2;
    let content_start = header.cell_content_offset as usize;

    let space_needed = 2 + cell.len();
    let free_space = content_start - ptr_area_end;

    if space_needed <= free_space {
        // Parse the cell to get its rowid for sorted insertion
        let ic = parse_table_interior_cell(cell, 0);
        let pointers = read_cell_pointers(data, ptr_area_start, header.cell_count);

        let mut insert_pos = pointers.len();
        for (i, &ptr) in pointers.iter().enumerate() {
            let existing = parse_table_interior_cell(data, ptr as usize);
            if ic.rowid <= existing.rowid {
                insert_pos = i;
                break;
            }
        }

        // Write cell content
        let new_content_start = content_start - cell.len();
        data[new_content_start..new_content_start + cell.len()].copy_from_slice(cell);

        // Update right_most_pointer: the new_right_child becomes the right pointer
        // of the cell we're inserting. We need to update the right_most_pointer
        // if we inserted at the end.
        // The right child of cell[i] in a table interior page is cell[i+1].left_child
        // or right_most_pointer if it's the last cell.

        // Rebuild pointers with insertion
        let mut new_pointers = Vec::with_capacity(pointers.len() + 1);
        for (i, &ptr) in pointers.iter().enumerate() {
            if i == insert_pos {
                new_pointers.push(new_content_start as u16);
            }
            new_pointers.push(ptr);
        }
        if insert_pos == pointers.len() {
            new_pointers.push(new_content_start as u16);
        }

        // The new cell's left_child points to the old page.
        // The right_most_pointer should be updated to new_right_child
        // if the new cell was inserted at the end. Otherwise, we need to
        // update it to the old right_most_pointer.
        // Actually, the right child of the inserted cell is new_right_child.
        // If inserted at end: right_most = new_right_child
        // If inserted in middle: the cell after it takes the new_right_child as left.
        // For simplicity, we always set right_most to new_right_child when inserting at end.
        if insert_pos == pointers.len() {
            data[offset + 8..offset + 12].copy_from_slice(&new_right_child.to_be_bytes());
        } else {
            // Need to update the next cell's left_child to new_right_child
            let next_ptr = new_pointers[insert_pos + 1];
            data[next_ptr as usize..next_ptr as usize + 4]
                .copy_from_slice(&new_right_child.to_be_bytes());
        }

        let new_count = header.cell_count + 1;
        data[offset + 3..offset + 5].copy_from_slice(&new_count.to_be_bytes());
        let cu16 = new_content_start as u16;
        data[offset + 5..offset + 7].copy_from_slice(&cu16.to_be_bytes());
        write_cell_pointers(data, ptr_area_start, &new_pointers);

        Ok(InsertResult::Ok)
    } else {
        // Split interior page — collect all cells + new one, split
        let pointers = read_cell_pointers(data, ptr_area_start, header.cell_count);
        let old_right = header.right_most_pointer.unwrap();

        let mut all_cells: Vec<(i64, Vec<u8>, u32)> = Vec::new();
        for (i, &ptr) in pointers.iter().enumerate() {
            let ic = parse_table_interior_cell(data, ptr as usize);
            let raw = build_table_interior_cell(ic.left_child_page, ic.rowid);
            // Right child of cell[i] is cell[i+1].left_child or right_most_pointer
            let right = if i + 1 < pointers.len() {
                parse_table_interior_cell(data, pointers[i + 1] as usize).left_child_page
            } else {
                old_right
            };
            all_cells.push((ic.rowid, raw, right));
        }

        // Insert the new cell
        let new_ic = parse_table_interior_cell(cell, 0);
        let new_raw = cell.to_vec();
        all_cells.push((new_ic.rowid, new_raw, new_right_child));
        all_cells.sort_by_key(|(rowid, _, _)| *rowid);

        let mid = all_cells.len() / 2;
        let median_rowid = all_cells[mid].0;

        // Left: cells[0..mid], right_most = cells[mid].left_child
        // Promote: cells[mid] goes to parent
        // Right: cells[mid+1..], right_most = last cell's right_child

        let left_cells = &all_cells[..mid];
        let promoted = &all_cells[mid];
        let right_cells = &all_cells[mid + 1..];

        // Rewrite current page as left
        {
            let page = pager.get_page_mut(page_num)?;
            let data = &mut page.data;
            let off = btree_header_offset(page_num);
            data[off..page_size].fill(0);
            // The right child of the left page is the left_child of the promoted cell
            init_interior_page(data, page_num, parse_table_interior_cell(&promoted.1, 0).left_child_page);

            let mut content_end = page_size;
            let ptr_start = off + 12;
            let mut ptrs = Vec::new();
            for (_, cell_data, _) in left_cells {
                content_end -= cell_data.len();
                data[content_end..content_end + cell_data.len()].copy_from_slice(cell_data);
                ptrs.push(content_end as u16);
            }
            let count = left_cells.len() as u16;
            data[off + 3..off + 5].copy_from_slice(&count.to_be_bytes());
            let cu16 = content_end as u16;
            data[off + 5..off + 7].copy_from_slice(&cu16.to_be_bytes());
            write_cell_pointers(data, ptr_start, &ptrs);
        }

        // Create right page
        let new_page = pager.allocate_page()?;
        {
            let right_right_child = if right_cells.is_empty() {
                promoted.2
            } else {
                right_cells.last().unwrap().2
            };
            let page = pager.get_page_mut(new_page)?;
            let data = &mut page.data;
            init_interior_page(data, new_page, right_right_child);

            let off = btree_header_offset(new_page);
            let ptr_start = off + 12;
            let mut content_end = page_size;
            let mut ptrs = Vec::new();
            for (_, cell_data, _) in right_cells {
                content_end -= cell_data.len();
                data[content_end..content_end + cell_data.len()].copy_from_slice(cell_data);
                ptrs.push(content_end as u16);
            }
            let count = right_cells.len() as u16;
            data[off + 3..off + 5].copy_from_slice(&count.to_be_bytes());
            let cu16 = content_end as u16;
            data[off + 5..off + 7].copy_from_slice(&cu16.to_be_bytes());
            write_cell_pointers(data, ptr_start, &ptrs);
        }

        Ok(InsertResult::Split {
            new_page,
            median_rowid,
        })
    }
}

fn update_child_pointer(
    pager: &mut Pager,
    page_num: u32,
    old_child: u32,
    new_child: u32,
) -> Result<()> {
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(data, offset)?;

    // Check right_most_pointer
    if header.right_most_pointer == Some(old_child) {
        data[offset + 8..offset + 12].copy_from_slice(&new_child.to_be_bytes());
        return Ok(());
    }

    // Check cell left_child pointers
    let ptr_area_start = offset + header.header_size();
    let pointers = read_cell_pointers(data, ptr_area_start, header.cell_count);
    for &ptr in &pointers {
        let cell_start = ptr as usize;
        let left = u32::from_be_bytes([
            data[cell_start],
            data[cell_start + 1],
            data[cell_start + 2],
            data[cell_start + 3],
        ]);
        if left == old_child {
            data[cell_start..cell_start + 4].copy_from_slice(&new_child.to_be_bytes());
            return Ok(());
        }
    }

    Ok(())
}

/// Get the maximum rowid in a table B-tree, or 0 if empty.
pub fn btree_max_rowid(pager: &mut Pager, root_page: u32) -> Result<i64> {
    let page_data = pager.get_page(root_page)?.data.clone();
    let offset = btree_header_offset(root_page);
    let header = parse_btree_header(&page_data, offset)?;

    if header.cell_count == 0 && header.right_most_pointer.is_none() {
        return Ok(0);
    }

    if header.page_type.is_leaf() {
        if header.cell_count == 0 {
            return Ok(0);
        }
        let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);
        let last_ptr = pointers[header.cell_count as usize - 1] as usize;
        let usable = pager.usable_size();
        let cell = parse_table_leaf_cell(&page_data, last_ptr, usable)?;
        Ok(cell.rowid)
    } else {
        // Descend to rightmost leaf
        let right = header.right_most_pointer.unwrap();
        btree_max_rowid(pager, right)
    }
}

/// Create a new empty table B-tree. Returns the root page number.
pub fn btree_create_table(pager: &mut Pager) -> Result<u32> {
    let page_num = pager.allocate_page()?;
    {
        let page = pager.get_page_mut(page_num)?;
        init_leaf_page(&mut page.data, page_num);
    }
    Ok(page_num)
}

// ── Index B-tree operations ──

fn build_index_leaf_cell(payload: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(payload.len() + 9);
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(payload.len() as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell.extend_from_slice(payload);
    cell
}

fn build_index_interior_cell(left_child: u32, payload: &[u8]) -> Vec<u8> {
    let mut cell = Vec::with_capacity(payload.len() + 13);
    cell.extend_from_slice(&left_child.to_be_bytes());
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(payload.len() as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell.extend_from_slice(payload);
    cell
}

#[derive(Debug)]
struct IndexLeafCell {
    payload: Vec<u8>,
}

#[derive(Debug)]
struct IndexInteriorCell {
    left_child_page: u32,
    payload: Vec<u8>,
}

fn parse_index_leaf_cell(data: &[u8], offset: usize, usable_size: u32) -> Result<IndexLeafCell> {
    let (payload_size, n1) = varint::read_varint(&data[offset..]);
    let payload_start = offset + n1;
    let payload_size = payload_size as usize;

    let max_local = max_local_payload_leaf(usable_size) as usize;
    let local_size = if payload_size <= max_local {
        payload_size
    } else {
        let min_local = min_local_payload(usable_size) as usize;
        let mut local = min_local + (payload_size - min_local) % (usable_size as usize - 4);
        if local > max_local {
            local = min_local;
        }
        local
    };

    let payload = data[payload_start..payload_start + local_size].to_vec();
    Ok(IndexLeafCell { payload })
}

fn parse_index_interior_cell(data: &[u8], offset: usize, usable_size: u32) -> Result<IndexInteriorCell> {
    let left_child = u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]);
    let (payload_size, n1) = varint::read_varint(&data[offset + 4..]);
    let payload_start = offset + 4 + n1;
    let payload_size = payload_size as usize;

    let max_local = max_local_payload_leaf(usable_size) as usize;
    let local_size = if payload_size <= max_local {
        payload_size
    } else {
        let min_local = min_local_payload(usable_size) as usize;
        let mut local = min_local + (payload_size - min_local) % (usable_size as usize - 4);
        if local > max_local {
            local = min_local;
        }
        local
    };

    let payload = data[payload_start..payload_start + local_size].to_vec();
    Ok(IndexInteriorCell {
        left_child_page: left_child,
        payload,
    })
}

fn init_leaf_index_page(data: &mut [u8], page_num: u32) {
    let offset = btree_header_offset(page_num);
    let usable = data.len() as u32;
    data[offset] = PageType::LeafIndex as u8;
    data[offset + 1] = 0;
    data[offset + 2] = 0;
    data[offset + 3] = 0;
    data[offset + 4] = 0;
    let content_offset = usable as u16;
    data[offset + 5] = (content_offset >> 8) as u8;
    data[offset + 6] = content_offset as u8;
    data[offset + 7] = 0;
}

fn init_interior_index_page(data: &mut [u8], page_num: u32, right_child: u32) {
    let offset = btree_header_offset(page_num);
    let usable = data.len() as u32;
    data[offset] = PageType::InteriorIndex as u8;
    data[offset + 1] = 0;
    data[offset + 2] = 0;
    data[offset + 3] = 0;
    data[offset + 4] = 0;
    let content_offset = usable as u16;
    data[offset + 5] = (content_offset >> 8) as u8;
    data[offset + 6] = content_offset as u8;
    data[offset + 7] = 0;
    data[offset + 8..offset + 12].copy_from_slice(&right_child.to_be_bytes());
}

fn compare_records(a: &Record, b: &Record) -> std::cmp::Ordering {
    let len = a.values.len().min(b.values.len());
    for i in 0..len {
        let ord = compare_values(&a.values[i], &b.values[i]);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a.values.len().cmp(&b.values.len())
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    let ao = value_type_order(a);
    let bo = value_type_order(b);
    if ao != bo {
        return ao.cmp(&bo);
    }
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Integer(x), Value::Real(y)) => (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Real(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

fn value_type_order(v: &Value) -> i32 {
    match v {
        Value::Null => 0,
        Value::Integer(_) | Value::Real(_) => 1,
        Value::Text(_) => 2,
        Value::Blob(_) => 3,
    }
}

/// Create a new empty index B-tree. Returns the root page number.
pub fn btree_create_index(pager: &mut Pager) -> Result<u32> {
    let page_num = pager.allocate_page()?;
    {
        let page = pager.get_page_mut(page_num)?;
        init_leaf_index_page(&mut page.data, page_num);
    }
    Ok(page_num)
}

/// Insert a key record into an index B-tree. Returns the new root page number.
pub fn btree_index_insert(pager: &mut Pager, root_page: u32, key: &Record) -> Result<u32> {
    let payload = key.encode();
    let cell = build_index_leaf_cell(&payload);
    index_insert_into_page(pager, root_page, key, &cell)
}

fn index_insert_into_page(
    pager: &mut Pager,
    page_num: u32,
    key: &Record,
    cell: &[u8],
) -> Result<u32> {
    let page_data = pager.get_page(page_num)?.data.clone();
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(&page_data, offset)?;
    let usable = pager.usable_size();

    if header.page_type == PageType::LeafIndex {
        let result = try_insert_cell_into_index_leaf(pager, page_num, key, cell)?;
        match result {
            InsertResult::Ok => Ok(page_num),
            InsertResult::Split {
                new_page,
                median_rowid: _,
            } => {
                let new_root = pager.allocate_page()?;
                let median_page_data = pager.get_page(page_num)?.data.clone();
                let median_offset = btree_header_offset(page_num);
                let median_header = parse_btree_header(&median_page_data, median_offset)?;
                let median_pointers = read_cell_pointers(
                    &median_page_data,
                    median_offset + median_header.header_size(),
                    median_header.cell_count,
                );
                let last_ptr = median_pointers[median_header.cell_count as usize - 1] as usize;
                let last_cell = parse_index_leaf_cell(&median_page_data, last_ptr, usable)?;

                {
                    let root_data = &mut pager.get_page_mut(new_root)?.data;
                    init_interior_index_page(root_data, new_root, new_page);
                }
                let interior_cell = build_index_interior_cell(page_num, &last_cell.payload);
                insert_cell_into_interior(pager, new_root, &interior_cell)?;
                Ok(new_root)
            }
        }
    } else {
        let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);
        let mut child_page = header.right_most_pointer.unwrap();

        for i in 0..header.cell_count as usize {
            let cell_offset = pointers[i] as usize;
            let ic = parse_index_interior_cell(&page_data, cell_offset, usable)?;
            let ic_record = Record::decode(&ic.payload)?;
            if compare_records(key, &ic_record) != std::cmp::Ordering::Greater {
                child_page = ic.left_child_page;
                break;
            }
        }

        let new_child_root = index_insert_into_page(pager, child_page, key, cell)?;
        if new_child_root != child_page {
            update_child_pointer(pager, page_num, child_page, new_child_root)?;
        }
        Ok(page_num)
    }
}

fn try_insert_cell_into_index_leaf(
    pager: &mut Pager,
    page_num: u32,
    key: &Record,
    cell: &[u8],
) -> Result<InsertResult> {
    let usable = pager.usable_size();
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(data, offset)?;

    let ptr_area_start = offset + header.header_size();
    let ptr_area_end = ptr_area_start + header.cell_count as usize * 2;
    let content_start = header.cell_content_offset as usize;

    let space_needed = 2 + cell.len();
    let free_space = content_start - ptr_area_end;

    if space_needed <= free_space {
        let pointers = read_cell_pointers(data, ptr_area_start, header.cell_count);

        let mut insert_pos = pointers.len();
        for (i, &ptr) in pointers.iter().enumerate() {
            let existing_cell = parse_index_leaf_cell(data, ptr as usize, usable)?;
            let existing_record = Record::decode(&existing_cell.payload)?;
            if compare_records(key, &existing_record) != std::cmp::Ordering::Greater {
                insert_pos = i;
                break;
            }
        }

        let new_content_start = content_start - cell.len();
        data[new_content_start..new_content_start + cell.len()].copy_from_slice(cell);

        let mut new_pointers = Vec::with_capacity(pointers.len() + 1);
        for (i, &ptr) in pointers.iter().enumerate() {
            if i == insert_pos {
                new_pointers.push(new_content_start as u16);
            }
            new_pointers.push(ptr);
        }
        if insert_pos == pointers.len() {
            new_pointers.push(new_content_start as u16);
        }

        let new_cell_count = header.cell_count + 1;
        data[offset + 3..offset + 5].copy_from_slice(&new_cell_count.to_be_bytes());
        let content_u16 = new_content_start as u16;
        data[offset + 5..offset + 7].copy_from_slice(&content_u16.to_be_bytes());
        write_cell_pointers(data, ptr_area_start, &new_pointers);

        Ok(InsertResult::Ok)
    } else {
        split_index_leaf(pager, page_num, key, cell)
    }
}

fn split_index_leaf(
    pager: &mut Pager,
    page_num: u32,
    new_key: &Record,
    new_cell: &[u8],
) -> Result<InsertResult> {
    let usable = pager.usable_size();
    let page_data = pager.get_page(page_num)?.data.clone();
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(&page_data, offset)?;
    let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);

    let mut cells: Vec<(Record, Vec<u8>)> = Vec::new();
    for &ptr in &pointers {
        let c = parse_index_leaf_cell(&page_data, ptr as usize, usable)?;
        let record = Record::decode(&c.payload)?;
        let raw = build_index_leaf_cell(&c.payload);
        cells.push((record, raw));
    }
    cells.push((new_key.clone(), new_cell.to_vec()));
    cells.sort_by(|(a, _), (b, _)| compare_records(a, b));

    let mid = cells.len() / 2;
    let left_cells: Vec<(i64, Vec<u8>)> = cells[..mid].iter().map(|(_, raw)| (0, raw.clone())).collect();
    let right_cells: Vec<(i64, Vec<u8>)> = cells[mid..].iter().map(|(_, raw)| (0, raw.clone())).collect();

    rewrite_index_leaf_page(pager, page_num, &left_cells)?;

    let new_page = pager.allocate_page()?;
    {
        let data = &mut pager.get_page_mut(new_page)?.data;
        init_leaf_index_page(data, new_page);
    }
    rewrite_index_leaf_page(pager, new_page, &right_cells)?;

    Ok(InsertResult::Split {
        new_page,
        median_rowid: 0,
    })
}

fn rewrite_index_leaf_page(pager: &mut Pager, page_num: u32, cells: &[(i64, Vec<u8>)]) -> Result<()> {
    let page_size = pager.page_size() as usize;
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);

    let clear_start = offset;
    data[clear_start..page_size].fill(0);
    init_leaf_index_page(data, page_num);

    let ptr_area_start = offset + 8;
    let mut content_end = page_size;
    let mut pointers = Vec::with_capacity(cells.len());

    for (_, cell_data) in cells {
        content_end -= cell_data.len();
        data[content_end..content_end + cell_data.len()].copy_from_slice(cell_data);
        pointers.push(content_end as u16);
    }

    let cell_count = cells.len() as u16;
    data[offset + 3..offset + 5].copy_from_slice(&cell_count.to_be_bytes());
    let content_u16 = content_end as u16;
    data[offset + 5..offset + 7].copy_from_slice(&content_u16.to_be_bytes());
    write_cell_pointers(data, ptr_area_start, &pointers);

    Ok(())
}

/// Delete a key from an index B-tree by matching exact record values.
pub fn btree_index_delete(pager: &mut Pager, root_page: u32, key: &Record) -> Result<()> {
    let mut cursor = IndexCursor::new(pager, root_page);
    let entries = cursor.collect_all()?;

    let remaining: Vec<Vec<u8>> = entries
        .into_iter()
        .filter(|rec| compare_records(rec, key) != std::cmp::Ordering::Equal)
        .map(|rec| {
            let payload = rec.encode();
            build_index_leaf_cell(&payload)
        })
        .collect();

    let cells: Vec<(i64, Vec<u8>)> = remaining.into_iter().map(|raw| (0, raw)).collect();
    rewrite_index_leaf_page(pager, root_page, &cells)?;

    Ok(())
}

/// Cursor for traversing an index B-tree.
pub struct IndexCursor<'a> {
    pager: &'a mut Pager,
    root_page: u32,
    stack: Vec<(u32, usize)>,
    state: CursorState,
}

impl<'a> IndexCursor<'a> {
    pub fn new(pager: &'a mut Pager, root_page: u32) -> Self {
        Self {
            pager,
            root_page,
            stack: Vec::new(),
            state: CursorState::Invalid,
        }
    }

    pub fn first(&mut self) -> Result<bool> {
        self.stack.clear();
        self.state = CursorState::Invalid;
        self.descend_to_leftmost(self.root_page)?;
        self.check_valid()
    }

    pub fn next(&mut self) -> Result<bool> {
        if self.state != CursorState::Valid {
            return Ok(false);
        }

        if let Some(entry) = self.stack.last_mut() {
            entry.1 += 1;
        }

        loop {
            let (page_num, idx) = match self.stack.last().copied() {
                Some(entry) => entry,
                None => {
                    self.state = CursorState::AtEnd;
                    return Ok(false);
                }
            };

            let page = self.pager.get_page(page_num)?.data.clone();
            let offset = btree_header_offset(page_num);
            let header = parse_btree_header(&page, offset)?;

            if header.page_type.is_leaf() {
                if idx < header.cell_count as usize {
                    self.state = CursorState::Valid;
                    return Ok(true);
                }
                self.stack.pop();
                if let Some(entry) = self.stack.last_mut() {
                    entry.1 += 1;
                }
            } else {
                let total_children = header.cell_count as usize + 1;
                if idx < total_children {
                    let child_page = self.get_child_page(&page, offset, &header, idx)?;
                    self.descend_to_leftmost(child_page)?;
                    return self.check_valid();
                }
                self.stack.pop();
                if let Some(entry) = self.stack.last_mut() {
                    entry.1 += 1;
                }
            }
        }
    }

    pub fn current(&mut self) -> Result<Record> {
        if self.state != CursorState::Valid {
            return Err(StorageError::Other("cursor not positioned".to_string()));
        }

        let &(page_num, cell_idx) = self.stack.last().unwrap();
        let page = self.pager.get_page(page_num)?.data.clone();
        let offset = btree_header_offset(page_num);
        let header = parse_btree_header(&page, offset)?;
        let usable = self.pager.usable_size();

        let pointers =
            read_cell_pointers(&page, offset + header.header_size(), header.cell_count);
        let cell_offset = pointers[cell_idx] as usize;
        let cell = parse_index_leaf_cell(&page, cell_offset, usable)?;
        Record::decode(&cell.payload)
    }

    pub fn collect_all(&mut self) -> Result<Vec<Record>> {
        let mut records = Vec::new();
        let mut has_row = self.first()?;
        while has_row {
            records.push(self.current()?);
            has_row = self.next()?;
        }
        Ok(records)
    }

    fn get_child_page(
        &mut self,
        page_data: &[u8],
        offset: usize,
        header: &BTreePageHeader,
        child_idx: usize,
    ) -> Result<u32> {
        let cell_count = header.cell_count as usize;
        if child_idx < cell_count {
            let pointers =
                read_cell_pointers(page_data, offset + header.header_size(), header.cell_count);
            let cell_offset = pointers[child_idx] as usize;
            let usable = self.pager.usable_size();
            let cell = parse_index_interior_cell(page_data, cell_offset, usable)?;
            Ok(cell.left_child_page)
        } else {
            header.right_most_pointer.ok_or_else(|| {
                StorageError::Corrupt("interior page missing rightmost pointer".to_string())
            })
        }
    }

    fn descend_to_leftmost(&mut self, page_num: u32) -> Result<()> {
        let mut current_page = page_num;
        loop {
            let page_data = self.pager.get_page(current_page)?.data.clone();
            let offset = btree_header_offset(current_page);
            let header = parse_btree_header(&page_data, offset)?;

            if header.page_type.is_leaf() {
                self.stack.push((current_page, 0));
                return Ok(());
            }

            self.stack.push((current_page, 0));
            let child = self.get_child_page(&page_data, offset, &header, 0)?;
            current_page = child;
        }
    }

    fn check_valid(&mut self) -> Result<bool> {
        if let Some(&(page_num, cell_idx)) = self.stack.last() {
            let page_data = self.pager.get_page(page_num)?.data.clone();
            let offset = btree_header_offset(page_num);
            let header = parse_btree_header(&page_data, offset)?;
            if header.page_type.is_leaf() && cell_idx < header.cell_count as usize {
                self.state = CursorState::Valid;
                return Ok(true);
            }
        }
        self.state = CursorState::AtEnd;
        Ok(false)
    }
}

/// Delete a row by rowid from a table B-tree. Simple approach: rebuild without the row.
pub fn btree_delete(pager: &mut Pager, root_page: u32, rowid: i64) -> Result<()> {
    let mut cursor = BTreeCursor::new(pager, root_page);
    let mut rows: Vec<(i64, Vec<u8>)> = Vec::new();
    let mut has_row = cursor.first()?;
    while has_row {
        let current = cursor.current()?;
        if current.rowid != rowid {
            let payload = current.record.encode();
            rows.push((current.rowid, payload));
        }
        has_row = cursor.next()?;
    }

    // Rewrite the root as a leaf with all remaining rows
    rewrite_leaf_page(pager, root_page, &rows.iter().map(|(r, p)| {
        (*r, build_table_leaf_cell(*r, p))
    }).collect::<Vec<_>>())?;

    Ok(())
}

/// Insert a row into the sqlite_schema table (page 1).
pub fn insert_schema_entry(
    pager: &mut Pager,
    entry_type: &str,
    name: &str,
    tbl_name: &str,
    rootpage: u32,
    sql: &str,
) -> Result<()> {
    let record = Record {
        values: vec![
            Value::Text(entry_type.to_string()),
            Value::Text(name.to_string()),
            Value::Text(tbl_name.to_string()),
            Value::Integer(rootpage as i64),
            Value::Text(sql.to_string()),
        ],
    };

    let max_rowid = btree_max_rowid(pager, 1)?;
    let new_rowid = max_rowid + 1;
    let new_root = btree_insert(pager, 1, new_rowid, &record)?;

    // If the schema B-tree root moved (split), that's a problem since
    // sqlite_schema is always rooted at page 1. For now this won't happen
    // for small schemas.
    if new_root != 1 {
        return Err(StorageError::Other(
            "sqlite_schema root page split — not yet supported".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsqlite_vfs::memory::MemoryVfs;

    #[test]
    fn parse_empty_leaf_page() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();
        let page = pager.get_page(1).unwrap();

        let header = parse_btree_header(&page.data, HEADER_SIZE).unwrap();
        assert_eq!(header.page_type, PageType::LeafTable);
        assert_eq!(header.cell_count, 0);
        assert!(header.right_most_pointer.is_none());
    }

    #[test]
    fn cursor_on_empty_table() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();
        let mut cursor = BTreeCursor::new(&mut pager, 1);
        assert!(!cursor.first().unwrap());
    }

    #[test]
    fn read_real_sqlite_database() {
        let test_db = "/tmp/rsqlite_btree_test.db";
        let _ = std::fs::remove_file(test_db);
        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);\
                 INSERT INTO users VALUES (1, 'Alice', 30);\
                 INSERT INTO users VALUES (2, 'Bob', 25);\
                 INSERT INTO users VALUES (3, 'Charlie', 35);",
            )
            .status();

        match status {
            Ok(s) if s.success() => {
                let vfs = rsqlite_vfs::native::NativeVfs::new();
                let mut pager = Pager::open(&vfs, test_db).unwrap();

                // Read schema
                let schema = read_schema(&mut pager).unwrap();
                assert!(
                    !schema.is_empty(),
                    "schema should have at least one entry"
                );
                let table_entry = schema.iter().find(|e| e.name == "users").unwrap();
                assert_eq!(table_entry.entry_type, "table");
                assert_eq!(table_entry.tbl_name, "users");
                assert!(table_entry.rootpage > 0);

                // Read table data
                let root = table_entry.rootpage;
                let mut cursor = BTreeCursor::new(&mut pager, root);
                let rows = cursor.collect_all().unwrap();
                assert_eq!(rows.len(), 3);

                // SQLite stores all columns in the record, including the INTEGER PRIMARY KEY
                // (which is NULL in the record since the rowid carries the value).
                // So: [NULL or rowid, name, age]
                assert_eq!(rows[0].rowid, 1);
                let vals = &rows[0].record.values;
                // Find name and age — they are the last two values
                let name_idx = vals.len() - 2;
                let age_idx = vals.len() - 1;
                assert_eq!(vals[name_idx], Value::Text("Alice".to_string()));
                assert_eq!(vals[age_idx], Value::Integer(30));

                assert_eq!(rows[1].rowid, 2);
                assert_eq!(
                    rows[1].record.values[name_idx],
                    Value::Text("Bob".to_string())
                );
                assert_eq!(rows[1].record.values[age_idx], Value::Integer(25));

                assert_eq!(rows[2].rowid, 3);
                assert_eq!(
                    rows[2].record.values[name_idx],
                    Value::Text("Charlie".to_string())
                );
                assert_eq!(rows[2].record.values[age_idx], Value::Integer(35));

                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping real database test");
            }
        }
    }

    #[test]
    fn read_schema_from_real_db() {
        let test_db = "/tmp/rsqlite_schema_test.db";
        let _ = std::fs::remove_file(test_db);
        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg(
                "CREATE TABLE t1 (a INTEGER, b TEXT);\
                 CREATE TABLE t2 (x REAL, y BLOB);\
                 CREATE INDEX idx_t1_a ON t1(a);",
            )
            .status();

        match status {
            Ok(s) if s.success() => {
                let vfs = rsqlite_vfs::native::NativeVfs::new();
                let mut pager = Pager::open(&vfs, test_db).unwrap();
                let schema = read_schema(&mut pager).unwrap();

                let tables: Vec<_> = schema
                    .iter()
                    .filter(|e| e.entry_type == "table")
                    .collect();
                let indexes: Vec<_> = schema
                    .iter()
                    .filter(|e| e.entry_type == "index")
                    .collect();

                assert_eq!(tables.len(), 2);
                assert!(tables.iter().any(|t| t.name == "t1"));
                assert!(tables.iter().any(|t| t.name == "t2"));

                assert_eq!(indexes.len(), 1);
                assert_eq!(indexes[0].name, "idx_t1_a");
                assert_eq!(indexes[0].tbl_name, "t1");

                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping schema test");
            }
        }
    }

    #[test]
    fn read_larger_database() {
        let test_db = "/tmp/rsqlite_larger_test.db";
        let _ = std::fs::remove_file(test_db);

        // Insert enough rows to potentially span multiple pages
        let mut sql = String::from("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);");
        for i in 1..=200 {
            sql.push_str(&format!(
                "INSERT INTO data VALUES ({i}, 'value_{i}_padding_to_make_it_longer_{i}');"
            ));
        }

        let status = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg(&sql)
            .status();

        match status {
            Ok(s) if s.success() => {
                let vfs = rsqlite_vfs::native::NativeVfs::new();
                let mut pager = Pager::open(&vfs, test_db).unwrap();

                let schema = read_schema(&mut pager).unwrap();
                let table_entry = schema.iter().find(|e| e.name == "data").unwrap();

                let mut cursor = BTreeCursor::new(&mut pager, table_entry.rootpage);
                let rows = cursor.collect_all().unwrap();
                assert_eq!(rows.len(), 200, "should have 200 rows");

                // Verify ordering (should be by rowid)
                for (i, row) in rows.iter().enumerate() {
                    assert_eq!(row.rowid, (i + 1) as i64);
                }

                // Verify last row content (value column is the last in the record)
                let last = &rows[199];
                assert_eq!(last.rowid, 200);
                let last_val = last.record.values.last().unwrap();
                if let Value::Text(s) = last_val {
                    assert!(s.contains("value_200"));
                } else {
                    panic!("expected text value, got {last_val:?}");
                }

                let _ = std::fs::remove_file(test_db);
            }
            _ => {
                eprintln!("sqlite3 not available, skipping larger database test");
            }
        }
    }

    #[test]
    fn insert_into_empty_leaf() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();

        let record = Record {
            values: vec![Value::Text("hello".to_string()), Value::Integer(42)],
        };
        let root = btree_insert(&mut pager, 1, 1, &record).unwrap();
        assert_eq!(root, 1);

        let mut cursor = BTreeCursor::new(&mut pager, 1);
        let rows = cursor.collect_all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].rowid, 1);
    }

    #[test]
    fn insert_multiple_rows_sorted() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();

        // Insert out of order
        for &id in &[3i64, 1, 4, 1, 5, 9, 2, 6] {
            let record = Record {
                values: vec![Value::Integer(id * 10)],
            };
            btree_insert(&mut pager, 1, id, &record).unwrap();
        }

        let mut cursor = BTreeCursor::new(&mut pager, 1);
        let rows = cursor.collect_all().unwrap();
        assert_eq!(rows.len(), 8);

        // Should be sorted by rowid
        let rowids: Vec<i64> = rows.iter().map(|r| r.rowid).collect();
        assert_eq!(rowids, vec![1, 1, 2, 3, 4, 5, 6, 9]);
    }

    #[test]
    fn insert_triggers_page_split() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();

        // Use a dedicated table page (not page 1 which has the DB header)
        let table_root = btree_create_table(&mut pager).unwrap();
        let mut root = table_root;
        // Use large records to guarantee page splits
        let padding = "x".repeat(200);
        for i in 1..=50 {
            let record = Record {
                values: vec![
                    Value::Text(format!("name_{i}_{padding}")),
                    Value::Integer(i * 100),
                ],
            };
            root = btree_insert(&mut pager, root, i, &record).unwrap();
        }

        // Should have multiple pages now (each record is ~210 bytes, 50*210=10500 > 4096)
        assert!(
            pager.page_count() > 2,
            "expected page splits for 50 large rows, got {} pages",
            pager.page_count()
        );

        // Read back all rows
        let mut cursor = BTreeCursor::new(&mut pager, root);
        let rows = cursor.collect_all().unwrap();
        assert_eq!(rows.len(), 50);

        // Verify order
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.rowid, (i + 1) as i64, "row order mismatch at index {i}");
        }
    }

    #[test]
    fn btree_max_rowid_works() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();

        assert_eq!(btree_max_rowid(&mut pager, 1).unwrap(), 0);

        for i in 1..=10 {
            let record = Record {
                values: vec![Value::Integer(i)],
            };
            btree_insert(&mut pager, 1, i, &record).unwrap();
        }

        assert_eq!(btree_max_rowid(&mut pager, 1).unwrap(), 10);
    }

    #[test]
    fn btree_create_and_insert() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();

        let table_root = btree_create_table(&mut pager).unwrap();
        assert!(table_root > 1);

        let record = Record {
            values: vec![Value::Text("test".to_string())],
        };
        let root = btree_insert(&mut pager, table_root, 1, &record).unwrap();
        assert_eq!(root, table_root);

        let mut cursor = BTreeCursor::new(&mut pager, table_root);
        let rows = cursor.collect_all().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn btree_delete_row() {
        let vfs = MemoryVfs::new();
        let mut pager = Pager::create(&vfs, "test.db").unwrap();

        for i in 1..=5 {
            let record = Record {
                values: vec![Value::Integer(i * 10)],
            };
            btree_insert(&mut pager, 1, i, &record).unwrap();
        }

        btree_delete(&mut pager, 1, 3).unwrap();

        let mut cursor = BTreeCursor::new(&mut pager, 1);
        let rows = cursor.collect_all().unwrap();
        assert_eq!(rows.len(), 4);
        let rowids: Vec<i64> = rows.iter().map(|r| r.rowid).collect();
        assert_eq!(rowids, vec![1, 2, 4, 5]);
    }

    #[test]
    fn write_and_verify_with_sqlite3() {
        let test_db = "/tmp/rsqlite_write_compat.db";
        let _ = std::fs::remove_file(test_db);

        // Create a database with rsqlite
        let vfs = rsqlite_vfs::native::NativeVfs::new();
        let mut pager = Pager::create(&vfs, test_db).unwrap();

        // Create a table (need to insert into sqlite_schema)
        let table_root = btree_create_table(&mut pager).unwrap();
        insert_schema_entry(
            &mut pager,
            "table",
            "test_table",
            "test_table",
            table_root,
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
        )
        .unwrap();

        // Insert some rows — for INTEGER PRIMARY KEY, record has [NULL, name, value]
        for i in 1..=5 {
            let record = Record {
                values: vec![
                    Value::Null,
                    Value::Text(format!("item_{i}")),
                    Value::Integer(i * 100),
                ],
            };
            btree_insert(&mut pager, table_root, i, &record).unwrap();
        }

        pager.flush().unwrap();

        // Verify with sqlite3
        let output = std::process::Command::new("sqlite3")
            .arg(test_db)
            .arg("SELECT * FROM test_table ORDER BY id;")
            .output();

        match output {
            Ok(out) if out.status.success() => {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let lines: Vec<&str> = stdout.trim().lines().collect();
                assert_eq!(lines.len(), 5, "expected 5 rows, got: {stdout}");
                assert!(lines[0].contains("item_1"), "first row: {}", lines[0]);
                assert!(lines[4].contains("item_5"), "last row: {}", lines[4]);
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                panic!("sqlite3 failed: {stderr}");
            }
            Err(_) => {
                eprintln!("sqlite3 not available, skipping write compat test");
            }
        }

        let _ = std::fs::remove_file(test_db);
    }
}
