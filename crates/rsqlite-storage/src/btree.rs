#[path = "btree_write.rs"]
mod btree_write;

pub use btree_write::{
    btree_create_index, btree_create_table, btree_delete, btree_delete_many, btree_index_delete,
    btree_index_delete_by_prefix, btree_index_delete_many, btree_index_insert, btree_insert,
    delete_schema_entries, insert_schema_entry,
};

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
        if self.page_type.is_leaf() { 8 } else { 12 }
    }
}

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

pub(crate) fn read_cell_pointers(data: &[u8], offset: usize, count: u16) -> Vec<u16> {
    let mut pointers = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        let pos = offset + i * 2;
        let ptr = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pointers.push(ptr);
    }
    pointers
}

#[derive(Debug)]
pub struct TableLeafCell {
    pub rowid: i64,
    /// The inline (local) portion of the payload. When the full payload
    /// spilled onto overflow pages this is only the first `local_size`
    /// bytes; use [`reassemble_payload`] (or `BTreeCursor`/`IndexCursor`
    /// which do it for you) to obtain the complete payload.
    pub payload: Vec<u8>,
    /// Total payload length as recorded in the cell header.
    pub payload_size: usize,
    /// First overflow page, when `payload.len() < payload_size`.
    pub overflow_page: Option<u32>,
}

#[derive(Debug)]
pub struct TableInteriorCell {
    pub left_child_page: u32,
    pub rowid: i64,
}

pub(crate) fn parse_table_leaf_cell(
    data: &[u8],
    offset: usize,
    usable_size: u32,
) -> Result<TableLeafCell> {
    let (payload_size, n1) = varint::read_varint(&data[offset..]);
    let (rowid, n2) = varint::read_varint(&data[offset + n1..]);
    let payload_start = offset + n1 + n2;
    let payload_size = payload_size as usize;

    let local_size = local_payload_size(payload_size, usable_size);

    let payload = data[payload_start..payload_start + local_size].to_vec();
    let overflow_page = if local_size < payload_size {
        let p = payload_start + local_size;
        Some(u32::from_be_bytes([
            data[p],
            data[p + 1],
            data[p + 2],
            data[p + 3],
        ]))
    } else {
        None
    };
    Ok(TableLeafCell {
        rowid: rowid as i64,
        payload,
        payload_size,
        overflow_page,
    })
}

/// Compute the number of payload bytes stored locally (inline) in a cell,
/// per the SQLite overflow-threshold formula. Identical for table-leaf and
/// index cells (both use the leaf max-local bound).
pub(crate) fn local_payload_size(payload_size: usize, usable_size: u32) -> usize {
    let max_local = max_local_payload_leaf(usable_size) as usize;
    if payload_size <= max_local {
        return payload_size;
    }
    let min_local = min_local_payload(usable_size) as usize;
    let mut local = min_local + (payload_size - min_local) % (usable_size as usize - 4);
    if local > max_local {
        local = min_local;
    }
    local
}

/// Walk an overflow-page chain starting at `first_page`, collecting
/// `remaining` bytes. Each overflow page begins with a 4-byte big-endian
/// pointer to the next overflow page (0 on the last page), followed by up
/// to `usable_size - 4` payload bytes.
pub(crate) fn read_overflow_chain(
    pager: &mut Pager,
    first_page: u32,
    remaining: usize,
) -> Result<Vec<u8>> {
    let usable = pager.usable_size() as usize;
    let per_page = usable - 4;
    let mut out = Vec::with_capacity(remaining);
    let mut left = remaining;
    let mut page = first_page;
    while left > 0 {
        if page == 0 {
            return Err(StorageError::Corrupt(
                "overflow chain ended before payload complete".to_string(),
            ));
        }
        let data = pager.get_page(page)?.data.clone();
        let next = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let take = left.min(per_page);
        out.extend_from_slice(&data[4..4 + take]);
        left -= take;
        page = next;
    }
    Ok(out)
}

/// Reassemble a full payload from a cell's inline portion plus its overflow
/// chain (if any). When the cell has no overflow this just clones the local
/// payload.
pub(crate) fn reassemble_payload(
    pager: &mut Pager,
    local: &[u8],
    payload_size: usize,
    overflow_page: Option<u32>,
) -> Result<Vec<u8>> {
    match overflow_page {
        None => Ok(local.to_vec()),
        Some(first) => {
            let mut full = Vec::with_capacity(payload_size);
            full.extend_from_slice(local);
            let tail = read_overflow_chain(pager, first, payload_size - local.len())?;
            full.extend_from_slice(&tail);
            Ok(full)
        }
    }
}

pub(crate) fn parse_table_interior_cell(data: &[u8], offset: usize) -> TableInteriorCell {
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

pub(crate) fn max_local_payload_leaf(usable_size: u32) -> u32 {
    usable_size - 35
}

pub(crate) fn min_local_payload(usable_size: u32) -> u32 {
    (usable_size - 12) * 32 / 255 - 23
}

pub struct BTreeCursor<'a> {
    pager: &'a mut Pager,
    root_page: u32,
    stack: Vec<(u32, usize)>,
    state: CursorState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    Invalid,
    Valid,
    AtEnd,
}

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

    pub fn current(&mut self) -> Result<CursorRow> {
        if self.state != CursorState::Valid {
            return Err(StorageError::Other("cursor not positioned".to_string()));
        }

        let &(page_num, cell_idx) = self.stack.last().unwrap();
        let page = self.pager.get_page(page_num)?.data.clone();
        let offset = btree_header_offset(page_num);
        let header = parse_btree_header(&page, offset)?;
        let usable = self.pager.usable_size();

        let pointers = read_cell_pointers(&page, offset + header.header_size(), header.cell_count);
        let cell_offset = pointers[cell_idx] as usize;
        let cell = parse_table_leaf_cell(&page, cell_offset, usable)?;

        let full = reassemble_payload(
            self.pager,
            &cell.payload,
            cell.payload_size,
            cell.overflow_page,
        )?;
        let record = Record::decode(&full)?;
        Ok(CursorRow {
            rowid: cell.rowid,
            record,
        })
    }

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

pub(crate) fn btree_header_offset(page_num: u32) -> usize {
    if page_num == 1 { HEADER_SIZE } else { 0 }
}

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
        let pointers =
            read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);
        let last_ptr = pointers[header.cell_count as usize - 1] as usize;
        let usable = pager.usable_size();
        let cell = parse_table_leaf_cell(&page_data, last_ptr, usable)?;
        Ok(cell.rowid)
    } else {
        let right = header.right_most_pointer.unwrap();
        btree_max_rowid(pager, right)
    }
}

pub fn btree_row_exists(pager: &mut Pager, root_page: u32, target_rowid: i64) -> Result<bool> {
    let page_data = pager.get_page(root_page)?.data.clone();
    let offset = btree_header_offset(root_page);
    let header = parse_btree_header(&page_data, offset)?;

    if header.cell_count == 0 && header.page_type.is_leaf() {
        return Ok(false);
    }

    let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);

    if header.page_type.is_leaf() {
        let usable = pager.usable_size();
        for &ptr in &pointers {
            let cell = parse_table_leaf_cell(&page_data, ptr as usize, usable)?;
            if cell.rowid == target_rowid {
                return Ok(true);
            }
            if cell.rowid > target_rowid {
                return Ok(false);
            }
        }
        Ok(false)
    } else {
        for &ptr in &pointers {
            let cell = parse_table_interior_cell(&page_data, ptr as usize);
            if target_rowid <= cell.rowid {
                return btree_row_exists(pager, cell.left_child_page, target_rowid);
            }
        }
        let right = header.right_most_pointer.unwrap();
        btree_row_exists(pager, right, target_rowid)
    }
}

// ── Index B-tree read operations ──

#[derive(Debug)]
pub(crate) struct IndexLeafCell {
    pub payload: Vec<u8>,
    pub payload_size: usize,
    pub overflow_page: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct IndexInteriorCell {
    pub left_child_page: u32,
    pub payload: Vec<u8>,
    pub payload_size: usize,
    pub overflow_page: Option<u32>,
}

pub(crate) fn parse_index_leaf_cell(
    data: &[u8],
    offset: usize,
    usable_size: u32,
) -> Result<IndexLeafCell> {
    let (payload_size, n1) = varint::read_varint(&data[offset..]);
    let payload_start = offset + n1;
    let payload_size = payload_size as usize;

    let local_size = local_payload_size(payload_size, usable_size);

    let payload = data[payload_start..payload_start + local_size].to_vec();
    let overflow_page = if local_size < payload_size {
        let p = payload_start + local_size;
        Some(u32::from_be_bytes([
            data[p],
            data[p + 1],
            data[p + 2],
            data[p + 3],
        ]))
    } else {
        None
    };
    Ok(IndexLeafCell {
        payload,
        payload_size,
        overflow_page,
    })
}

pub(crate) fn parse_index_interior_cell(
    data: &[u8],
    offset: usize,
    usable_size: u32,
) -> Result<IndexInteriorCell> {
    let left_child = u32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]);
    let (payload_size, n1) = varint::read_varint(&data[offset + 4..]);
    let payload_start = offset + 4 + n1;
    let payload_size = payload_size as usize;

    let local_size = local_payload_size(payload_size, usable_size);

    let payload = data[payload_start..payload_start + local_size].to_vec();
    let overflow_page = if local_size < payload_size {
        let p = payload_start + local_size;
        Some(u32::from_be_bytes([
            data[p],
            data[p + 1],
            data[p + 2],
            data[p + 3],
        ]))
    } else {
        None
    };
    Ok(IndexInteriorCell {
        left_child_page: left_child,
        payload,
        payload_size,
        overflow_page,
    })
}

pub(crate) fn compare_records(a: &Record, b: &Record) -> std::cmp::Ordering {
    let len = a.values.len().min(b.values.len());
    for i in 0..len {
        let ord = compare_values(&a.values[i], &b.values[i]);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a.values.len().cmp(&b.values.len())
}

/// Compare only the leading `prefix_len` values of `a` and `b`. Used by
/// WITHOUT ROWID lookups where the PK occupies the leading columns of the
/// stored record but the trailing payload columns differ. Length is not
/// considered — a record with fewer than `prefix_len` values is treated as
/// short and compares less for missing positions.
pub fn compare_records_by_prefix(a: &Record, b: &Record, prefix_len: usize) -> std::cmp::Ordering {
    for i in 0..prefix_len {
        match (a.values.get(i), b.values.get(i)) {
            (Some(av), Some(bv)) => {
                let ord = compare_values(av, bv);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (None, None) => return std::cmp::Ordering::Equal,
        }
    }
    std::cmp::Ordering::Equal
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
        (Value::Integer(x), Value::Real(y)) => (*x as f64)
            .partial_cmp(y)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Real(x), Value::Integer(y)) => x
            .partial_cmp(&(*y as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
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

        let pointers = read_cell_pointers(&page, offset + header.header_size(), header.cell_count);
        let cell_offset = pointers[cell_idx] as usize;
        let cell = parse_index_leaf_cell(&page, cell_offset, usable)?;
        let full = reassemble_payload(
            self.pager,
            &cell.payload,
            cell.payload_size,
            cell.overflow_page,
        )?;
        Record::decode(&full)
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

    /// Position the cursor at the first record whose leading
    /// `prefix.values.len()` values equal `prefix`. Returns true when
    /// such a record is found, false otherwise (cursor left at end).
    pub fn seek_first_with_prefix(&mut self, prefix: &Record) -> Result<bool> {
        let prefix_len = prefix.values.len();
        let mut has_row = self.first()?;
        while has_row {
            let rec = self.current()?;
            if compare_records_by_prefix(&rec, prefix, prefix_len) == std::cmp::Ordering::Equal {
                return Ok(true);
            }
            has_row = self.next()?;
        }
        Ok(false)
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

// ── Write helpers (pub(crate) for btree_write) ──

pub(crate) fn write_cell_pointers(data: &mut [u8], offset: usize, pointers: &[u16]) {
    for (i, ptr) in pointers.iter().enumerate() {
        let pos = offset + i * 2;
        data[pos..pos + 2].copy_from_slice(&ptr.to_be_bytes());
    }
}

pub(crate) fn init_leaf_page(data: &mut [u8], page_num: u32) {
    let offset = btree_header_offset(page_num);
    let usable = data.len() as u32;
    data[offset] = PageType::LeafTable as u8;
    data[offset + 1] = 0;
    data[offset + 2] = 0;
    data[offset + 3] = 0;
    data[offset + 4] = 0;
    let content_offset = usable as u16;
    data[offset + 5] = (content_offset >> 8) as u8;
    data[offset + 6] = content_offset as u8;
    data[offset + 7] = 0;
}

pub(crate) fn init_interior_page(data: &mut [u8], page_num: u32, right_child: u32) {
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

pub(crate) fn init_leaf_index_page(data: &mut [u8], page_num: u32) {
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

pub(crate) fn init_interior_index_page(data: &mut [u8], page_num: u32, right_child: u32) {
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

#[cfg(test)]
#[path = "btree_tests.rs"]
mod tests;
