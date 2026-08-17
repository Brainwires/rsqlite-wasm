use crate::btree::{
    BTreeCursor, IndexCursor, PageType, btree_header_offset, compare_records,
    compare_records_by_prefix, init_interior_index_page, init_interior_page, init_leaf_index_page,
    init_leaf_page, local_payload_size, parse_btree_header, parse_index_interior_cell,
    parse_index_leaf_cell, parse_table_interior_cell, parse_table_leaf_cell, read_cell_pointers,
    reassemble_payload, write_cell_pointers,
};
use crate::codec::{Record, Value};
use crate::error::{Result, StorageError};
use crate::pager::Pager;
use crate::varint;

/// Write the spilled tail of an oversized payload across a chain of overflow
/// pages (SQLite format): each overflow page begins with a 4-byte big-endian
/// pointer to the next page (0 on the last), followed by up to `usable - 4`
/// payload bytes. Returns the page number of the first overflow page.
fn write_overflow_chain(pager: &mut Pager, tail: &[u8]) -> Result<u32> {
    let usable = pager.usable_size() as usize;
    let per_page = usable - 4;

    // Allocate all pages first so each can point at its successor.
    let page_count = tail.len().div_ceil(per_page);
    let mut pages = Vec::with_capacity(page_count);
    for _ in 0..page_count {
        pages.push(pager.allocate_page()?);
    }

    for (i, &page) in pages.iter().enumerate() {
        let start = i * per_page;
        let end = (start + per_page).min(tail.len());
        let chunk = &tail[start..end];
        let next = if i + 1 < pages.len() { pages[i + 1] } else { 0 };
        let data = &mut pager.get_page_mut(page)?.data;
        data.fill(0);
        data[0..4].copy_from_slice(&next.to_be_bytes());
        data[4..4 + chunk.len()].copy_from_slice(chunk);
    }

    Ok(pages[0])
}

/// Build a table-leaf cell, spilling the payload onto overflow pages when it
/// exceeds the local threshold. The on-disk layout is:
/// `varint(payload_size) varint(rowid) <local payload bytes> [4-byte first
/// overflow page]`.
fn build_table_leaf_cell_with_overflow(
    pager: &mut Pager,
    rowid: i64,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let usable = pager.usable_size();
    let local_size = local_payload_size(payload.len(), usable);

    let mut cell = Vec::with_capacity(local_size + 18 + 4);
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(payload.len() as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    let n = varint::write_varint(rowid as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell.extend_from_slice(&payload[..local_size]);

    if local_size < payload.len() {
        let first = write_overflow_chain(pager, &payload[local_size..])?;
        cell.extend_from_slice(&first.to_be_bytes());
    }
    Ok(cell)
}

/// On-disk byte length of a table-leaf cell beginning at `offset`, including
/// the inline payload and the 4-byte overflow pointer when the payload spills.
fn table_leaf_cell_raw_len(data: &[u8], offset: usize, usable: u32) -> usize {
    let (payload_size, n1) = varint::read_varint(&data[offset..]);
    let (_, n2) = varint::read_varint(&data[offset + n1..]);
    let payload_size = payload_size as usize;
    let local = local_payload_size(payload_size, usable);
    let overflow = if local < payload_size { 4 } else { 0 };
    n1 + n2 + local + overflow
}

/// On-disk byte length of an index-leaf cell beginning at `offset`.
fn index_leaf_cell_raw_len(data: &[u8], offset: usize, usable: u32) -> usize {
    let (payload_size, n1) = varint::read_varint(&data[offset..]);
    let payload_size = payload_size as usize;
    let local = local_payload_size(payload_size, usable);
    let overflow = if local < payload_size { 4 } else { 0 };
    n1 + local + overflow
}

/// Build an index-leaf cell, spilling onto overflow pages when needed.
/// Layout: `varint(payload_size) <local payload bytes> [4-byte first
/// overflow page]`.
fn build_index_leaf_cell_with_overflow(pager: &mut Pager, payload: &[u8]) -> Result<Vec<u8>> {
    let usable = pager.usable_size();
    let local_size = local_payload_size(payload.len(), usable);

    let mut cell = Vec::with_capacity(local_size + 9 + 4);
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(payload.len() as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell.extend_from_slice(&payload[..local_size]);

    if local_size < payload.len() {
        let first = write_overflow_chain(pager, &payload[local_size..])?;
        cell.extend_from_slice(&first.to_be_bytes());
    }
    Ok(cell)
}

fn build_table_interior_cell(left_child: u32, rowid: i64) -> Vec<u8> {
    let mut cell = Vec::with_capacity(13);
    cell.extend_from_slice(&left_child.to_be_bytes());
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(rowid as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell
}

fn build_index_interior_cell(
    pager: &mut Pager,
    left_child: u32,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let usable = pager.usable_size();
    let local_size = local_payload_size(payload.len(), usable);

    let mut cell = Vec::with_capacity(local_size + 13 + 4);
    cell.extend_from_slice(&left_child.to_be_bytes());
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(payload.len() as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
    cell.extend_from_slice(&payload[..local_size]);

    if local_size < payload.len() {
        let first = write_overflow_chain(pager, &payload[local_size..])?;
        cell.extend_from_slice(&first.to_be_bytes());
    }
    Ok(cell)
}

pub fn btree_insert(pager: &mut Pager, root_page: u32, rowid: i64, record: &Record) -> Result<u32> {
    let payload = record.encode();
    let cell = build_table_leaf_cell_with_overflow(pager, rowid, &payload)?;
    insert_into_page(pager, root_page, rowid, &cell, true)?;
    // Roots are immutable: deepening relocates content to fresh children
    // and re-inits the original page in place, so the root number never
    // changes. We return it unconditionally for clarity at call sites.
    Ok(root_page)
}

/// Insert `cell` into the table btree rooted at `page_num`.
///
/// `is_root` marks the call as operating on a table's actual root page. Root
/// pages are immutable in this engine (SQLite-faithful): when a split would
/// otherwise create a new root, we instead deepen the tree in place via
/// [`balance_deeper_table_root`], relocating the old root's content to a fresh
/// child and re-initializing the original page as an interior page. This keeps
/// the catalog's stored root page number valid forever. The recursive call for
/// deeper interior nodes passes `is_root = false`, where a returned new page is
/// stitched in via [`update_child_pointer`].
fn insert_into_page(
    pager: &mut Pager,
    page_num: u32,
    rowid: i64,
    cell: &[u8],
    is_root: bool,
) -> Result<u32> {
    let page_data = pager.get_page(page_num)?.data.clone();
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(&page_data, offset)?;

    if header.page_type.is_leaf() {
        let result = try_insert_cell_into_leaf(pager, page_num, rowid, cell)?;
        match result {
            InsertResult::Ok => Ok(page_num),
            InsertResult::Split {
                new_page,
                median_rowid,
            } => {
                if is_root {
                    // The root must keep its page number; deepen in place.
                    balance_deeper_table_root(pager, page_num, new_page, median_rowid)?;
                    Ok(page_num)
                } else {
                    let new_root = pager.allocate_page()?;
                    {
                        let root_data = &mut pager.get_page_mut(new_root)?.data;
                        init_interior_page(root_data, new_root, new_page);
                    }
                    let interior_cell = build_table_interior_cell(page_num, median_rowid);
                    insert_cell_into_interior(pager, new_root, &interior_cell)?;
                    Ok(new_root)
                }
            }
        }
    } else {
        let pointers =
            read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);
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
                    let int_result =
                        try_insert_cell_into_interior(pager, page_num, &interior_cell, new_page)?;
                    match int_result {
                        InsertResult::Ok => Ok(page_num),
                        InsertResult::Split {
                            new_page: new_int_page,
                            median_rowid: med,
                        } => {
                            if is_root {
                                balance_deeper_table_root(pager, page_num, new_int_page, med)?;
                                Ok(page_num)
                            } else {
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
            }
        } else {
            let new_child_root = insert_into_page(pager, child_page, rowid, cell, false)?;
            if new_child_root != child_page {
                update_child_pointer(pager, page_num, child_page, new_child_root)?;
            }
            Ok(page_num)
        }
    }
}

/// Keep `root_page` as the b-tree root while accommodating a split. The
/// existing root content moves to a freshly allocated `new_left`, and the
/// root is re-initialized as an interior page pointing to (`new_left`,
/// `new_right_page`). Used for page 1 (sqlite_schema) which must remain
/// the schema's root forever.
fn balance_deeper_table_root(
    pager: &mut Pager,
    root_page: u32,
    new_right_page: u32,
    median_rowid: i64,
) -> Result<()> {
    let new_left = pager.allocate_page()?;
    copy_table_page_content(pager, root_page, new_left)?;
    {
        let root_data = &mut pager.get_page_mut(root_page)?.data;
        init_interior_page(root_data, root_page, new_right_page);
    }
    let interior_cell = build_table_interior_cell(new_left, median_rowid);
    insert_cell_into_interior(pager, root_page, &interior_cell)?;
    Ok(())
}

/// Copy the b-tree content of `src` to `dst`. Cells are reparsed and
/// rewritten so the offset shift between page 1 (offset 100) and a regular
/// page (offset 0) is handled transparently. Supports both leaf and
/// interior table pages.
fn copy_table_page_content(pager: &mut Pager, src: u32, dst: u32) -> Result<()> {
    let usable = pager.usable_size();
    let src_data = pager.get_page(src)?.data.clone();
    let src_offset = btree_header_offset(src);
    let header = parse_btree_header(&src_data, src_offset)?;
    let pointers = read_cell_pointers(
        &src_data,
        src_offset + header.header_size(),
        header.cell_count,
    );

    match header.page_type {
        PageType::LeafTable => {
            // Relocate raw cell bytes verbatim so overflow chains survive.
            let mut cells = Vec::with_capacity(pointers.len());
            for &ptr in &pointers {
                let cell_start = ptr as usize;
                let (_, n1) = varint::read_varint(&src_data[cell_start..]);
                let (rowid, _) = varint::read_varint(&src_data[cell_start + n1..]);
                let raw_len = table_leaf_cell_raw_len(&src_data, cell_start, usable);
                let raw = src_data[cell_start..cell_start + raw_len].to_vec();
                cells.push((rowid as i64, raw));
            }
            {
                let data = &mut pager.get_page_mut(dst)?.data;
                init_leaf_page(data, dst);
            }
            rewrite_leaf_page(pager, dst, &cells)?;
        }
        PageType::InteriorTable => {
            let right_child = header
                .right_most_pointer
                .ok_or_else(|| StorageError::Other("interior page missing right_most".into()))?;
            let mut cells: Vec<Vec<u8>> = Vec::with_capacity(pointers.len());
            for &ptr in &pointers {
                let ic = parse_table_interior_cell(&src_data, ptr as usize);
                cells.push(build_table_interior_cell(ic.left_child_page, ic.rowid));
            }
            {
                let data = &mut pager.get_page_mut(dst)?.data;
                init_interior_page(data, dst, right_child);
            }
            for cell in cells {
                insert_cell_into_interior(pager, dst, &cell)?;
            }
        }
        other => {
            return Err(StorageError::Other(format!(
                "copy_table_page_content: unsupported page type {other:?}"
            )));
        }
    }
    Ok(())
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
    // With overflow spilling, a built leaf cell can never exceed
    // `max_local + varint headers + 4-byte overflow ptr`, which always fits
    // an empty page. If this fires, the cell was built without overflow.
    debug_assert!(
        cell.len() + 2 <= pager.usable_size() as usize - btree_header_offset(page_num) - 8,
        "inline leaf cell of {} bytes does not fit an empty page",
        cell.len()
    );
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
            let (_, n1) = varint::read_varint(&data[ptr as usize..]);
            let (existing_rowid, _) = varint::read_varint(&data[ptr as usize + n1..]);
            if rowid <= existing_rowid as i64 {
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
        split_leaf(pager, page_num, rowid, cell)
    }
}

/// Choose a split index for a sorted list of cell sizes so that the left
/// prefix holds roughly half of the total on-page footprint (cell bytes plus
/// the 2-byte cell-pointer each). At least one cell is kept on each side.
fn size_balanced_split(sizes: impl Iterator<Item = usize>) -> usize {
    let footprints: Vec<usize> = sizes.map(|s| s + 2).collect();
    let total: usize = footprints.iter().sum();
    let half = total / 2;
    let mut acc = 0usize;
    let mut mid = 0usize;
    for (i, &f) in footprints.iter().enumerate() {
        acc += f;
        if acc >= half {
            mid = i + 1;
            break;
        }
    }
    // Clamp so neither side is empty.
    mid.clamp(1, footprints.len().saturating_sub(1)).max(1)
}

fn split_leaf(
    pager: &mut Pager,
    page_num: u32,
    new_rowid: i64,
    new_cell: &[u8],
) -> Result<InsertResult> {
    let usable = pager.usable_size();

    let page_data = pager.get_page(page_num)?.data.clone();
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(&page_data, offset)?;
    let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);

    // Relocate cells verbatim (raw bytes, including any trailing overflow
    // pointer) so existing overflow chains are preserved untouched.
    let mut cells: Vec<(i64, Vec<u8>)> = Vec::new();
    for &ptr in &pointers {
        let cell_start = ptr as usize;
        let (rowid, _) = {
            let (_, n1) = varint::read_varint(&page_data[cell_start..]);
            let (rid, _) = varint::read_varint(&page_data[cell_start + n1..]);
            (rid as i64, ())
        };
        let raw_len = table_leaf_cell_raw_len(&page_data, cell_start, usable);
        let raw_cell = page_data[cell_start..cell_start + raw_len].to_vec();
        cells.push((rowid, raw_cell));
    }
    cells.push((new_rowid, new_cell.to_vec()));
    cells.sort_by_key(|(rowid, _)| *rowid);

    // Split by accumulated cell size, not by count: a single overflow cell
    // can nearly fill a page, so a naive count-based midpoint could leave one
    // half larger than a page. Pick the smallest left prefix whose byte total
    // reaches half the combined size, keeping at least one cell on each side.
    let mid = size_balanced_split(cells.iter().map(|(_, c)| c.len()));
    let left_cells = &cells[..mid];
    let right_cells = &cells[mid..];
    let median_rowid = left_cells.last().map(|(r, _)| *r).unwrap_or(0);

    rewrite_leaf_page(pager, page_num, left_cells)?;

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

    let clear_start = offset;
    data[clear_start..page_size].fill(0);

    init_leaf_page(data, page_num);

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

fn insert_cell_into_interior(pager: &mut Pager, page_num: u32, cell: &[u8]) -> Result<()> {
    let page = pager.get_page_mut(page_num)?;
    let data = &mut page.data;
    let offset = btree_header_offset(page_num);
    let header = parse_btree_header(data, offset)?;

    let ptr_area_start = offset + header.header_size();
    let content_start = header.cell_content_offset as usize;

    let new_content_start = content_start - cell.len();
    data[new_content_start..new_content_start + cell.len()].copy_from_slice(cell);

    let ptr_pos = ptr_area_start + header.cell_count as usize * 2;
    let ptr_val = new_content_start as u16;
    data[ptr_pos..ptr_pos + 2].copy_from_slice(&ptr_val.to_be_bytes());

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

        if insert_pos == pointers.len() {
            data[offset + 8..offset + 12].copy_from_slice(&new_right_child.to_be_bytes());
        } else {
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
        let pointers = read_cell_pointers(data, ptr_area_start, header.cell_count);
        let old_right = header.right_most_pointer.unwrap();

        let mut all_cells: Vec<(i64, Vec<u8>, u32)> = Vec::new();
        for (i, &ptr) in pointers.iter().enumerate() {
            let ic = parse_table_interior_cell(data, ptr as usize);
            let raw = build_table_interior_cell(ic.left_child_page, ic.rowid);
            let right = if i + 1 < pointers.len() {
                parse_table_interior_cell(data, pointers[i + 1] as usize).left_child_page
            } else {
                old_right
            };
            all_cells.push((ic.rowid, raw, right));
        }

        let new_ic = parse_table_interior_cell(cell, 0);
        let new_raw = cell.to_vec();
        all_cells.push((new_ic.rowid, new_raw, new_right_child));
        all_cells.sort_by_key(|(rowid, _, _)| *rowid);

        let mid = all_cells.len() / 2;
        let median_rowid = all_cells[mid].0;

        let left_cells = &all_cells[..mid];
        let promoted = &all_cells[mid];
        let right_cells = &all_cells[mid + 1..];

        {
            let page = pager.get_page_mut(page_num)?;
            let data = &mut page.data;
            let off = btree_header_offset(page_num);
            data[off..page_size].fill(0);
            init_interior_page(
                data,
                page_num,
                parse_table_interior_cell(&promoted.1, 0).left_child_page,
            );

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

    if header.right_most_pointer == Some(old_child) {
        data[offset + 8..offset + 12].copy_from_slice(&new_child.to_be_bytes());
        return Ok(());
    }

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

pub fn btree_create_table(pager: &mut Pager) -> Result<u32> {
    let page_num = pager.allocate_page()?;
    {
        let page = pager.get_page_mut(page_num)?;
        init_leaf_page(&mut page.data, page_num);
    }
    Ok(page_num)
}

pub fn btree_create_index(pager: &mut Pager) -> Result<u32> {
    let page_num = pager.allocate_page()?;
    {
        let page = pager.get_page_mut(page_num)?;
        init_leaf_index_page(&mut page.data, page_num);
    }
    Ok(page_num)
}

pub fn btree_index_insert(pager: &mut Pager, root_page: u32, key: &Record) -> Result<u32> {
    let payload = key.encode();
    let cell = build_index_leaf_cell_with_overflow(pager, &payload)?;
    index_insert_into_page(pager, root_page, key, &cell, true)?;
    // Index roots are immutable, just like table roots.
    Ok(root_page)
}

fn index_insert_into_page(
    pager: &mut Pager,
    page_num: u32,
    key: &Record,
    cell: &[u8],
    is_root: bool,
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
                // The median separator is the largest key remaining on the
                // left page (page_num after the split rewrote it as the left
                // half). Reassemble its payload in case it overflows.
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
                let sep_payload = reassemble_payload(
                    pager,
                    &last_cell.payload,
                    last_cell.payload_size,
                    last_cell.overflow_page,
                )?;

                if is_root {
                    // Keep the root page number stable: relocate the old root
                    // (now the left half) to a fresh child and rebuild the
                    // root as an interior index page.
                    let new_left = pager.allocate_page()?;
                    copy_index_page_content(pager, page_num, new_left)?;
                    {
                        let root_data = &mut pager.get_page_mut(page_num)?.data;
                        init_interior_index_page(root_data, page_num, new_page);
                    }
                    let interior_cell = build_index_interior_cell(pager, new_left, &sep_payload)?;
                    insert_cell_into_interior(pager, page_num, &interior_cell)?;
                    Ok(page_num)
                } else {
                    let new_root = pager.allocate_page()?;
                    {
                        let root_data = &mut pager.get_page_mut(new_root)?.data;
                        init_interior_index_page(root_data, new_root, new_page);
                    }
                    let interior_cell = build_index_interior_cell(pager, page_num, &sep_payload)?;
                    insert_cell_into_interior(pager, new_root, &interior_cell)?;
                    Ok(new_root)
                }
            }
        }
    } else {
        let pointers =
            read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);
        let mut child_page = header.right_most_pointer.unwrap();

        for i in 0..header.cell_count as usize {
            let cell_offset = pointers[i] as usize;
            let ic = parse_index_interior_cell(&page_data, cell_offset, usable)?;
            let ic_payload =
                reassemble_payload(pager, &ic.payload, ic.payload_size, ic.overflow_page)?;
            let ic_record = Record::decode(&ic_payload)?;
            if compare_records(key, &ic_record) != std::cmp::Ordering::Greater {
                child_page = ic.left_child_page;
                break;
            }
        }

        let new_child_root = index_insert_into_page(pager, child_page, key, cell, false)?;
        if new_child_root != child_page {
            update_child_pointer(pager, page_num, child_page, new_child_root)?;
        }
        Ok(page_num)
    }
}

/// Copy the b-tree content of an index page `src` to `dst`. Cells are
/// relocated verbatim (raw bytes, preserving overflow pointers) so the
/// offset shift between page 1 and a regular page is irrelevant for index
/// roots (which are never page 1). Supports leaf and interior index pages.
fn copy_index_page_content(pager: &mut Pager, src: u32, dst: u32) -> Result<()> {
    let usable = pager.usable_size();
    let src_data = pager.get_page(src)?.data.clone();
    let src_offset = btree_header_offset(src);
    let header = parse_btree_header(&src_data, src_offset)?;
    let pointers = read_cell_pointers(
        &src_data,
        src_offset + header.header_size(),
        header.cell_count,
    );

    match header.page_type {
        PageType::LeafIndex => {
            let mut cells: Vec<(i64, Vec<u8>)> = Vec::with_capacity(pointers.len());
            for &ptr in &pointers {
                let cell_start = ptr as usize;
                let raw_len = index_leaf_cell_raw_len(&src_data, cell_start, usable);
                let raw = src_data[cell_start..cell_start + raw_len].to_vec();
                cells.push((0, raw));
            }
            {
                let data = &mut pager.get_page_mut(dst)?.data;
                init_leaf_index_page(data, dst);
            }
            rewrite_index_leaf_page(pager, dst, &cells)?;
        }
        PageType::InteriorIndex => {
            let right_child = header
                .right_most_pointer
                .ok_or_else(|| StorageError::Other("interior index missing right_most".into()))?;
            let mut cells: Vec<Vec<u8>> = Vec::with_capacity(pointers.len());
            for &ptr in &pointers {
                let cell_start = ptr as usize;
                let raw_len = index_interior_cell_raw_len(&src_data, cell_start, usable);
                cells.push(src_data[cell_start..cell_start + raw_len].to_vec());
            }
            {
                let data = &mut pager.get_page_mut(dst)?.data;
                init_interior_index_page(data, dst, right_child);
            }
            for cell in cells {
                insert_cell_into_interior(pager, dst, &cell)?;
            }
        }
        other => {
            return Err(StorageError::Other(format!(
                "copy_index_page_content: unsupported page type {other:?}"
            )));
        }
    }
    Ok(())
}

/// On-disk byte length of an index-interior cell beginning at `offset`
/// (4-byte left child + varint(size) + local payload + optional overflow ptr).
fn index_interior_cell_raw_len(data: &[u8], offset: usize, usable: u32) -> usize {
    let (payload_size, n1) = varint::read_varint(&data[offset + 4..]);
    let payload_size = payload_size as usize;
    let local = local_payload_size(payload_size, usable);
    let overflow = if local < payload_size { 4 } else { 0 };
    4 + n1 + local + overflow
}

fn try_insert_cell_into_index_leaf(
    pager: &mut Pager,
    page_num: u32,
    key: &Record,
    cell: &[u8],
) -> Result<InsertResult> {
    let usable = pager.usable_size();
    let offset = btree_header_offset(page_num);

    // Determine the insert position first, reassembling any overflowing
    // payloads via the pager before taking the mutable page borrow.
    let snapshot = pager.get_page(page_num)?.data.clone();
    let header = parse_btree_header(&snapshot, offset)?;
    let ptr_area_start = offset + header.header_size();
    let ptr_area_end = ptr_area_start + header.cell_count as usize * 2;
    let content_start = header.cell_content_offset as usize;

    let space_needed = 2 + cell.len();
    let free_space = content_start - ptr_area_end;

    if space_needed <= free_space {
        let pointers = read_cell_pointers(&snapshot, ptr_area_start, header.cell_count);

        let mut insert_pos = pointers.len();
        for (i, &ptr) in pointers.iter().enumerate() {
            let existing_cell = parse_index_leaf_cell(&snapshot, ptr as usize, usable)?;
            let existing_payload = reassemble_payload(
                pager,
                &existing_cell.payload,
                existing_cell.payload_size,
                existing_cell.overflow_page,
            )?;
            let existing_record = Record::decode(&existing_payload)?;
            if compare_records(key, &existing_record) != std::cmp::Ordering::Greater {
                insert_pos = i;
                break;
            }
        }

        let page = pager.get_page_mut(page_num)?;
        let data = &mut page.data;
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

    // Reassemble each key's full payload for the sort comparison, but
    // relocate the raw cell bytes verbatim so overflow chains are preserved.
    let mut cells: Vec<(Record, Vec<u8>)> = Vec::new();
    for &ptr in &pointers {
        let cell_start = ptr as usize;
        let c = parse_index_leaf_cell(&page_data, cell_start, usable)?;
        let full = reassemble_payload(pager, &c.payload, c.payload_size, c.overflow_page)?;
        let record = Record::decode(&full)?;
        let raw_len = index_leaf_cell_raw_len(&page_data, cell_start, usable);
        let raw = page_data[cell_start..cell_start + raw_len].to_vec();
        cells.push((record, raw));
    }
    cells.push((new_key.clone(), new_cell.to_vec()));
    cells.sort_by(|(a, _), (b, _)| compare_records(a, b));

    let mid = size_balanced_split(cells.iter().map(|(_, raw)| raw.len()));
    let left_cells: Vec<(i64, Vec<u8>)> = cells[..mid]
        .iter()
        .map(|(_, raw)| (0, raw.clone()))
        .collect();
    let right_cells: Vec<(i64, Vec<u8>)> = cells[mid..]
        .iter()
        .map(|(_, raw)| (0, raw.clone()))
        .collect();

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

fn rewrite_index_leaf_page(
    pager: &mut Pager,
    page_num: u32,
    cells: &[(i64, Vec<u8>)],
) -> Result<()> {
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

/// Descend an index btree to the leaf where `key` routes (same rule as
/// insertion: the first separator `>= key` sends the search to its left
/// child), returning the leaf page number.
fn find_index_leaf(pager: &mut Pager, page: u32, key: &Record) -> Result<u32> {
    let data = pager.get_page(page)?.data.clone();
    let offset = btree_header_offset(page);
    let header = parse_btree_header(&data, offset)?;
    if header.page_type == PageType::LeafIndex {
        return Ok(page);
    }
    let usable = pager.usable_size();
    let pointers = read_cell_pointers(&data, offset + header.header_size(), header.cell_count);
    let mut child = header
        .right_most_pointer
        .ok_or_else(|| StorageError::Corrupt("interior index missing rightmost pointer".into()))?;
    for &ptr in &pointers {
        let ic = parse_index_interior_cell(&data, ptr as usize, usable)?;
        let ic_payload = reassemble_payload(pager, &ic.payload, ic.payload_size, ic.overflow_page)?;
        let ic_record = Record::decode(&ic_payload)?;
        if compare_records(key, &ic_record) != std::cmp::Ordering::Greater {
            child = ic.left_child_page;
            break;
        }
    }
    find_index_leaf(pager, child, key)
}

/// Remove the first index-leaf cell matching `matches` from `leaf` in place:
/// rewrite just that page without the cell and free any overflow chain it
/// owned. Interior separators are deliberately left untouched — a delete only
/// shrinks a subtree's contents, so every separator stays a valid routing
/// bound (searches for the removed key route correctly and find nothing).
/// Returns whether a cell was removed.
fn index_leaf_delete_one(
    pager: &mut Pager,
    leaf: u32,
    matches: impl Fn(&Record) -> bool,
) -> Result<bool> {
    let usable = pager.usable_size();
    let page_data = pager.get_page(leaf)?.data.clone();
    let offset = btree_header_offset(leaf);
    let header = parse_btree_header(&page_data, offset)?;
    let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);

    let mut survivors: Vec<(i64, Vec<u8>)> = Vec::with_capacity(pointers.len());
    let mut freed: Vec<u32> = Vec::new();
    let mut found = false;
    for &ptr in &pointers {
        let cell_start = ptr as usize;
        let cell = parse_index_leaf_cell(&page_data, cell_start, usable)?;
        if !found {
            let full =
                reassemble_payload(pager, &cell.payload, cell.payload_size, cell.overflow_page)?;
            let rec = Record::decode(&full)?;
            if matches(&rec) {
                found = true;
                if let Some(first) = cell.overflow_page {
                    let n = cell.payload_size - cell.payload.len();
                    freed.extend(collect_overflow_pages(pager, first, n)?);
                }
                continue;
            }
        }
        let raw_len = index_leaf_cell_raw_len(&page_data, cell_start, usable);
        survivors.push((0, page_data[cell_start..cell_start + raw_len].to_vec()));
    }
    if !found {
        return Ok(false);
    }
    rewrite_index_leaf_page(pager, leaf, &survivors)?;
    for p in freed {
        pager.free_page(p);
    }
    Ok(true)
}

/// Delete one index entry equal to `key`, in place (O(log n)): route to its
/// leaf and rewrite that single page. See [`index_leaf_delete_one`].
pub fn btree_index_delete(pager: &mut Pager, root_page: u32, key: &Record) -> Result<()> {
    let leaf = find_index_leaf(pager, root_page, key)?;
    index_leaf_delete_one(pager, leaf, |rec| {
        compare_records(rec, key) == std::cmp::Ordering::Equal
    })?;
    Ok(())
}

/// Delete several index keys, each in place (O(k log n)) — one leaf rewrite
/// per key rather than a whole-tree rebuild.
pub fn btree_index_delete_many(pager: &mut Pager, root_page: u32, keys: &[Record]) -> Result<()> {
    for key in keys {
        let leaf = find_index_leaf(pager, root_page, key)?;
        index_leaf_delete_one(pager, leaf, |rec| {
            compare_records(rec, key) == std::cmp::Ordering::Equal
        })?;
    }
    Ok(())
}

/// Re-initialize the index root as an empty leaf and re-insert all `keys`
/// through the normal index-insert path, rebuilding a valid (possibly
/// multi-page) index tree with the root in place. The old tree's non-root
/// pages (interior, leaf, overflow) are reclaimed onto the freelist first so
/// the rebuild reuses them rather than orphaning them.
fn rebuild_index_btree(pager: &mut Pager, root_page: u32, keys: &[Record]) -> Result<()> {
    let old_pages = collect_index_tree_pages(pager, root_page)?;
    let page_size = pager.page_size() as usize;
    {
        let data = &mut pager.get_page_mut(root_page)?.data;
        let offset = btree_header_offset(root_page);
        data[offset..page_size].fill(0);
        init_leaf_index_page(data, root_page);
    }
    for p in old_pages {
        pager.free_page(p);
    }
    for key in keys {
        let payload = key.encode();
        let cell = build_index_leaf_cell_with_overflow(pager, &payload)?;
        index_insert_into_page(pager, root_page, key, &cell, true)?;
    }
    Ok(())
}

/// Delete the first record in the index btree whose leading
/// `prefix_len` values equal those of `prefix`. Used by WITHOUT ROWID
/// tables, where the PK columns are the leading prefix of the stored
/// record but the trailing payload columns vary per row.
pub fn btree_index_delete_by_prefix(
    pager: &mut Pager,
    root_page: u32,
    prefix: &Record,
    prefix_len: usize,
) -> Result<()> {
    let mut cursor = IndexCursor::new(pager, root_page);
    let entries = cursor.collect_all()?;

    let mut deleted = false;
    let mut survivors: Vec<Record> = Vec::with_capacity(entries.len());
    for rec in entries {
        if !deleted
            && compare_records_by_prefix(&rec, prefix, prefix_len) == std::cmp::Ordering::Equal
        {
            deleted = true;
            continue;
        }
        survivors.push(rec);
    }

    rebuild_index_btree(pager, root_page, &survivors)
}

/// Descend a table btree to the leaf where `rowid` routes, returning the leaf
/// page number (mirrors the routing in [`crate::btree::btree_row_exists`]).
fn find_table_leaf(pager: &mut Pager, page: u32, rowid: i64) -> Result<u32> {
    let data = pager.get_page(page)?.data.clone();
    let offset = btree_header_offset(page);
    let header = parse_btree_header(&data, offset)?;
    if header.page_type.is_leaf() {
        return Ok(page);
    }
    let pointers = read_cell_pointers(&data, offset + header.header_size(), header.cell_count);
    for &ptr in &pointers {
        let cell = parse_table_interior_cell(&data, ptr as usize);
        if rowid <= cell.rowid {
            return find_table_leaf(pager, cell.left_child_page, rowid);
        }
    }
    let right = header
        .right_most_pointer
        .ok_or_else(|| StorageError::Corrupt("interior table missing rightmost pointer".into()))?;
    find_table_leaf(pager, right, rowid)
}

/// Delete the row with `rowid` in place (O(log n)): route to its leaf and
/// rewrite that single page without the cell, freeing any overflow chain.
/// Returns whether a row was removed.
///
/// The leaf may become empty and interior separators are left as-is; both are
/// tolerated — a table separator is a rowid upper bound for its left subtree,
/// and a delete only lowers a subtree's max, so routing stays correct and an
/// empty leaf simply yields no rows on lookup / accepts inserts normally. See
/// [`btree_max_rowid`], which is robust to an empty rightmost leaf.
pub fn btree_delete_one(pager: &mut Pager, root_page: u32, rowid: i64) -> Result<bool> {
    let leaf = find_table_leaf(pager, root_page, rowid)?;
    let usable = pager.usable_size();
    let page_data = pager.get_page(leaf)?.data.clone();
    let offset = btree_header_offset(leaf);
    let header = parse_btree_header(&page_data, offset)?;
    let pointers = read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);

    let mut survivors: Vec<(i64, Vec<u8>)> = Vec::with_capacity(pointers.len());
    let mut freed: Vec<u32> = Vec::new();
    let mut found = false;
    for &ptr in &pointers {
        let cell_start = ptr as usize;
        let cell = parse_table_leaf_cell(&page_data, cell_start, usable)?;
        if cell.rowid == rowid {
            found = true;
            if let Some(first) = cell.overflow_page {
                let n = cell.payload_size - cell.payload.len();
                freed.extend(collect_overflow_pages(pager, first, n)?);
            }
            continue;
        }
        let raw_len = table_leaf_cell_raw_len(&page_data, cell_start, usable);
        survivors.push((cell.rowid, page_data[cell_start..cell_start + raw_len].to_vec()));
    }
    if !found {
        return Ok(false);
    }
    rewrite_leaf_page(pager, leaf, &survivors)?;
    for p in freed {
        pager.free_page(p);
    }
    Ok(true)
}

/// Delete the row with `rowid` from the table btree rooted at `root_page`
/// (in place — see [`btree_delete_one`]).
pub fn btree_delete(pager: &mut Pager, root_page: u32, rowid: i64) -> Result<()> {
    btree_delete_one(pager, root_page, rowid)?;
    Ok(())
}

/// Delete every rowid in `rowids`, each in place (O(k log n)) — one leaf
/// rewrite per row rather than a whole-tree rebuild.
pub fn btree_delete_many(pager: &mut Pager, root_page: u32, rowids: &[i64]) -> Result<()> {
    for &rowid in rowids {
        btree_delete_one(pager, root_page, rowid)?;
    }
    Ok(())
}

/// Walk the overflow chain starting at `first` and return its page numbers.
fn collect_overflow_pages(pager: &mut Pager, first: u32, mut remaining: usize) -> Result<Vec<u32>> {
    let usable = pager.usable_size() as usize;
    let per_page = usable - 4;
    let mut pages = Vec::new();
    let mut page = first;
    while remaining > 0 && page != 0 {
        pages.push(page);
        let data = pager.get_page(page)?.data.clone();
        let next = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        remaining = remaining.saturating_sub(per_page);
        page = next;
    }
    Ok(pages)
}

/// Collect every page belonging to the table btree rooted at `root_page`,
/// excluding the root itself: interior pages, leaf pages, and any overflow
/// chains. Used to reclaim a tree's pages before a rebuild so they are reused
/// rather than orphaned (which would leave `PRAGMA integrity_check`-rejecting
/// "never used" pages).
///
/// Retained for a future whole-tree compaction path; table deletes are now
/// in-place (see [`btree_delete_one`]) and no longer rebuild.
#[allow(dead_code)]
fn collect_table_tree_pages(pager: &mut Pager, root_page: u32) -> Result<Vec<u32>> {
    let usable = pager.usable_size();
    let mut out = Vec::new();
    let mut stack = vec![root_page];
    while let Some(page_num) = stack.pop() {
        let data = pager.get_page(page_num)?.data.clone();
        let offset = btree_header_offset(page_num);
        let header = parse_btree_header(&data, offset)?;
        let pointers = read_cell_pointers(&data, offset + header.header_size(), header.cell_count);
        match header.page_type {
            PageType::LeafTable => {
                for &ptr in &pointers {
                    let cell = parse_table_leaf_cell(&data, ptr as usize, usable)?;
                    if let Some(first) = cell.overflow_page {
                        let n = cell.payload_size - cell.payload.len();
                        out.extend(collect_overflow_pages(pager, first, n)?);
                    }
                }
            }
            PageType::InteriorTable => {
                for &ptr in &pointers {
                    let ic = parse_table_interior_cell(&data, ptr as usize);
                    if ic.left_child_page != root_page {
                        stack.push(ic.left_child_page);
                    }
                }
                if let Some(right) = header.right_most_pointer {
                    if right != root_page {
                        stack.push(right);
                    }
                }
            }
            other => {
                return Err(StorageError::Other(format!(
                    "collect_table_tree_pages: unexpected page type {other:?}"
                )));
            }
        }
        if page_num != root_page {
            out.push(page_num);
        }
    }
    Ok(out)
}

/// Like [`collect_table_tree_pages`] but for an index btree.
fn collect_index_tree_pages(pager: &mut Pager, root_page: u32) -> Result<Vec<u32>> {
    let usable = pager.usable_size();
    let mut out = Vec::new();
    let mut stack = vec![root_page];
    while let Some(page_num) = stack.pop() {
        let data = pager.get_page(page_num)?.data.clone();
        let offset = btree_header_offset(page_num);
        let header = parse_btree_header(&data, offset)?;
        let pointers = read_cell_pointers(&data, offset + header.header_size(), header.cell_count);
        match header.page_type {
            PageType::LeafIndex => {
                for &ptr in &pointers {
                    let cell = parse_index_leaf_cell(&data, ptr as usize, usable)?;
                    if let Some(first) = cell.overflow_page {
                        let n = cell.payload_size - cell.payload.len();
                        out.extend(collect_overflow_pages(pager, first, n)?);
                    }
                }
            }
            PageType::InteriorIndex => {
                for &ptr in &pointers {
                    let ic = parse_index_interior_cell(&data, ptr as usize, usable)?;
                    if let Some(first) = ic.overflow_page {
                        let n = ic.payload_size - ic.payload.len();
                        out.extend(collect_overflow_pages(pager, first, n)?);
                    }
                    if ic.left_child_page != root_page {
                        stack.push(ic.left_child_page);
                    }
                }
                if let Some(right) = header.right_most_pointer {
                    if right != root_page {
                        stack.push(right);
                    }
                }
            }
            other => {
                return Err(StorageError::Other(format!(
                    "collect_index_tree_pages: unexpected page type {other:?}"
                )));
            }
        }
        if page_num != root_page {
            out.push(page_num);
        }
    }
    Ok(out)
}

/// Re-initialize the table root as an empty leaf and re-insert all `rows`
/// (already ordered by rowid) through the normal insert path so the tree is
/// rebuilt into a valid (possibly multi-page) shape with the root in place.
///
/// All survivor payloads are already materialized in `rows` (owned `Vec`s), so
/// before re-initializing we reclaim every non-root page of the old tree onto
/// the pager's freelist. The rebuild then reuses those pages instead of growing
/// the file, keeping the database free of orphaned "never used" pages.
///
/// Retained for a future whole-tree compaction path; table deletes are now
/// in-place (see [`btree_delete_one`]) and no longer rebuild.
#[allow(dead_code)]
fn rebuild_table_btree(pager: &mut Pager, root_page: u32, rows: &[(i64, Vec<u8>)]) -> Result<()> {
    let old_pages = collect_table_tree_pages(pager, root_page)?;
    let page_size = pager.page_size() as usize;
    {
        let data = &mut pager.get_page_mut(root_page)?.data;
        let offset = btree_header_offset(root_page);
        data[offset..page_size].fill(0);
        init_leaf_page(data, root_page);
    }
    for p in old_pages {
        pager.free_page(p);
    }
    for (rowid, payload) in rows {
        let cell = build_table_leaf_cell_with_overflow(pager, *rowid, payload)?;
        insert_into_page(pager, root_page, *rowid, &cell, true)?;
    }
    Ok(())
}

pub fn delete_schema_entries(pager: &mut Pager, name: &str) -> Result<()> {
    let mut cursor = BTreeCursor::new(pager, 1);
    let mut rowids_to_delete = Vec::new();
    let mut has_row = cursor.first()?;
    while has_row {
        let current = cursor.current()?;
        let matches = current.record.values.get(1).is_some_and(|v| {
            if let Value::Text(s) = v {
                s.eq_ignore_ascii_case(name)
            } else {
                false
            }
        }) || current.record.values.get(2).is_some_and(|v| {
            if let Value::Text(s) = v {
                s.eq_ignore_ascii_case(name)
            } else {
                false
            }
        });
        if matches {
            rowids_to_delete.push(current.rowid);
        }
        has_row = cursor.next()?;
    }
    for rowid in rowids_to_delete {
        btree_delete(pager, 1, rowid)?;
    }
    Ok(())
}

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

    let max_rowid = crate::btree::btree_max_rowid(pager, 1)?;
    let new_rowid = max_rowid + 1;
    let new_root = btree_insert(pager, 1, new_rowid, &record)?;

    debug_assert_eq!(
        new_root, 1,
        "sqlite_schema root must remain page 1 after insert (deepening should keep it)"
    );

    Ok(())
}

/// Like [`insert_schema_entry`] but writes SQL `NULL`. Used for implicit
/// `sqlite_autoindex_*` rows which, matching SQLite, carry no CREATE
/// statement; their columns are re-derived from the owning table on load.
pub fn insert_schema_entry_null_sql(
    pager: &mut Pager,
    entry_type: &str,
    name: &str,
    tbl_name: &str,
    rootpage: u32,
) -> Result<()> {
    let record = Record {
        values: vec![
            Value::Text(entry_type.to_string()),
            Value::Text(name.to_string()),
            Value::Text(tbl_name.to_string()),
            Value::Integer(rootpage as i64),
            Value::Null,
        ],
    };

    let max_rowid = crate::btree::btree_max_rowid(pager, 1)?;
    let new_rowid = max_rowid + 1;
    let new_root = btree_insert(pager, 1, new_rowid, &record)?;

    debug_assert_eq!(
        new_root, 1,
        "sqlite_schema root must remain page 1 after insert (deepening should keep it)"
    );

    Ok(())
}
