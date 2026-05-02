use crate::btree::{
    BTreeCursor, IndexCursor, PageType, btree_header_offset, build_index_leaf_cell,
    compare_records, compare_records_by_prefix, init_interior_index_page, init_interior_page,
    init_leaf_index_page, init_leaf_page, parse_btree_header, parse_index_interior_cell,
    parse_index_leaf_cell, parse_table_interior_cell, parse_table_leaf_cell, read_cell_pointers,
    write_cell_pointers,
};
use crate::codec::{Record, Value};
use crate::error::{Result, StorageError};
use crate::pager::Pager;
use crate::varint;

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

fn build_table_interior_cell(left_child: u32, rowid: i64) -> Vec<u8> {
    let mut cell = Vec::with_capacity(13);
    cell.extend_from_slice(&left_child.to_be_bytes());
    let mut tmp = [0u8; 9];
    let n = varint::write_varint(rowid as u64, &mut tmp);
    cell.extend_from_slice(&tmp[..n]);
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

pub fn btree_insert(pager: &mut Pager, root_page: u32, rowid: i64, record: &Record) -> Result<u32> {
    let payload = record.encode();
    let cell = build_table_leaf_cell(rowid, &payload);
    insert_into_page(pager, root_page, rowid, &cell)
}

fn insert_into_page(pager: &mut Pager, page_num: u32, rowid: i64, cell: &[u8]) -> Result<u32> {
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
                if page_num == 1 {
                    // Page 1 must remain the schema root; deepen instead.
                    balance_deeper_table_root(pager, 1, new_page, median_rowid)?;
                    Ok(1)
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
                            if page_num == 1 {
                                balance_deeper_table_root(pager, 1, new_int_page, med)?;
                                Ok(1)
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
            let new_child_root = insert_into_page(pager, child_page, rowid, cell)?;
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
    let pointers =
        read_cell_pointers(&src_data, src_offset + header.header_size(), header.cell_count);

    match header.page_type {
        PageType::LeafTable => {
            let mut cells = Vec::with_capacity(pointers.len());
            for &ptr in &pointers {
                let c = parse_table_leaf_cell(&src_data, ptr as usize, usable)?;
                let raw = build_table_leaf_cell(c.rowid, &c.payload);
                cells.push((c.rowid, raw));
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

    let mut cells: Vec<(i64, Vec<u8>)> = Vec::new();
    for &ptr in &pointers {
        let cell_start = ptr as usize;
        let c = parse_table_leaf_cell(&page_data, cell_start, usable)?;
        let raw_cell = build_table_leaf_cell(c.rowid, &c.payload);
        cells.push((c.rowid, raw_cell));
    }
    cells.push((new_rowid, new_cell.to_vec()));
    cells.sort_by_key(|(rowid, _)| *rowid);

    let mid = cells.len() / 2;
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
        let pointers =
            read_cell_pointers(&page_data, offset + header.header_size(), header.cell_count);
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
    let mut remaining_cells: Vec<(i64, Vec<u8>)> = Vec::with_capacity(entries.len());
    for rec in entries {
        if !deleted
            && compare_records_by_prefix(&rec, prefix, prefix_len) == std::cmp::Ordering::Equal
        {
            deleted = true;
            continue;
        }
        let payload = rec.encode();
        remaining_cells.push((0, build_index_leaf_cell(&payload)));
    }

    rewrite_index_leaf_page(pager, root_page, &remaining_cells)?;
    Ok(())
}

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

    rewrite_leaf_page(
        pager,
        root_page,
        &rows
            .iter()
            .map(|(r, p)| (*r, build_table_leaf_cell(*r, p)))
            .collect::<Vec<_>>(),
    )?;

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
