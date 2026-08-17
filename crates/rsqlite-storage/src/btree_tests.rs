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

            let schema = read_schema(&mut pager).unwrap();
            assert!(!schema.is_empty(), "schema should have at least one entry");
            let table_entry = schema.iter().find(|e| e.name == "users").unwrap();
            assert_eq!(table_entry.entry_type, "table");
            assert_eq!(table_entry.tbl_name, "users");
            assert!(table_entry.rootpage > 0);

            let root = table_entry.rootpage;
            let mut cursor = BTreeCursor::new(&mut pager, root);
            let rows = cursor.collect_all().unwrap();
            assert_eq!(rows.len(), 3);

            assert_eq!(rows[0].rowid, 1);
            let vals = &rows[0].record.values;
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

            let tables: Vec<_> = schema.iter().filter(|e| e.entry_type == "table").collect();
            let indexes: Vec<_> = schema.iter().filter(|e| e.entry_type == "index").collect();

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

            for (i, row) in rows.iter().enumerate() {
                assert_eq!(row.rowid, (i + 1) as i64);
            }

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

    for &id in &[3i64, 1, 4, 1, 5, 9, 2, 6] {
        let record = Record {
            values: vec![Value::Integer(id * 10)],
        };
        btree_insert(&mut pager, 1, id, &record).unwrap();
    }

    let mut cursor = BTreeCursor::new(&mut pager, 1);
    let rows = cursor.collect_all().unwrap();
    assert_eq!(rows.len(), 8);

    let rowids: Vec<i64> = rows.iter().map(|r| r.rowid).collect();
    assert_eq!(rowids, vec![1, 1, 2, 3, 4, 5, 6, 9]);
}

#[test]
fn insert_triggers_page_split() {
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();

    let table_root = btree_create_table(&mut pager).unwrap();
    let mut root = table_root;
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

    assert!(
        pager.page_count() > 2,
        "expected page splits for 50 large rows, got {} pages",
        pager.page_count()
    );

    let mut cursor = BTreeCursor::new(&mut pager, root);
    let rows = cursor.collect_all().unwrap();
    assert_eq!(rows.len(), 50);

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

    let vfs = rsqlite_vfs::native::NativeVfs::new();
    let mut pager = Pager::create(&vfs, test_db).unwrap();

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

// ── Index / rowid seek primitives (Phase 1) ──

/// Deterministic LCG shuffle so tests exercise real interior-page layouts
/// without depending on `rand` (and without `Math.random`-style nondeterminism).
fn lcg_shuffle(items: &mut [i64], mut seed: u64) {
    for i in (1..items.len()).rev() {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (seed >> 33) as usize % (i + 1);
        items.swap(i, j);
    }
}

#[test]
fn index_seek_matches_linear_scan() {
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();
    let mut root = btree_create_index(&mut pager).unwrap();

    // 800 entries [key, rowid] inserted shuffled → forces multiple b-tree levels.
    let n = 800i64;
    let mut order: Vec<i64> = (0..n).collect();
    lcg_shuffle(&mut order, 12345);
    for &k in &order {
        let rec = Record {
            values: vec![Value::Integer(k), Value::Integer(k + 1000)],
        };
        root = btree_index_insert(&mut pager, root, &rec).unwrap();
    }

    let all = {
        let mut c = IndexCursor::new(&mut pager, root);
        c.collect_all().unwrap()
    };
    assert_eq!(all.len(), n as usize);

    // Every key is found, at the same first entry a linear scan would return.
    for k in 0..n {
        let prefix = Record {
            values: vec![Value::Integer(k)],
        };
        let mut cur = IndexCursor::new(&mut pager, root);
        assert!(
            cur.seek_first_with_prefix(&prefix).unwrap(),
            "seek failed for key {k}"
        );
        let rec = cur.current().unwrap();
        let linear = all
            .iter()
            .find(|r| r.values[0] == Value::Integer(k))
            .unwrap();
        assert_eq!(rec.values, linear.values, "seek/linear mismatch at {k}");
    }

    // Keys outside the tree return false (below, above, far above).
    for k in [-1i64, n, n + 50, 100_000] {
        let prefix = Record {
            values: vec![Value::Integer(k)],
        };
        let mut cur = IndexCursor::new(&mut pager, root);
        assert!(
            !cur.seek_first_with_prefix(&prefix).unwrap(),
            "unexpected hit for absent key {k}"
        );
    }
}

#[test]
fn index_seek_duplicate_prefix_is_first_and_iterates() {
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();
    let mut root = btree_create_index(&mut pager).unwrap();

    // Distinct keys 0..400 (minus 42) plus a 300-entry duplicate group on
    // key=42, inserted descending so the group is not pre-sorted on the page.
    let n = 400i64;
    for k in 0..n {
        if k == 42 {
            continue;
        }
        let rec = Record {
            values: vec![Value::Integer(k), Value::Integer(k)],
        };
        root = btree_index_insert(&mut pager, root, &rec).unwrap();
    }
    let dup_rowids: Vec<i64> = (0..300).collect();
    for &r in dup_rowids.iter().rev() {
        let rec = Record {
            values: vec![Value::Integer(42), Value::Integer(r)],
        };
        root = btree_index_insert(&mut pager, root, &rec).unwrap();
    }

    let prefix = Record {
        values: vec![Value::Integer(42)],
    };
    let mut cur = IndexCursor::new(&mut pager, root);
    assert!(cur.seek_first_with_prefix(&prefix).unwrap());
    // First hit is the smallest-rowid duplicate.
    assert_eq!(cur.current().unwrap().values[1], Value::Integer(0));

    let mut seen = Vec::new();
    loop {
        let rec = cur.current().unwrap();
        if compare_records_by_prefix(&rec, &prefix, 1) != std::cmp::Ordering::Equal {
            break;
        }
        if let Value::Integer(r) = rec.values[1] {
            seen.push(r);
        }
        if !cur.next().unwrap() {
            break;
        }
    }
    assert_eq!(seen, dup_rowids, "must iterate every duplicate exactly once");
}

#[test]
fn table_rowid_seek_matches_linear() {
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();
    let mut root = btree_create_table(&mut pager).unwrap();

    let n = 1000i64;
    let mut order: Vec<i64> = (1..=n).collect();
    lcg_shuffle(&mut order, 987654321);
    for &rid in &order {
        let rec = Record {
            values: vec![Value::Text(format!("row{rid}")), Value::Integer(rid * 7)],
        };
        root = btree_insert(&mut pager, root, rid, &rec).unwrap();
    }

    for rid in 1..=n {
        let row = btree_read_row_by_rowid(&mut pager, root, rid)
            .unwrap()
            .unwrap_or_else(|| panic!("missing rowid {rid}"));
        assert_eq!(row.rowid, rid);
        assert_eq!(row.record.values[0], Value::Text(format!("row{rid}")));
        assert_eq!(row.record.values[1], Value::Integer(rid * 7));
    }

    for rid in [0i64, n + 1, n + 999] {
        assert!(
            btree_read_row_by_rowid(&mut pager, root, rid)
                .unwrap()
                .is_none(),
            "unexpected row for absent rowid {rid}"
        );
    }
}

// ── In-place deletes (Phase: write path) ──

#[test]
fn table_inplace_delete_keeps_survivors() {
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();
    let mut root = btree_create_table(&mut pager).unwrap();

    let n = 1200i64;
    for rid in 1..=n {
        let rec = Record {
            values: vec![Value::Text(format!("v{rid}"))],
        };
        root = btree_insert(&mut pager, root, rid, &rec).unwrap();
    }

    // Delete a scattered ~1/3 of the rows, one at a time (in-place).
    let mut deleted = std::collections::HashSet::new();
    let mut order: Vec<i64> = (1..=n).collect();
    lcg_shuffle(&mut order, 2024);
    for &rid in order.iter().take((n / 3) as usize) {
        btree_delete(&mut pager, root, rid).unwrap();
        deleted.insert(rid);
    }

    // collect_all returns exactly the survivors, in rowid order, values intact.
    let rows = {
        let mut c = BTreeCursor::new(&mut pager, root);
        c.collect_all().unwrap()
    };
    let survivors: Vec<i64> = (1..=n).filter(|r| !deleted.contains(r)).collect();
    assert_eq!(rows.len(), survivors.len(), "survivor count");
    for (row, &rid) in rows.iter().zip(&survivors) {
        assert_eq!(row.rowid, rid);
        assert_eq!(row.record.values[0], Value::Text(format!("v{rid}")));
    }

    // Point lookups: survivors found, deleted gone.
    for rid in 1..=n {
        let got = btree_read_row_by_rowid(&mut pager, root, rid).unwrap();
        assert_eq!(
            got.is_some(),
            !deleted.contains(&rid),
            "rowid {rid} presence"
        );
    }
}

#[test]
fn index_inplace_delete_keeps_survivors() {
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();
    let mut root = btree_create_index(&mut pager).unwrap();

    let n = 900i64;
    let mut order: Vec<i64> = (0..n).collect();
    lcg_shuffle(&mut order, 55);
    for &k in &order {
        let rec = Record {
            values: vec![Value::Integer(k), Value::Integer(k)],
        };
        root = btree_index_insert(&mut pager, root, &rec).unwrap();
    }

    // Delete ~half the keys in place.
    let mut deleted = std::collections::HashSet::new();
    for &k in order.iter().take((n / 2) as usize) {
        let key = Record {
            values: vec![Value::Integer(k), Value::Integer(k)],
        };
        btree_index_delete(&mut pager, root, &key).unwrap();
        deleted.insert(k);
    }

    // Seeks find survivors and miss deleted keys.
    for k in 0..n {
        let prefix = Record {
            values: vec![Value::Integer(k)],
        };
        let mut cur = IndexCursor::new(&mut pager, root);
        let found = cur.seek_first_with_prefix(&prefix).unwrap();
        assert_eq!(found, !deleted.contains(&k), "key {k} presence via seek");
    }

    // Full scan yields exactly the survivors.
    let remaining = {
        let mut c = IndexCursor::new(&mut pager, root);
        c.collect_all().unwrap()
    };
    assert_eq!(remaining.len(), (n - deleted.len() as i64) as usize);
}

#[test]
fn inplace_delete_all_then_reinsert() {
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();
    let mut root = btree_create_table(&mut pager).unwrap();

    let n = 500i64;
    for rid in 1..=n {
        let rec = Record {
            values: vec![Value::Integer(rid)],
        };
        root = btree_insert(&mut pager, root, rid, &rec).unwrap();
    }
    let all: Vec<i64> = (1..=n).collect();
    btree_delete_many(&mut pager, root, &all).unwrap();

    let rows = {
        let mut c = BTreeCursor::new(&mut pager, root);
        c.collect_all().unwrap()
    };
    assert_eq!(rows.len(), 0, "table empty after deleting everything");

    // Re-insert into the (now sparse/empty) tree works and reads back.
    for rid in 1..=n {
        let rec = Record {
            values: vec![Value::Integer(rid * 2)],
        };
        root = btree_insert(&mut pager, root, rid, &rec).unwrap();
    }
    for rid in 1..=n {
        let row = btree_read_row_by_rowid(&mut pager, root, rid)
            .unwrap()
            .unwrap();
        assert_eq!(row.record.values[0], Value::Integer(rid * 2));
    }
}

#[test]
fn max_rowid_robust_after_deleting_highest() {
    // Deleting the highest rowid may empty the rightmost leaf; btree_max_rowid
    // must not fall back to 0 and let a re-insert collide with a live rowid.
    let vfs = MemoryVfs::new();
    let mut pager = Pager::create(&vfs, "test.db").unwrap();
    let mut root = btree_create_table(&mut pager).unwrap();

    let n = 1000i64;
    for rid in 1..=n {
        let rec = Record {
            values: vec![Value::Integer(rid)],
        };
        root = btree_insert(&mut pager, root, rid, &rec).unwrap();
    }
    // Delete the top 50 rowids one at a time.
    for rid in (n - 49..=n).rev() {
        btree_delete(&mut pager, root, rid).unwrap();
    }
    // Next rowid must be strictly greater than every surviving rowid (n-50).
    let next = btree_max_rowid(&mut pager, root).unwrap() + 1;
    assert!(
        next > n - 50,
        "next rowid {next} must exceed the surviving max {}",
        n - 50
    );
    // And inserting at `next` must not overwrite a survivor.
    let rec = Record {
        values: vec![Value::Integer(9999)],
    };
    root = btree_insert(&mut pager, root, next, &rec).unwrap();
    let survivors = {
        let mut c = BTreeCursor::new(&mut pager, root);
        c.collect_all().unwrap()
    };
    // (n - 50) survivors + 1 new row, all distinct rowids.
    assert_eq!(survivors.len(), (n - 50 + 1) as usize);
    let mut ids: Vec<i64> = survivors.iter().map(|r| r.rowid).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), (n - 50 + 1) as usize, "no rowid collision");
}
