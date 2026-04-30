use rsqlite_storage::codec::Value;
use rsqlite_storage::pager::Pager;

use crate::catalog::Catalog;
use crate::eval_helpers::compare;
use crate::planner::SortKey;
use crate::types::Row;

pub(super) fn compare_rows_by_keys(
    a: &Row,
    b: &Row,
    keys: &[SortKey],
    columns: &[String],
    pager: &mut Pager,
    catalog: &Catalog,
) -> std::cmp::Ordering {
    for key in keys {
        let va = super::eval::eval_expr(&key.expr, a, columns, pager, catalog).unwrap_or(Value::Null);
        let vb = super::eval::eval_expr(&key.expr, b, columns, pager, catalog).unwrap_or(Value::Null);

        let nocase = super::eval::has_nocase_collation(&key.expr);
        let cmp_val = if nocase {
            compare(&super::eval::fold_nocase(&va), &super::eval::fold_nocase(&vb))
        } else {
            compare(&va, &vb)
        };
        let ordering = if cmp_val < 0 {
            std::cmp::Ordering::Less
        } else if cmp_val > 0 {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        };

        let ordering = if key.descending {
            ordering.reverse()
        } else {
            ordering
        };

        if ordering != std::cmp::Ordering::Equal {
            let a_null = matches!(va, Value::Null);
            let b_null = matches!(vb, Value::Null);
            if a_null || b_null {
                let nulls_first = key.nulls_first.unwrap_or(!key.descending);
                if a_null && !b_null {
                    return if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                }
                if !a_null && b_null {
                    return if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    };
                }
            }

            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

pub(super) fn row_hash_key(row: &Row) -> Vec<u8> {
    let mut key = Vec::new();
    for val in &row.values {
        match val {
            Value::Null => key.push(0),
            Value::Integer(n) => {
                key.push(1);
                key.extend_from_slice(&n.to_le_bytes());
            }
            Value::Real(f) => {
                key.push(2);
                key.extend_from_slice(&f.to_le_bytes());
            }
            Value::Text(s) => {
                key.push(3);
                key.extend_from_slice(s.as_bytes());
                key.push(0);
            }
            Value::Blob(b) => {
                key.push(4);
                key.extend_from_slice(b);
                key.push(0);
            }
        }
    }
    key
}
