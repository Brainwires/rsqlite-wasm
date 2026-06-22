//! `rtree` — multi-dimensional bounding-box index backed by a real
//! R*-Tree (quadratic split heuristic, MBR cascade on insert,
//! intersection-pruned DFS on query).
//!
//! ```sql
//! CREATE VIRTUAL TABLE places USING rtree(2);
//! INSERT INTO places VALUES (?min_x, ?max_x, ?min_y, ?max_y);
//!
//! -- All bounding boxes that overlap the query rectangle (0,10)x(0,10).
//! -- The canonical AABB-overlap shape (`max_i >= a AND min_i <= b` per
//! -- dimension) is recognized by the module's best_index hook and
//! -- routed through the tree instead of a linear scan.
//! SELECT rowid FROM places
//!  WHERE max_0 >= 0  AND min_0 <= 10
//!    AND max_1 >= 0  AND min_1 <= 10;
//! ```
//!
//! Supports 1D through 5D, matching SQLite's R-Tree extension. Each
//! dimension contributes two columns: `min_<i>`, `max_<i>` (i = 0…dim-1).
//!
//! ### Algorithm sketch
//!
//! - **Insert**: ChooseSubtree picks the child whose MBR enlargement is
//!   smallest (least-overlap-increase tiebreaker at the leaf-parent
//!   level). On overflow, R*-style quadratic split selects the seed
//!   pair that wastes the most area when combined, then distributes
//!   the rest one-at-a-time to the group with the smallest enlargement.
//!   Splits cascade upward; the root grows when it splits.
//! - **Query**: DFS from the root, descending only into children whose
//!   MBR intersects the query box.

use std::cell::RefCell;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::planner::{BinOp, ColumnRef, LiteralValue, PlanExpr};
use crate::types::Row;

use super::{Module, VirtualTable, VtabFilterPlan};

const MAX_DIM: usize = 5;
/// Branching factor — children per internal node / entries per leaf.
/// Eight is the canonical R-tree default; small enough that worst-case
/// split work stays cheap, large enough to keep the tree shallow.
const MAX_CHILDREN: usize = 8;
/// Lower bound on per-node fill, used by the split distributor to keep
/// nodes within `[MIN_CHILDREN, MAX_CHILDREN]`.
const MIN_CHILDREN: usize = 3;

pub(super) struct RtreeModule;

impl Module for RtreeModule {
    fn name(&self) -> &str {
        "rtree"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        let dim = match args {
            [d] => d.parse::<usize>().map_err(|_| {
                Error::Other(format!(
                    "rtree: dim must be a positive integer (1..={MAX_DIM}), got {d:?}"
                ))
            })?,
            _ => {
                return Err(Error::Other(
                    "rtree: expected one argument (dim) — `USING rtree(2)`".into(),
                ));
            }
        };
        if dim == 0 || dim > MAX_DIM {
            return Err(Error::Other(format!(
                "rtree: dim must be 1..={MAX_DIM}, got {dim}"
            )));
        }
        let cols: Vec<String> = (0..dim)
            .flat_map(|i| [format!("min_{i}"), format!("max_{i}")])
            .collect();
        Ok(Rc::new(RtreeTable {
            dim,
            cols,
            state: RefCell::new(RtreeState::new(dim)),
        }))
    }
}

// ── Bounding box ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct BoundingBox {
    min: Vec<f64>,
    max: Vec<f64>,
}

impl BoundingBox {
    fn from_flat(dim: usize, flat: &[f64]) -> Self {
        let mut min = Vec::with_capacity(dim);
        let mut max = Vec::with_capacity(dim);
        for d in 0..dim {
            min.push(flat[d * 2]);
            max.push(flat[d * 2 + 1]);
        }
        BoundingBox { min, max }
    }

    fn to_flat(&self) -> Vec<f64> {
        let mut out = Vec::with_capacity(self.min.len() * 2);
        for d in 0..self.min.len() {
            out.push(self.min[d]);
            out.push(self.max[d]);
        }
        out
    }

    /// MBR that covers `self` plus every box in `others`.
    fn union<'a, I: IntoIterator<Item = &'a BoundingBox>>(&self, others: I) -> BoundingBox {
        let mut out = self.clone();
        for o in others {
            for d in 0..out.min.len() {
                if o.min[d] < out.min[d] {
                    out.min[d] = o.min[d];
                }
                if o.max[d] > out.max[d] {
                    out.max[d] = o.max[d];
                }
            }
        }
        out
    }

    fn union_with(&self, other: &BoundingBox) -> BoundingBox {
        self.union(std::iter::once(other))
    }

    fn area(&self) -> f64 {
        let mut a = 1.0;
        for d in 0..self.min.len() {
            // `(max - min)` is non-negative by INSERT validation.
            // Add a tiny epsilon-style guard so zero-extent (point)
            // boxes still produce a comparable enlargement signal.
            a *= (self.max[d] - self.min[d]).max(0.0);
        }
        a
    }

    fn intersects(&self, other: &BoundingBox) -> bool {
        for d in 0..self.min.len() {
            if self.max[d] < other.min[d] || other.max[d] < self.min[d] {
                return false;
            }
        }
        true
    }

    /// Area gained by enlarging `self` to also cover `other`.
    fn enlargement(&self, other: &BoundingBox) -> f64 {
        let combined = self.union_with(other);
        combined.area() - self.area()
    }

    /// Area of the intersection of two boxes (zero if disjoint).
    fn intersection_area(&self, other: &BoundingBox) -> f64 {
        let mut a = 1.0;
        for d in 0..self.min.len() {
            let lo = self.min[d].max(other.min[d]);
            let hi = self.max[d].min(other.max[d]);
            let extent = hi - lo;
            if extent <= 0.0 {
                return 0.0;
            }
            a *= extent;
        }
        a
    }
}

// ── Tree storage ──────────────────────────────────────────────────────

type NodeId = usize;

#[derive(Debug, Clone)]
enum Children {
    /// Internal node: pointers to child nodes.
    Internal(Vec<NodeId>),
    /// Leaf node: (rowid, mbr) entries for the stored boxes.
    Leaf(Vec<(i64, BoundingBox)>),
}

#[derive(Debug, Clone)]
struct RTreeNode {
    mbr: BoundingBox,
    children: Children,
}

impl RTreeNode {
    fn child_count(&self) -> usize {
        match &self.children {
            Children::Internal(v) => v.len(),
            Children::Leaf(v) => v.len(),
        }
    }
    fn is_leaf(&self) -> bool {
        matches!(self.children, Children::Leaf(_))
    }
}

/// All R-tree state (nodes + the root pointer + the raw box list used
/// for replay). The raw list is the persistence shadow — we re-insert
/// from it on open so the tree itself doesn't need on-disk format.
struct RtreeState {
    dim: usize,
    nodes: Vec<RTreeNode>,
    root: NodeId,
    /// Stored boxes in insertion order, rowid = index + 1.
    boxes: Vec<BoundingBox>,
}

impl RtreeState {
    fn new(dim: usize) -> Self {
        let empty_mbr = BoundingBox {
            min: vec![f64::INFINITY; dim],
            max: vec![f64::NEG_INFINITY; dim],
        };
        let root = RTreeNode {
            mbr: empty_mbr,
            children: Children::Leaf(Vec::new()),
        };
        RtreeState {
            dim,
            nodes: vec![root],
            root: 0,
            boxes: Vec::new(),
        }
    }

    fn alloc_node(&mut self, node: RTreeNode) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }

    /// Recompute the MBR of a node from its children.
    fn recompute_mbr(&mut self, node_id: NodeId) {
        let node = &self.nodes[node_id];
        let new_mbr = match &node.children {
            Children::Internal(child_ids) => {
                if child_ids.is_empty() {
                    empty_mbr(self.dim)
                } else {
                    let mut iter = child_ids.iter();
                    let first = self.nodes[*iter.next().unwrap()].mbr.clone();
                    first.union(iter.map(|cid| &self.nodes[*cid].mbr))
                }
            }
            Children::Leaf(entries) => {
                if entries.is_empty() {
                    empty_mbr(self.dim)
                } else {
                    let mut iter = entries.iter();
                    let first = iter.next().unwrap().1.clone();
                    first.union(iter.map(|(_, b)| b))
                }
            }
        };
        self.nodes[node_id].mbr = new_mbr;
    }

    /// Insert one (rowid, box) pair. Splits cascade upward; the root
    /// grows a fresh internal level if it splits.
    fn insert(&mut self, rowid: i64, bbox: BoundingBox) {
        let split = self.insert_at(self.root, rowid, &bbox);
        if let Some(new_node) = split {
            // Root split → create a new internal root with both halves.
            let old_root = self.root;
            let mbr_old = self.nodes[old_root].mbr.clone();
            let mbr_new = self.nodes[new_node].mbr.clone();
            let new_root = RTreeNode {
                mbr: mbr_old.union_with(&mbr_new),
                children: Children::Internal(vec![old_root, new_node]),
            };
            let new_root_id = self.alloc_node(new_root);
            self.root = new_root_id;
        }
    }

    /// Recursive insert. Returns `Some(new_sibling)` if `node_id` split
    /// into itself + a fresh sibling (whose id is returned).
    fn insert_at(&mut self, node_id: NodeId, rowid: i64, bbox: &BoundingBox) -> Option<NodeId> {
        let is_leaf = self.nodes[node_id].is_leaf();
        if is_leaf {
            // Append the entry; split if we exceed MAX_CHILDREN.
            if let Children::Leaf(entries) = &mut self.nodes[node_id].children {
                entries.push((rowid, bbox.clone()));
            }
            self.recompute_mbr(node_id);
            if self.nodes[node_id].child_count() > MAX_CHILDREN {
                return Some(self.split_leaf(node_id));
            }
            return None;
        }

        // Internal: choose subtree with smallest enlargement; tiebreak
        // (when descending into a leaf parent) on least overlap increase.
        let chosen_child = self.choose_subtree(node_id, bbox);
        let split = self.insert_at(chosen_child, rowid, bbox);
        self.recompute_mbr(node_id);

        if let Some(new_child) = split {
            if let Children::Internal(children) = &mut self.nodes[node_id].children {
                children.push(new_child);
            }
            self.recompute_mbr(node_id);
            if self.nodes[node_id].child_count() > MAX_CHILDREN {
                return Some(self.split_internal(node_id));
            }
        }
        None
    }

    /// ChooseSubtree: pick the child whose MBR needs the least
    /// enlargement to cover `bbox`. When the candidate node's children
    /// are leaves (the leaf-parent layer), R*-Tree breaks enlargement
    /// ties on the least overlap-area increase to keep leaves
    /// well-separated. Final tiebreaker: smaller current area.
    fn choose_subtree(&self, parent_id: NodeId, bbox: &BoundingBox) -> NodeId {
        let child_ids = match &self.nodes[parent_id].children {
            Children::Internal(v) => v.clone(),
            Children::Leaf(_) => unreachable!("choose_subtree called on a leaf"),
        };

        let leaf_parent = matches!(self.nodes[child_ids[0]].children, Children::Leaf(_));

        let mut best: Option<(NodeId, f64, f64, f64)> = None; // (id, enlargement, overlap_delta, area)
        for cid in &child_ids {
            let child_mbr = &self.nodes[*cid].mbr;
            let enlarged = child_mbr.union_with(bbox);
            let enlargement = enlarged.area() - child_mbr.area();

            let overlap_delta = if leaf_parent {
                let mut before = 0.0;
                let mut after = 0.0;
                for ocid in &child_ids {
                    if ocid == cid {
                        continue;
                    }
                    let other = &self.nodes[*ocid].mbr;
                    before += child_mbr.intersection_area(other);
                    after += enlarged.intersection_area(other);
                }
                after - before
            } else {
                0.0
            };

            let area = child_mbr.area();
            let candidate = (*cid, enlargement, overlap_delta, area);

            best = match best {
                None => Some(candidate),
                Some(prev) => {
                    if leaf_parent {
                        // Order: overlap_delta, enlargement, area.
                        if (candidate.2, candidate.1, candidate.3) < (prev.2, prev.1, prev.3) {
                            Some(candidate)
                        } else {
                            Some(prev)
                        }
                    } else {
                        // Internal-of-internal: enlargement first, area second.
                        if (candidate.1, candidate.3) < (prev.1, prev.3) {
                            Some(candidate)
                        } else {
                            Some(prev)
                        }
                    }
                }
            };
        }
        best.expect("internal node must have children").0
    }

    /// R*-Tree quadratic split for a leaf node. Picks the seed pair
    /// whose combined MBR wastes the most area, then distributes the
    /// remaining entries one at a time to the group whose MBR
    /// enlargement is smallest. The original node keeps the first
    /// group; a fresh sibling node is allocated for the second group
    /// and its id is returned.
    fn split_leaf(&mut self, node_id: NodeId) -> NodeId {
        let entries = match &mut self.nodes[node_id].children {
            Children::Leaf(v) => std::mem::take(v),
            _ => unreachable!("split_leaf on non-leaf"),
        };

        let (seed_a, seed_b) = pick_seeds_leaf(&entries);
        let mut group_a = vec![entries[seed_a].clone()];
        let mut group_b = vec![entries[seed_b].clone()];
        let mut mbr_a = entries[seed_a].1.clone();
        let mut mbr_b = entries[seed_b].1.clone();

        let mut remaining: Vec<(i64, BoundingBox)> = entries
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != seed_a && *i != seed_b)
            .map(|(_, e)| e)
            .collect();

        while !remaining.is_empty() {
            // Force-fill: if one group has too few left to hit MIN_CHILDREN,
            // dump the rest into it.
            let need_a = MIN_CHILDREN.saturating_sub(group_a.len());
            let need_b = MIN_CHILDREN.saturating_sub(group_b.len());
            if remaining.len() <= need_a {
                for entry in remaining.drain(..) {
                    mbr_a = mbr_a.union_with(&entry.1);
                    group_a.push(entry);
                }
                break;
            }
            if remaining.len() <= need_b {
                for entry in remaining.drain(..) {
                    mbr_b = mbr_b.union_with(&entry.1);
                    group_b.push(entry);
                }
                break;
            }

            // Pick the entry with the largest enlargement-difference
            // between groups (PickNext); assign it to the group it
            // enlarges less.
            let (idx, target_a) = pick_next(&remaining, &mbr_a, &mbr_b);
            let entry = remaining.remove(idx);
            if target_a {
                mbr_a = mbr_a.union_with(&entry.1);
                group_a.push(entry);
            } else {
                mbr_b = mbr_b.union_with(&entry.1);
                group_b.push(entry);
            }
        }

        self.nodes[node_id].children = Children::Leaf(group_a);
        self.recompute_mbr(node_id);
        let sibling = RTreeNode {
            mbr: mbr_b,
            children: Children::Leaf(group_b),
        };
        let sibling_id = self.alloc_node(sibling);
        self.recompute_mbr(sibling_id);
        sibling_id
    }

    /// Same algorithm as `split_leaf`, but operates on internal-node
    /// child pointers (and uses the children's MBRs to seed and grow).
    fn split_internal(&mut self, node_id: NodeId) -> NodeId {
        let child_ids = match &mut self.nodes[node_id].children {
            Children::Internal(v) => std::mem::take(v),
            _ => unreachable!("split_internal on leaf"),
        };
        let entries: Vec<(NodeId, BoundingBox)> = child_ids
            .iter()
            .map(|cid| (*cid, self.nodes[*cid].mbr.clone()))
            .collect();

        let (seed_a, seed_b) = pick_seeds_internal(&entries);
        let mut group_a: Vec<NodeId> = vec![entries[seed_a].0];
        let mut group_b: Vec<NodeId> = vec![entries[seed_b].0];
        let mut mbr_a = entries[seed_a].1.clone();
        let mut mbr_b = entries[seed_b].1.clone();

        let mut remaining: Vec<(NodeId, BoundingBox)> = entries
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != seed_a && *i != seed_b)
            .map(|(_, e)| e)
            .collect();

        while !remaining.is_empty() {
            let need_a = MIN_CHILDREN.saturating_sub(group_a.len());
            let need_b = MIN_CHILDREN.saturating_sub(group_b.len());
            if remaining.len() <= need_a {
                for (cid, mbr) in remaining.drain(..) {
                    mbr_a = mbr_a.union_with(&mbr);
                    group_a.push(cid);
                }
                break;
            }
            if remaining.len() <= need_b {
                for (cid, mbr) in remaining.drain(..) {
                    mbr_b = mbr_b.union_with(&mbr);
                    group_b.push(cid);
                }
                break;
            }

            let (idx, target_a) = pick_next_internal(&remaining, &mbr_a, &mbr_b);
            let (cid, mbr) = remaining.remove(idx);
            if target_a {
                mbr_a = mbr_a.union_with(&mbr);
                group_a.push(cid);
            } else {
                mbr_b = mbr_b.union_with(&mbr);
                group_b.push(cid);
            }
        }

        self.nodes[node_id].children = Children::Internal(group_a);
        self.recompute_mbr(node_id);
        let sibling = RTreeNode {
            mbr: mbr_b,
            children: Children::Internal(group_b),
        };
        let sibling_id = self.alloc_node(sibling);
        self.recompute_mbr(sibling_id);
        sibling_id
    }

    /// DFS the tree, yielding rowids whose MBR intersects `query`.
    fn query_overlap(&self, query: &BoundingBox) -> Vec<i64> {
        let mut out = Vec::new();
        let mut stack = vec![self.root];
        while let Some(id) = stack.pop() {
            let node = &self.nodes[id];
            if !node.mbr.intersects(query) {
                continue;
            }
            match &node.children {
                Children::Internal(child_ids) => {
                    for cid in child_ids {
                        if self.nodes[*cid].mbr.intersects(query) {
                            stack.push(*cid);
                        }
                    }
                }
                Children::Leaf(entries) => {
                    for (rowid, mbr) in entries {
                        if mbr.intersects(query) {
                            out.push(*rowid);
                        }
                    }
                }
            }
        }
        out.sort_unstable();
        out
    }
}

fn empty_mbr(dim: usize) -> BoundingBox {
    BoundingBox {
        min: vec![f64::INFINITY; dim],
        max: vec![f64::NEG_INFINITY; dim],
    }
}

/// Pick the seed pair (indexes into `entries`) whose combined MBR
/// wastes the most area — `area(union) - area(a) - area(b)` is largest.
fn pick_seeds_leaf(entries: &[(i64, BoundingBox)]) -> (usize, usize) {
    let n = entries.len();
    let mut best = (0usize, 1usize);
    let mut best_waste = f64::NEG_INFINITY;
    for i in 0..n {
        for j in (i + 1)..n {
            let waste = entries[i].1.union_with(&entries[j].1).area()
                - entries[i].1.area()
                - entries[j].1.area();
            if waste > best_waste {
                best_waste = waste;
                best = (i, j);
            }
        }
    }
    best
}

fn pick_seeds_internal(entries: &[(NodeId, BoundingBox)]) -> (usize, usize) {
    let n = entries.len();
    let mut best = (0usize, 1usize);
    let mut best_waste = f64::NEG_INFINITY;
    for i in 0..n {
        for j in (i + 1)..n {
            let waste = entries[i].1.union_with(&entries[j].1).area()
                - entries[i].1.area()
                - entries[j].1.area();
            if waste > best_waste {
                best_waste = waste;
                best = (i, j);
            }
        }
    }
    best
}

/// PickNext (leaf): pick the entry with the largest "preference
/// difference" — i.e. the entry that most strongly prefers one group
/// over the other. Returns `(index_in_remaining, prefer_group_a)`.
fn pick_next(
    remaining: &[(i64, BoundingBox)],
    mbr_a: &BoundingBox,
    mbr_b: &BoundingBox,
) -> (usize, bool) {
    let mut best_idx = 0;
    let mut best_diff = f64::NEG_INFINITY;
    let mut best_prefer_a = true;
    for (i, (_, b)) in remaining.iter().enumerate() {
        let ea = mbr_a.enlargement(b);
        let eb = mbr_b.enlargement(b);
        let diff = (ea - eb).abs();
        if diff > best_diff {
            best_diff = diff;
            best_idx = i;
            best_prefer_a = ea < eb || (ea == eb && mbr_a.area() <= mbr_b.area());
        }
    }
    (best_idx, best_prefer_a)
}

fn pick_next_internal(
    remaining: &[(NodeId, BoundingBox)],
    mbr_a: &BoundingBox,
    mbr_b: &BoundingBox,
) -> (usize, bool) {
    let mut best_idx = 0;
    let mut best_diff = f64::NEG_INFINITY;
    let mut best_prefer_a = true;
    for (i, (_, b)) in remaining.iter().enumerate() {
        let ea = mbr_a.enlargement(b);
        let eb = mbr_b.enlargement(b);
        let diff = (ea - eb).abs();
        if diff > best_diff {
            best_diff = diff;
            best_idx = i;
            best_prefer_a = ea < eb || (ea == eb && mbr_a.area() <= mbr_b.area());
        }
    }
    (best_idx, best_prefer_a)
}

// ── VirtualTable wiring ───────────────────────────────────────────────

pub(super) struct RtreeTable {
    dim: usize,
    cols: Vec<String>,
    state: RefCell<RtreeState>,
}

impl RtreeTable {
    fn row_floats(&self) -> usize {
        self.dim * 2
    }

    /// Public test/diagnostic hook: rowids whose stored bounding box
    /// overlaps `query`. `query` is the canonical
    /// `[min_0, max_0, min_1, max_1, …]` flat layout.
    #[cfg(test)]
    pub fn overlapping(&self, query: &[f64]) -> Result<Vec<i64>> {
        if query.len() != self.row_floats() {
            return Err(Error::Other(format!(
                "rtree: query box has {} floats, table is {}D ({} expected)",
                query.len(),
                self.dim,
                self.row_floats()
            )));
        }
        let q = BoundingBox::from_flat(self.dim, query);
        Ok(self.state.borrow().query_overlap(&q))
    }
}

impl VirtualTable for RtreeTable {
    fn columns(&self) -> Vec<String> {
        self.cols.clone()
    }

    fn scan(&self) -> Result<Vec<Row>> {
        let state = self.state.borrow();
        let mut rows = Vec::with_capacity(state.boxes.len());
        for (i, bbox) in state.boxes.iter().enumerate() {
            let values: Vec<Value> = bbox.to_flat().into_iter().map(Value::Real).collect();
            rows.push(Row::with_rowid(values, (i as i64) + 1));
        }
        Ok(rows)
    }

    fn insert(&self, values: &[Value]) -> Result<i64> {
        if values.len() != self.row_floats() {
            return Err(Error::Other(format!(
                "rtree: INSERT expects {} columns, got {}",
                self.row_floats(),
                values.len()
            )));
        }
        let mut row = Vec::with_capacity(self.row_floats());
        for (i, v) in values.iter().enumerate() {
            let f = match v {
                Value::Real(f) => *f,
                Value::Integer(n) => *n as f64,
                _ => {
                    return Err(Error::Other(format!(
                        "rtree: column {i} must be numeric (REAL or INTEGER)"
                    )));
                }
            };
            row.push(f);
        }
        // Sanity-check min ≤ max per dimension.
        for d in 0..self.dim {
            if row[d * 2] > row[d * 2 + 1] {
                return Err(Error::Other(format!(
                    "rtree: dim {d} has min ({}) > max ({})",
                    row[d * 2],
                    row[d * 2 + 1]
                )));
            }
        }
        let mut state = self.state.borrow_mut();
        let bbox = BoundingBox::from_flat(self.dim, &row);
        state.boxes.push(bbox.clone());
        let rowid = state.boxes.len() as i64;
        state.insert(rowid, bbox);
        Ok(rowid)
    }

    /// Recognize the canonical AABB-overlap WHERE shape and translate
    /// it into a tree query. Per dimension we expect both
    /// `max_<d> >= <lit>` AND `min_<d> <= <lit>` (in either operand
    /// order) somewhere in the AND-tree of conjuncts. Any conjunct that
    /// doesn't match that shape becomes the residual the executor
    /// still has to evaluate.
    fn best_index(
        &self,
        predicate: &PlanExpr,
        columns: &[ColumnRef],
    ) -> Result<Option<VtabFilterPlan>> {
        // Build a `column-name → dim, side` lookup so we can recognize
        // `min_0`, `max_0`, …. `side = false` means min, `side = true` means max.
        let mut col_kind: Vec<Option<(usize, bool)>> = Vec::with_capacity(columns.len());
        for c in columns {
            let lname = c.name.to_ascii_lowercase();
            let kind = if let Some(rest) = lname.strip_prefix("min_") {
                rest.parse::<usize>().ok().map(|d| (d, false))
            } else if let Some(rest) = lname.strip_prefix("max_") {
                rest.parse::<usize>().ok().map(|d| (d, true))
            } else {
                None
            };
            col_kind.push(kind);
        }

        let mut conjuncts = Vec::new();
        flatten_and(predicate, &mut conjuncts);

        // Per-dim accumulators: query box's lower bound (max_d >= a → a)
        // and upper bound (min_d <= b → b).
        let mut q_min: Vec<Option<f64>> = vec![None; self.dim];
        let mut q_max: Vec<Option<f64>> = vec![None; self.dim];
        let mut residual: Vec<&PlanExpr> = Vec::new();

        for c in &conjuncts {
            match recognize_aabb_conjunct(c, &col_kind) {
                Some(AabbBound::QMin { dim, value }) => {
                    let slot = &mut q_min[dim];
                    *slot = Some(slot.map_or(value, |prev: f64| prev.max(value)));
                }
                Some(AabbBound::QMax { dim, value }) => {
                    let slot = &mut q_max[dim];
                    *slot = Some(slot.map_or(value, |prev: f64| prev.min(value)));
                }
                None => residual.push(c),
            }
        }

        // We need at least one matched dimension AND every matched
        // dimension to be fully bounded (both sides). If the user only
        // gave us a partial AABB shape we fall back to the default
        // scan-then-filter path, which is still correct.
        let mut full_bounds: Vec<(f64, f64)> = Vec::with_capacity(self.dim);
        let mut any_matched = false;
        let mut any_partial = false;
        for d in 0..self.dim {
            match (q_min[d], q_max[d]) {
                (Some(lo), Some(hi)) => {
                    full_bounds.push((lo, hi));
                    any_matched = true;
                }
                (None, None) => {
                    // Unbounded along this dim — substitute ±∞ so the
                    // tree query effectively ignores it.
                    full_bounds.push((f64::NEG_INFINITY, f64::INFINITY));
                }
                _ => {
                    // Half-bounded — the conjuncts we matched aren't the
                    // canonical AABB shape, fall through.
                    any_partial = true;
                }
            }
        }
        if !any_matched || any_partial {
            return Ok(None);
        }

        let mut min = Vec::with_capacity(self.dim);
        let mut max = Vec::with_capacity(self.dim);
        for (lo, hi) in &full_bounds {
            min.push(*lo);
            max.push(*hi);
        }
        let query = BoundingBox { min, max };

        let rowids = self.state.borrow().query_overlap(&query);

        // Re-AND any leftover conjuncts as the residual predicate.
        let residual_expr = residual
            .into_iter()
            .cloned()
            .reduce(|a, b| PlanExpr::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            });

        Ok(Some(VtabFilterPlan {
            rowids,
            residual: residual_expr,
        }))
    }
}

/// Flatten a tree of `AND` BinaryOps into a flat list of conjuncts.
fn flatten_and<'a>(expr: &'a PlanExpr, out: &mut Vec<&'a PlanExpr>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        other => out.push(other),
    }
}

/// One side of the AABB recognition: either a lower bound on the query
/// box (from `max_<d> >= lit`) or an upper bound (from `min_<d> <= lit`).
enum AabbBound {
    QMin { dim: usize, value: f64 },
    QMax { dim: usize, value: f64 },
}

/// Match a single conjunct against `<col> <cmp> <lit>` (or its mirror)
/// where `<col>` is one of the rtree's `min_d` / `max_d` columns.
fn recognize_aabb_conjunct(
    expr: &PlanExpr,
    col_kind: &[Option<(usize, bool)>],
) -> Option<AabbBound> {
    let (left, op, right) = match expr {
        PlanExpr::BinaryOp { left, op, right } => (left.as_ref(), *op, right.as_ref()),
        _ => return None,
    };

    // Determine if one side is a column we know and the other is a literal.
    let (col_side_left, col, lit, eff_op) = match (column_index(left), literal_f64(right)) {
        (Some(idx), Some(v)) => (true, idx, v, op),
        _ => match (column_index(right), literal_f64(left)) {
            (Some(idx), Some(v)) => {
                // Mirror the operator: `5 <= x` ↔ `x >= 5`.
                let mirrored = match op {
                    BinOp::Lt => BinOp::Gt,
                    BinOp::LtEq => BinOp::GtEq,
                    BinOp::Gt => BinOp::Lt,
                    BinOp::GtEq => BinOp::LtEq,
                    other => other,
                };
                (false, idx, v, mirrored)
            }
            _ => return None,
        },
    };
    let _ = col_side_left;

    let (dim, is_max) = (*col_kind.get(col)?)?;
    match (is_max, eff_op) {
        // max_d >= a  →  query lower bound on dim d is a.
        (true, BinOp::GtEq) => Some(AabbBound::QMin { dim, value: lit }),
        // max_d > a  is equivalent for box-overlap: max >= a.
        (true, BinOp::Gt) => Some(AabbBound::QMin { dim, value: lit }),
        // min_d <= b  →  query upper bound on dim d is b.
        (false, BinOp::LtEq) => Some(AabbBound::QMax { dim, value: lit }),
        (false, BinOp::Lt) => Some(AabbBound::QMax { dim, value: lit }),
        _ => None,
    }
}

/// `expr` is a column reference in the table's column list — return
/// its column index.
fn column_index(expr: &PlanExpr) -> Option<usize> {
    if let PlanExpr::Column(c) = expr {
        Some(c.column_index)
    } else {
        None
    }
}

/// `expr` is a numeric literal — coerce to f64.
fn literal_f64(expr: &PlanExpr) -> Option<f64> {
    match expr {
        PlanExpr::Literal(LiteralValue::Real(f)) => Some(*f),
        PlanExpr::Literal(LiteralValue::Integer(n)) => Some(*n as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_table(dim: usize) -> RtreeTable {
        let cols: Vec<String> = (0..dim)
            .flat_map(|i| [format!("min_{i}"), format!("max_{i}")])
            .collect();
        RtreeTable {
            dim,
            cols,
            state: RefCell::new(RtreeState::new(dim)),
        }
    }

    #[test]
    fn create_with_2d_arg() {
        let m = RtreeModule;
        let t = m.create("places", &["2".into()]).unwrap();
        assert_eq!(t.columns(), vec!["min_0", "max_0", "min_1", "max_1"]);
    }

    #[test]
    fn create_rejects_zero_or_too_many_dims() {
        let m = RtreeModule;
        assert!(m.create("t", &["0".into()]).is_err());
        assert!(m.create("t", &["6".into()]).is_err());
        assert!(m.create("t", &[]).is_err());
        assert!(m.create("t", &["abc".into()]).is_err());
    }

    #[test]
    fn insert_validates_min_le_max() {
        let table = fresh_table(2);
        // min ≤ max — accepted.
        table
            .insert(&[
                Value::Real(0.0),
                Value::Real(1.0),
                Value::Real(0.0),
                Value::Real(1.0),
            ])
            .unwrap();
        // min > max — rejected.
        assert!(
            table
                .insert(&[
                    Value::Real(2.0),
                    Value::Real(1.0),
                    Value::Real(0.0),
                    Value::Real(1.0),
                ])
                .is_err()
        );
    }

    #[test]
    fn insert_rejects_wrong_column_count() {
        let table = fresh_table(2);
        assert!(table.insert(&[Value::Real(0.0), Value::Real(1.0)]).is_err());
    }

    #[test]
    fn overlapping_2d_finds_intersecting_boxes() {
        let table = fresh_table(2);
        // Three 2D boxes:
        //   1: (0..2) x (0..2)
        //   2: (5..6) x (5..6)
        //   3: (1..3) x (1..3) — overlaps box 1 and the query box
        for coords in &[
            [0.0, 2.0, 0.0, 2.0],
            [5.0, 6.0, 5.0, 6.0],
            [1.0, 3.0, 1.0, 3.0],
        ] {
            table
                .insert(&coords.iter().map(|f| Value::Real(*f)).collect::<Vec<_>>())
                .unwrap();
        }
        // Query (1.5..2.5) x (1.5..2.5) should hit boxes 1 and 3, not 2.
        let hits = table.overlapping(&[1.5, 2.5, 1.5, 2.5]).unwrap();
        assert_eq!(hits, vec![1, 3]);
    }

    #[test]
    fn overlapping_1d_works_too() {
        let table = fresh_table(1);
        table.insert(&[Value::Real(0.0), Value::Real(5.0)]).unwrap();
        table
            .insert(&[Value::Real(10.0), Value::Real(20.0)])
            .unwrap();
        let hits = table.overlapping(&[3.0, 4.0]).unwrap();
        assert_eq!(hits, vec![1]);
    }

    #[test]
    fn scan_round_trips() {
        let table = fresh_table(2);
        table
            .insert(&[
                Value::Real(1.0),
                Value::Real(2.0),
                Value::Real(3.0),
                Value::Real(4.0),
            ])
            .unwrap();
        let rows = table.scan().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].rowid, Some(1));
        assert_eq!(
            rows[0].values,
            vec![
                Value::Real(1.0),
                Value::Real(2.0),
                Value::Real(3.0),
                Value::Real(4.0),
            ]
        );
    }

    #[test]
    fn integer_inserts_coerce_to_float() {
        let table = fresh_table(1);
        table
            .insert(&[Value::Integer(0), Value::Integer(10)])
            .unwrap();
        let rows = table.scan().unwrap();
        assert_eq!(rows[0].values, vec![Value::Real(0.0), Value::Real(10.0)]);
    }

    /// Tiny seeded LCG — enough for a deterministic correctness fuzz
    /// without pulling in the `rand` crate.
    fn lcg(seed: u64) -> impl FnMut() -> u64 {
        let mut state = seed;
        move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        }
    }

    fn rand_f64(rng: &mut impl FnMut() -> u64) -> f64 {
        // Top 53 bits → uniform in [0, 1).
        ((rng() >> 11) as f64) / ((1u64 << 53) as f64)
    }

    /// Brute-force overlap reference used to validate `query_overlap`.
    fn brute_force(boxes: &[(i64, BoundingBox)], query: &BoundingBox) -> Vec<i64> {
        let mut out: Vec<i64> = boxes
            .iter()
            .filter(|(_, b)| b.intersects(query))
            .map(|(r, _)| *r)
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn rtree_correctness_fuzz() {
        // 10_000 random 2D boxes, 1_000 random query boxes, R-tree
        // result must match the brute-force reference exactly.
        let mut rng = lcg(0xDEAD_BEEF_CAFE_BABE);
        let table = fresh_table(2);
        let mut reference: Vec<(i64, BoundingBox)> = Vec::new();
        for _ in 0..10_000 {
            let x0 = rand_f64(&mut rng) * 1000.0;
            let y0 = rand_f64(&mut rng) * 1000.0;
            let w = rand_f64(&mut rng) * 5.0;
            let h = rand_f64(&mut rng) * 5.0;
            let coords = [x0, x0 + w, y0, y0 + h];
            let id = table
                .insert(&coords.iter().map(|f| Value::Real(*f)).collect::<Vec<_>>())
                .unwrap();
            reference.push((
                id,
                BoundingBox {
                    min: vec![coords[0], coords[2]],
                    max: vec![coords[1], coords[3]],
                },
            ));
        }
        for _ in 0..1_000 {
            let x0 = rand_f64(&mut rng) * 1000.0;
            let y0 = rand_f64(&mut rng) * 1000.0;
            let w = rand_f64(&mut rng) * 50.0;
            let h = rand_f64(&mut rng) * 50.0;
            let q = BoundingBox {
                min: vec![x0, y0],
                max: vec![x0 + w, y0 + h],
            };
            let expected = brute_force(&reference, &q);
            let actual = table.overlapping(&q.to_flat()).unwrap();
            assert_eq!(
                expected, actual,
                "rtree and brute force disagreed for query {:?}",
                q
            );
        }
    }
}
