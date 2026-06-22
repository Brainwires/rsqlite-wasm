//! `vec_index` — typed vector storage backed by an HNSW (Hierarchical
//! Navigable Small World) graph for sub-linear approximate-NN search.
//!
//! ```sql
//! CREATE VIRTUAL TABLE embeds USING vec_index(dim=384, metric=cosine);
//! INSERT INTO embeds VALUES (?vector_blob);
//! SELECT rowid FROM embeds
//!   ORDER BY vec_distance_cosine(vector, ?)
//!   LIMIT 10;
//! ```
//!
//! What this module provides over a plain `(rowid INTEGER PRIMARY KEY,
//! vector BLOB)` table:
//!
//! - **Typed declaration.** `dim=N, metric=M` are recorded at CREATE
//!   time. The `metric` parameter selects the distance function; `dim`
//!   lets us reject wrong-shape inserts immediately.
//! - **Strict insert validation.** The `vector` blob must decode to
//!   exactly `dim` floats. Rejecting at write time keeps queries from
//!   surfacing "vector dimension mismatch" errors at scan time.
//! - **HNSW-indexed lookup.** Inserts incrementally build a
//!   multi-layer navigable small-world graph; nearest-neighbor queries
//!   traverse the graph from a sparse top layer down to layer 0,
//!   returning ANN results in `O(log N)` expected time. The planner
//!   pushes `SELECT rowid FROM t ORDER BY vec_distance_<metric>(col,
//!   ?) LIMIT k` into [`VecIndexTable::nearest`] directly, skipping the
//!   outer Sort.
//!
//! Tunables (passed at CREATE time, all optional):
//! - `m=N` — max graph degree per layer above 0 (default 16).
//!   Layer 0 caps degree at `2 * m`.
//! - `ef_construction=N` — candidate-pool size during inserts
//!   (default 200). Higher → better recall, slower build.
//! - `ef=N` — candidate-pool size at query time (default 50).
//!   Higher → better recall, slower query.

use std::cell::RefCell;
use std::rc::Rc;

use rsqlite_storage::codec::Value;

use crate::error::{Error, Result};
use crate::types::Row;

use super::{Module, VirtualTable};

/// Distance metric declared by the user at CREATE time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VecMetric {
    Cosine,
    L2,
    Dot,
}

impl VecMetric {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "cosine" => Some(VecMetric::Cosine),
            "l2" | "euclidean" => Some(VecMetric::L2),
            "dot" => Some(VecMetric::Dot),
            _ => None,
        }
    }
}

pub(super) struct VecIndexModule;

impl Module for VecIndexModule {
    fn name(&self) -> &str {
        "vec_index"
    }

    fn create(&self, _table_name: &str, args: &[String]) -> Result<Rc<dyn VirtualTable>> {
        // Args are `key=value` pairs. Required: `dim`. Optional:
        // `metric` (default cosine), `m` (default 16), `ef`
        // (default 50), `ef_construction` (default 200).
        let mut dim: Option<usize> = None;
        let mut metric = VecMetric::Cosine;
        let mut m: usize = 16;
        let mut ef_search: usize = 50;
        let mut ef_construction: usize = 200;
        for arg in args {
            let eq = arg.find('=').ok_or_else(|| {
                Error::Other(format!(
                    "vec_index: expected `key=value` argument, got {arg:?}"
                ))
            })?;
            let key = arg[..eq].trim().to_ascii_lowercase();
            let val = arg[eq + 1..].trim();
            let parse_pos = |label: &str, raw: &str| -> Result<usize> {
                let n: usize = raw.parse().map_err(|_| {
                    Error::Other(format!(
                        "vec_index: {label} must be a positive integer, got {raw:?}"
                    ))
                })?;
                if n == 0 {
                    return Err(Error::Other(format!("vec_index: {label} must be > 0")));
                }
                Ok(n)
            };
            match key.as_str() {
                "dim" => dim = Some(parse_pos("dim", val)?),
                "metric" => {
                    metric = VecMetric::parse(val).ok_or_else(|| {
                        Error::Other(format!(
                            "vec_index: metric must be one of cosine, l2, dot — got {val:?}"
                        ))
                    })?;
                }
                "m" => m = parse_pos("m", val)?,
                "ef" | "ef_search" => ef_search = parse_pos("ef", val)?,
                "ef_construction" | "ef_construct" => {
                    ef_construction = parse_pos("ef_construction", val)?
                }
                _ => {
                    return Err(Error::Other(format!(
                        "vec_index: unknown argument key {key:?}"
                    )));
                }
            }
        }
        let dim =
            dim.ok_or_else(|| Error::Other("vec_index: missing required argument `dim=N`".into()))?;
        let m_max0 = m.saturating_mul(2);
        // Stable seed so test runs are deterministic; the graph
        // topology depends only on insert order, not wall-clock RNG
        // state.
        let rng_seed = 0x9E37_79B9_7F4A_7C15u64;
        Ok(Rc::new(VecIndexTable {
            dim,
            metric,
            index: RefCell::new(HnswIndex {
                nodes: Vec::new(),
                m,
                m_max0,
                ef_construction,
                ef_search,
                level_mult: 1.0 / (m as f64).max(2.0).ln(),
                entry_point: None,
                rng_state: rng_seed,
            }),
        }))
    }
}

pub(crate) struct VecIndexTable {
    dim: usize,
    metric: VecMetric,
    index: RefCell<HnswIndex>,
}

impl VecIndexTable {
    /// k-nearest-neighbor lookup served by the HNSW graph. Returns
    /// `(rowid, distance)` pairs sorted ascending. `k == 0` returns
    /// every stored row (used by tests that compare against
    /// brute-force ground truth).
    pub fn nearest(&self, query: &[f32], k: usize) -> Result<Vec<(i64, f64)>> {
        if query.len() != self.dim {
            return Err(Error::Other(format!(
                "vec_index: query has {} dims, table declared {}",
                query.len(),
                self.dim
            )));
        }
        let index = self.index.borrow();
        Ok(index.search(query, k, self.metric))
    }
}

// ── HNSW core ─────────────────────────────────────────────────────────

type NodeId = u32;

struct HnswNode {
    rowid: i64,
    vector: Vec<f32>,
    /// Outer index = layer; inner = neighbor NodeIds at that layer.
    /// `neighbors[0]` is layer 0; `neighbors.len() - 1` is the node's
    /// top layer.
    neighbors: Vec<Vec<NodeId>>,
}

impl HnswNode {
    fn level(&self) -> usize {
        self.neighbors.len() - 1
    }
}

struct HnswIndex {
    nodes: Vec<HnswNode>,
    m: usize,
    m_max0: usize,
    ef_construction: usize,
    ef_search: usize,
    /// `1 / ln(M)` — converts a uniform [0,1) sample to a
    /// geometric-distribution layer pick.
    level_mult: f64,
    entry_point: Option<NodeId>,
    /// SplitMix64 state. We avoid pulling in `rand` (not a dep) and
    /// don't need cryptographic randomness — just a reproducible
    /// stream of u64s.
    rng_state: u64,
}

impl HnswIndex {
    fn distance(&self, a: &[f32], b: &[f32], metric: VecMetric) -> f64 {
        match metric {
            VecMetric::Cosine => crate::eval_helpers::cosine_distance(a, b),
            VecMetric::L2 => crate::eval_helpers::l2_distance(a, b),
            // Negate so "ascending distance" still ranks larger dot
            // products first — matches the SQL `vec_distance_dot`
            // convention.
            VecMetric::Dot => -dot_product(a, b),
        }
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64. Tiny, well-distributed.
        self.rng_state = self.rng_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Sample a level via geometric distribution: `floor(-ln(U) *
    /// level_mult)` where `U ~ Uniform(0,1)`. Yields layer 0 with
    /// probability `1 - 1/M`, layer 1 with `(1/M)(1 - 1/M)`, etc.
    fn random_level(&mut self) -> usize {
        let raw = self.next_u64();
        // 53-bit precision → U ∈ (0, 1].
        let mantissa = (raw >> 11) as f64;
        let u = (mantissa + 1.0) / ((1u64 << 53) as f64 + 1.0);
        let lvl = (-u.ln() * self.level_mult).floor() as i64;
        // Clamp so we don't run away on a tiny u; 16 layers is plenty
        // for billion-row graphs.
        lvl.clamp(0, 16) as usize
    }

    fn insert(&mut self, rowid: i64, vector: Vec<f32>, metric: VecMetric) {
        let new_id = self.nodes.len() as NodeId;
        let new_level = self.random_level();

        // First node: just seat it.
        if self.entry_point.is_none() {
            let mut neighbors = Vec::with_capacity(new_level + 1);
            for _ in 0..=new_level {
                neighbors.push(Vec::new());
            }
            self.nodes.push(HnswNode {
                rowid,
                vector,
                neighbors,
            });
            self.entry_point = Some(new_id);
            return;
        }

        let ep_id = self.entry_point.unwrap();
        let ep_level = self.nodes[ep_id as usize].level();

        // Greedy descent from ep down to (new_level + 1) — single
        // best-neighbor search per layer.
        let mut cur = ep_id;
        let mut cur_dist = self.distance(&vector, &self.nodes[cur as usize].vector, metric);
        let mut layer = ep_level;
        while layer > new_level {
            let (next, next_dist) = self.greedy_search_layer(cur, cur_dist, &vector, layer, metric);
            cur = next;
            cur_dist = next_dist;
            if layer == 0 {
                break;
            }
            layer -= 1;
        }

        // Insert new node — allocate its neighbor lists up front.
        let mut new_neighbors = Vec::with_capacity(new_level + 1);
        for _ in 0..=new_level {
            new_neighbors.push(Vec::new());
        }
        self.nodes.push(HnswNode {
            rowid,
            vector,
            neighbors: new_neighbors,
        });

        // For layers ≤ new_level: ef_construction candidate-pool
        // search, then connect to the M (or M_max0) closest,
        // bidirectionally.
        let new_vec_clone = self.nodes[new_id as usize].vector.clone();
        let mut entries = vec![(cur, cur_dist)];
        let start_layer = std::cmp::min(new_level, ep_level);
        for layer in (0..=start_layer).rev() {
            let candidates = self.search_layer_pool(
                &entries,
                &new_vec_clone,
                self.ef_construction,
                layer,
                metric,
            );
            // Pick best M (or 2M for layer 0) to connect.
            let cap = if layer == 0 { self.m_max0 } else { self.m };
            let selected = select_neighbors_simple(&candidates, cap);

            // Forward links: new_id → selected. Bidirectional links:
            // selected[i] → new_id, with the receiving node pruned
            // back to its degree cap if exceeded.
            for &(nb, _) in &selected {
                self.nodes[new_id as usize].neighbors[layer].push(nb);
                self.nodes[nb as usize].neighbors[layer].push(new_id);
                let nb_cap = if layer == 0 { self.m_max0 } else { self.m };
                if self.nodes[nb as usize].neighbors[layer].len() > nb_cap {
                    self.prune_neighbors(nb, layer, nb_cap, metric);
                }
            }

            // Carry forward the candidate pool as the entry set for
            // the next-lower layer. `candidates` is the full ef
            // pool, sorted best-first — perfect for seeding a deeper
            // search.
            entries = candidates;
        }

        // Promote entry_point if this node lives on a higher layer
        // than the current entry.
        if new_level > ep_level {
            self.entry_point = Some(new_id);
        }
    }

    /// Single-best greedy walk on one layer: keep stepping to the
    /// closest neighbor until no neighbor is closer than where we
    /// stand.
    fn greedy_search_layer(
        &self,
        entry: NodeId,
        entry_dist: f64,
        target: &[f32],
        layer: usize,
        metric: VecMetric,
    ) -> (NodeId, f64) {
        let mut cur = entry;
        let mut cur_dist = entry_dist;
        loop {
            let neighbors = self.nodes[cur as usize]
                .neighbors
                .get(layer)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let mut improved = false;
            for &nb in neighbors {
                let d = self.distance(target, &self.nodes[nb as usize].vector, metric);
                if d < cur_dist {
                    cur_dist = d;
                    cur = nb;
                    improved = true;
                }
            }
            if !improved {
                return (cur, cur_dist);
            }
        }
    }

    /// `searchLayer` from the HNSW paper: keep an ef-bounded result
    /// pool. Returns the pool sorted ascending by distance.
    fn search_layer_pool(
        &self,
        entries: &[(NodeId, f64)],
        target: &[f32],
        ef: usize,
        layer: usize,
        metric: VecMetric,
    ) -> Vec<(NodeId, f64)> {
        let mut visited = vec![false; self.nodes.len()];
        // Result pool: max-heap on distance — we evict the worst when
        // exceeding ef.
        let mut results: std::collections::BinaryHeap<HeapItem> =
            std::collections::BinaryHeap::new();
        // Candidates: min-heap (next-closest to expand). Wrap in
        // `Reverse` to invert.
        let mut candidates: std::collections::BinaryHeap<std::cmp::Reverse<HeapItem>> =
            std::collections::BinaryHeap::new();

        for &(id, d) in entries {
            if (id as usize) >= visited.len() || visited[id as usize] {
                continue;
            }
            visited[id as usize] = true;
            results.push(HeapItem { id, dist: d });
            candidates.push(std::cmp::Reverse(HeapItem { id, dist: d }));
        }

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            // If the closest unexplored candidate is already worse
            // than the worst result we've kept, we can stop.
            if let Some(worst) = results.peek() {
                if c.dist > worst.dist && results.len() >= ef {
                    break;
                }
            }
            let neighbors = self.nodes[c.id as usize]
                .neighbors
                .get(layer)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            for &nb in neighbors {
                if visited[nb as usize] {
                    continue;
                }
                visited[nb as usize] = true;
                let d = self.distance(target, &self.nodes[nb as usize].vector, metric);
                let push_it = if results.len() < ef {
                    true
                } else {
                    results.peek().map(|w| d < w.dist).unwrap_or(true)
                };
                if push_it {
                    results.push(HeapItem { id: nb, dist: d });
                    candidates.push(std::cmp::Reverse(HeapItem { id: nb, dist: d }));
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        let mut out: Vec<(NodeId, f64)> = results.into_iter().map(|h| (h.id, h.dist)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        out
    }

    /// Trim a node's neighbor list at `layer` back to `cap` by keeping
    /// the `cap` closest neighbors.
    fn prune_neighbors(&mut self, node: NodeId, layer: usize, cap: usize, metric: VecMetric) {
        let node_vec = self.nodes[node as usize].vector.clone();
        let nbs = std::mem::take(&mut self.nodes[node as usize].neighbors[layer]);
        let mut scored: Vec<(NodeId, f64)> = nbs
            .into_iter()
            .map(|n| {
                let d = self.distance(&node_vec, &self.nodes[n as usize].vector, metric);
                (n, d)
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(cap);
        self.nodes[node as usize].neighbors[layer] = scored.into_iter().map(|(n, _)| n).collect();
    }

    fn search(&self, query: &[f32], k: usize, metric: VecMetric) -> Vec<(i64, f64)> {
        let Some(ep) = self.entry_point else {
            return Vec::new();
        };
        let ep_level = self.nodes[ep as usize].level();
        // Greedy descent down to layer 1.
        let mut cur = ep;
        let mut cur_dist = self.distance(query, &self.nodes[cur as usize].vector, metric);
        let mut layer = ep_level;
        while layer > 0 {
            let (n, d) = self.greedy_search_layer(cur, cur_dist, query, layer, metric);
            cur = n;
            cur_dist = d;
            layer -= 1;
        }
        // ef-pool search at layer 0.
        let ef = std::cmp::max(self.ef_search, k);
        let pool = self.search_layer_pool(&[(cur, cur_dist)], query, ef, 0, metric);
        let mut out: Vec<(i64, f64)> = pool
            .into_iter()
            .map(|(id, d)| (self.nodes[id as usize].rowid, d))
            .collect();
        if k > 0 && out.len() > k {
            out.truncate(k);
        }
        out
    }

    fn len(&self) -> usize {
        self.nodes.len()
    }
}

#[derive(Clone, Copy)]
struct HeapItem {
    id: NodeId,
    dist: f64,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist && self.id == other.id
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Total order — distances are non-NaN (cosine clamps via
        // norms; l2 is ≥ 0; dot is finite for finite inputs).
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.id.cmp(&other.id))
    }
}

fn select_neighbors_simple(candidates: &[(NodeId, f64)], cap: usize) -> Vec<(NodeId, f64)> {
    candidates.iter().take(cap).cloned().collect()
}

fn dot_product(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (*x as f64) * (*y as f64))
        .sum()
}

// ── VirtualTable impl ─────────────────────────────────────────────────

impl VirtualTable for VecIndexTable {
    fn columns(&self) -> Vec<String> {
        vec!["vector".to_string()]
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn scan(&self) -> Result<Vec<Row>> {
        let index = self.index.borrow();
        let n = index.len();
        let mut rows = Vec::with_capacity(n);
        for node in &index.nodes {
            let bytes = f32_slice_to_blob(&node.vector);
            rows.push(Row::with_rowid(vec![Value::Blob(bytes)], node.rowid));
        }
        // Stable: emit rows in rowid order.
        rows.sort_by_key(|r| r.rowid.unwrap_or(0));
        Ok(rows)
    }

    fn insert(&self, values: &[Value]) -> Result<i64> {
        if values.len() != 1 {
            return Err(Error::Other(format!(
                "vec_index: INSERT expects 1 column (vector), got {}",
                values.len()
            )));
        }
        let blob = match &values[0] {
            Value::Blob(b) => b.as_slice(),
            Value::Null => {
                return Err(Error::Other(
                    "vec_index: vector column does not accept NULL".into(),
                ));
            }
            _ => {
                return Err(Error::Other(
                    "vec_index: vector column requires a BLOB value".into(),
                ));
            }
        };
        let parsed = crate::eval_helpers::blob_to_f32_vec(blob)?;
        if parsed.len() != self.dim {
            return Err(Error::Other(format!(
                "vec_index: vector dimension mismatch — got {}, expected {}",
                parsed.len(),
                self.dim
            )));
        }
        let metric = self.metric;
        let mut index = self.index.borrow_mut();
        let new_rowid = (index.len() as i64) + 1;
        index.insert(new_rowid, parsed, metric);
        Ok(new_rowid)
    }
}

fn f32_slice_to_blob(slice: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(slice.len() * 4);
    for v in slice {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec_blob(values: &[f32]) -> Vec<u8> {
        f32_slice_to_blob(values)
    }

    fn fresh_table(dim: usize, metric: VecMetric) -> Rc<dyn VirtualTable> {
        let m = VecIndexModule;
        let metric_arg = match metric {
            VecMetric::Cosine => "cosine",
            VecMetric::L2 => "l2",
            VecMetric::Dot => "dot",
        };
        m.create("e", &[format!("dim={dim}"), format!("metric={metric_arg}")])
            .unwrap()
    }

    /// Pull a `&VecIndexTable` out of an `Rc<dyn VirtualTable>` we
    /// just created. Tests need access to `nearest()` directly to
    /// compare against ground truth; the pushdown path through
    /// `best_index` / `execute_plan` is exercised by integration
    /// tests in `database_tests/scalar_extras.rs`.
    fn as_vec_index(t: &Rc<dyn VirtualTable>) -> &VecIndexTable {
        // SAFETY: tests build this via `VecIndexModule::create`,
        // which always wraps a `VecIndexTable`. Rc::as_ptr returns a
        // valid pointer to the wrapped trait object's data.
        unsafe { &*(Rc::as_ptr(t) as *const VecIndexTable) }
    }

    #[test]
    fn create_with_dim_and_metric_args() {
        let m = VecIndexModule;
        let t = m
            .create("e", &["dim=3".into(), "metric=l2".into()])
            .unwrap();
        // Empty until populated.
        assert_eq!(t.scan().unwrap().len(), 0);
    }

    #[test]
    fn create_rejects_missing_dim() {
        let m = VecIndexModule;
        assert!(m.create("e", &[]).is_err());
        assert!(m.create("e", &["metric=cosine".into()]).is_err());
    }

    #[test]
    fn create_accepts_hnsw_tunables() {
        let m = VecIndexModule;
        assert!(
            m.create(
                "e",
                &[
                    "dim=4".into(),
                    "m=8".into(),
                    "ef=32".into(),
                    "ef_construction=64".into(),
                ],
            )
            .is_ok()
        );
    }

    #[test]
    fn insert_validates_dimension() {
        let m = VecIndexModule;
        let t = m.create("e", &["dim=3".into()]).unwrap();
        // Right shape — accepted.
        t.insert(&[Value::Blob(vec_blob(&[1.0, 0.0, 0.0]))])
            .unwrap();
        // Wrong shape — rejected at write time.
        assert!(t.insert(&[Value::Blob(vec_blob(&[1.0, 0.0]))]).is_err());
    }

    #[test]
    fn insert_rejects_null_and_non_blob() {
        let m = VecIndexModule;
        let t = m.create("e", &["dim=2".into()]).unwrap();
        assert!(t.insert(&[Value::Null]).is_err());
        assert!(t.insert(&[Value::Integer(7)]).is_err());
    }

    #[test]
    fn nearest_hnsw_cosine_small() {
        let table = fresh_table(3, VecMetric::Cosine);
        // 3 unit vectors along axes.
        table
            .insert(&[Value::Blob(vec_blob(&[1.0, 0.0, 0.0]))])
            .unwrap();
        table
            .insert(&[Value::Blob(vec_blob(&[0.0, 1.0, 0.0]))])
            .unwrap();
        table
            .insert(&[Value::Blob(vec_blob(&[0.0, 0.0, 1.0]))])
            .unwrap();

        let res = as_vec_index(&table).nearest(&[0.9, 0.1, 0.0], 2).unwrap();
        assert_eq!(res.len(), 2);
        // Nearest is rowid 1 (1,0,0) — small angle to (0.9, 0.1, 0.0).
        assert_eq!(res[0].0, 1);
        assert!(res[0].1 < res[1].1);
    }

    #[test]
    fn nearest_l2_distances_small() {
        let table = fresh_table(2, VecMetric::L2);
        table.insert(&[Value::Blob(vec_blob(&[0.0, 0.0]))]).unwrap();
        table.insert(&[Value::Blob(vec_blob(&[3.0, 4.0]))]).unwrap();
        table.insert(&[Value::Blob(vec_blob(&[1.0, 1.0]))]).unwrap();

        let res = as_vec_index(&table).nearest(&[0.0, 0.0], 0).unwrap();
        assert_eq!(res.len(), 3);
        assert_eq!(res[0].0, 1); // distance 0
        assert_eq!(res[1].0, 3); // sqrt(2) ≈ 1.41
        assert_eq!(res[2].0, 2); // 5.0
    }

    #[test]
    fn nearest_dot_metric_returns_largest_dot_first() {
        let table = fresh_table(2, VecMetric::Dot);
        table.insert(&[Value::Blob(vec_blob(&[1.0, 0.0]))]).unwrap(); // dot with (2,1) = 2
        table.insert(&[Value::Blob(vec_blob(&[0.5, 1.0]))]).unwrap(); // dot = 2
        table.insert(&[Value::Blob(vec_blob(&[2.0, 1.0]))]).unwrap(); // dot = 5

        let res = as_vec_index(&table).nearest(&[2.0, 1.0], 1).unwrap();
        assert_eq!(res.len(), 1);
        // Largest dot product wins — that's rowid 3.
        assert_eq!(res[0].0, 3);
    }

    #[test]
    fn scan_round_trips_inserted_vectors() {
        let m = VecIndexModule;
        let t = m.create("e", &["dim=2".into()]).unwrap();
        t.insert(&[Value::Blob(vec_blob(&[1.5, 2.5]))]).unwrap();
        t.insert(&[Value::Blob(vec_blob(&[-0.5, 7.0]))]).unwrap();
        let rows = t.scan().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rowid, Some(1));
        assert_eq!(rows[1].rowid, Some(2));
        if let Value::Blob(b) = &rows[0].values[0] {
            let decoded = crate::eval_helpers::blob_to_f32_vec(b).unwrap();
            assert_eq!(decoded, vec![1.5, 2.5]);
        } else {
            panic!("expected blob");
        }
    }

    #[test]
    fn unknown_arg_is_an_error() {
        let m = VecIndexModule;
        assert!(m.create("e", &["dim=3".into(), "bogus=1".into()]).is_err());
        assert!(
            m.create("e", &["dim=3".into(), "metric=manhattan".into()])
                .is_err()
        );
    }

    #[test]
    fn hnsw_recall_at_10_above_95_percent() {
        // 1000 random 384-dim vectors → 100 random queries; compare
        // top-10 from HNSW against brute-force ground truth.
        const N: usize = 1000;
        const D: usize = 384;
        const QN: usize = 100;
        const K: usize = 10;

        let m = VecIndexModule;
        let t = m
            .create(
                "e",
                &[
                    format!("dim={D}"),
                    "metric=cosine".into(),
                    "m=16".into(),
                    "ef=128".into(),
                    "ef_construction=200".into(),
                ],
            )
            .unwrap();

        // Reproducible LCG for the test data — independent of the
        // index's own RNG.
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let mut next_f32 = || -> f32 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 32) as u32 as f32 / u32::MAX as f32) * 2.0 - 1.0
        };

        // Insert N vectors.
        let mut all_vectors: Vec<Vec<f32>> = Vec::with_capacity(N);
        for _ in 0..N {
            let v: Vec<f32> = (0..D).map(|_| next_f32()).collect();
            t.insert(&[Value::Blob(vec_blob(&v))]).unwrap();
            all_vectors.push(v);
        }

        let mut total_hits = 0usize;
        for _ in 0..QN {
            let q: Vec<f32> = (0..D).map(|_| next_f32()).collect();
            // Ground truth: brute force.
            let mut gt: Vec<(i64, f64)> = (0..N)
                .map(|i| {
                    let d = crate::eval_helpers::cosine_distance(&all_vectors[i], &q);
                    ((i as i64) + 1, d)
                })
                .collect();
            gt.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            gt.truncate(K);
            let gt_set: std::collections::HashSet<i64> = gt.into_iter().map(|(r, _)| r).collect();

            let approx = as_vec_index(&t).nearest(&q, K).unwrap();
            for (rid, _) in approx {
                if gt_set.contains(&rid) {
                    total_hits += 1;
                }
            }
        }

        let recall = total_hits as f64 / (QN * K) as f64;
        assert!(recall >= 0.95, "expected recall@10 ≥ 0.95, got {recall}");
        eprintln!("hnsw recall@10 = {recall}");
    }
}
