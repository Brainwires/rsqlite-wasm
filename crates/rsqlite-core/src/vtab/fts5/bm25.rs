//! Standard Okapi BM25 scoring.
//!
//! ```text
//! score(q, D) = Σ_term idf(t) · ((k1+1) · tf) / (k1 · ((1-b) + b · |D|/avgdl) + tf)
//! idf(t)      = ln( (N - df(t) + 0.5) / (df(t) + 0.5) + 1 )
//! ```
//!
//! Constants follow common defaults: `k1 = 1.2`, `b = 0.75`. We use
//! the `+ 1` form of IDF to keep the score non-negative (mirrors
//! Lucene / SQLite's FTS5 convention).
//!
//! The function expects `query_terms` already tokenized through the
//! same pipeline as the index — case-folded and diacritic-stripped —
//! and `col_weights` to be aligned with the `matches` slice (one
//! per column, multiplied through the per-column subtotal). For the
//! single-column case pass `&[1.0]`.

use std::collections::HashMap;

use super::inverted_index::{InvertedIndex, RowId};

const K1: f64 = 1.2;
const B: f64 = 0.75;

/// Score one column's worth of matches (rowid → matched positions).
///
/// `query_terms` is the unique tokenized query terms after the query
/// parser collapsed phrases/prefix expansion. Each is looked up
/// directly in the index for its document frequency. Returns rowid →
/// score (descending by score).
pub fn bm25_score(
    matches: &[(RowId, Vec<u32>)],
    idx: &InvertedIndex,
    query_terms: &[String],
) -> Vec<(RowId, f64)> {
    if matches.is_empty() || query_terms.is_empty() {
        return matches.iter().map(|(r, _)| (*r, 0.0)).collect();
    }
    let n = idx.total_docs.max(1) as f64;
    let avgdl = idx.avg_doc_length().max(1.0);
    // Pre-compute IDF per term.
    let idf: HashMap<&str, f64> = query_terms
        .iter()
        .map(|t| {
            let df = idx.doc_frequency(t) as f64;
            let val = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
            (t.as_str(), val)
        })
        .collect();

    let mut scored: Vec<(RowId, f64)> = matches
        .iter()
        .map(|(rowid, _)| {
            let dl = idx.doc_lengths.get(rowid).copied().unwrap_or(0) as f64;
            let mut score = 0.0_f64;
            for term in query_terms {
                let tf = term_frequency(idx, term, *rowid) as f64;
                if tf <= 0.0 {
                    continue;
                }
                let term_idf = idf.get(term.as_str()).copied().unwrap_or(0.0);
                let numer = tf * (K1 + 1.0);
                let denom = tf + K1 * (1.0 - B + B * dl / avgdl);
                score += term_idf * (numer / denom);
            }
            (*rowid, score)
        })
        .collect();
    // Highest score first; rowid as a stable tiebreaker.
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    scored
}

/// Per-column scoring path used by the multi-column FTS5 vtab. Picks
/// each rowid's max-weighted score across columns rather than a sum,
/// matching SQLite's `bm25(weight1, weight2, ...)` accumulator.
#[allow(dead_code)]
pub fn bm25_combine(per_col: Vec<(f32, Vec<(RowId, f64)>)>) -> Vec<(RowId, f64)> {
    let mut acc: HashMap<RowId, f64> = HashMap::new();
    for (weight, scores) in per_col {
        let w = weight as f64;
        for (rid, s) in scores {
            let entry = acc.entry(rid).or_insert(0.0);
            *entry += s * w;
        }
    }
    let mut out: Vec<(RowId, f64)> = acc.into_iter().collect();
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    out
}

fn term_frequency(idx: &InvertedIndex, term: &str, rowid: RowId) -> u32 {
    idx.tokens
        .get(term)
        .and_then(|m| m.get(&rowid))
        .map(|pl| pl.positions.len() as u32)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vtab::fts5::tokenizer::tokenize;

    #[test]
    fn rare_term_outscores_common_term() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("the quick rare gem"));
        idx.upsert(2, &tokenize("the quick brown fox"));
        idx.upsert(3, &tokenize("the the the the"));

        // `gem` is rare → row 1 should score higher than rows
        // matching the common term `quick`.
        let matches: Vec<(RowId, Vec<u32>)> = vec![(1, vec![2]), (2, vec![1])];
        let s = bm25_score(&matches, &idx, &["gem".into()]);
        assert!(s[0].0 == 1);
    }

    #[test]
    fn empty_inputs_score_zero() {
        let idx = InvertedIndex::new();
        let s = bm25_score(&[], &idx, &["x".into()]);
        assert!(s.is_empty());
    }

    #[test]
    fn frequency_boosts_score() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("apple"));
        idx.upsert(2, &tokenize("apple apple apple apple"));
        let matches: Vec<(RowId, Vec<u32>)> = vec![(1, vec![0]), (2, vec![0, 1, 2, 3])];
        let s = bm25_score(&matches, &idx, &["apple".into()]);
        assert_eq!(s[0].0, 2, "doc 2 should rank higher (more occurrences)");
    }

    #[test]
    fn shorter_doc_outscores_longer_for_equal_tf() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("apple"));
        idx.upsert(2, &tokenize("apple b c d e f g h i j"));
        let matches: Vec<(RowId, Vec<u32>)> = vec![(1, vec![0]), (2, vec![0])];
        let s = bm25_score(&matches, &idx, &["apple".into()]);
        assert_eq!(s[0].0, 1, "shorter doc should win on equal tf");
    }
}
