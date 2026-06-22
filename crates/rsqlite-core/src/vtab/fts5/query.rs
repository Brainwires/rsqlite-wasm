//! Parser + evaluator for SQLite-FTS5-style query strings.
//!
//! Supported grammar (a strict subset of FTS5):
//!
//! ```text
//! query    := or_expr
//! or_expr  := and_expr ( OR and_expr )*
//! and_expr := atom ( atom )*           -- juxtaposition = AND
//! atom     := term
//!           | "phrase phrase"          -- quoted phrase
//!           | term*                    -- prefix wildcard
//!           | NEAR ( term1 term2 ..., N )
//! term     := identifier
//! ```
//!
//! - `OR` is the keyword `OR` in any case.
//! - Whitespace between terms means AND.
//! - Phrases are double-quoted; consecutive tokens must appear in
//!   order, adjacent in the source positions.
//! - `term*` is a wildcard prefix; matches every term beginning with
//!   `term`.
//! - `NEAR(t1 t2 ..., N)` matches when every listed term appears
//!   within `N` token positions of the others.

use std::collections::{BTreeMap, BTreeSet};

use super::inverted_index::{InvertedIndex, PostingList, RowId};
use super::tokenizer::clean as clean_token;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryExpr {
    Term(String),
    Phrase(Vec<String>),
    Prefix(String),
    Near(Vec<String>, u32),
    And(Vec<QueryExpr>),
    Or(Vec<QueryExpr>),
}

/// Parse a user-supplied FTS5 query string. Returns `None` for an
/// empty query (caller decides — match-everything or match-nothing).
pub fn parse(input: &str) -> Result<Option<QueryExpr>, String> {
    let mut p = Parser::new(input);
    let expr = p.parse_or()?;
    p.skip_ws();
    if !p.is_done() {
        return Err(format!(
            "fts5 query: trailing input at offset {}: {:?}",
            p.pos,
            &p.src[p.pos..]
        ));
    }
    Ok(expr)
}

struct Parser<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            src: s,
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() && (self.bytes[self.pos] as char).is_whitespace() {
            self.pos += 1;
        }
    }

    fn is_done(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<char> {
        self.bytes.get(self.pos).map(|b| *b as char)
    }

    fn parse_or(&mut self) -> Result<Option<QueryExpr>, String> {
        let mut parts = Vec::new();
        if let Some(first) = self.parse_and()? {
            parts.push(first);
        }
        loop {
            self.skip_ws();
            if !self.consume_keyword("OR") {
                break;
            }
            match self.parse_and()? {
                Some(rhs) => parts.push(rhs),
                None => return Err("fts5 query: empty RHS after OR".into()),
            }
        }
        Ok(match parts.len() {
            0 => None,
            1 => Some(parts.into_iter().next().unwrap()),
            _ => Some(QueryExpr::Or(parts)),
        })
    }

    fn parse_and(&mut self) -> Result<Option<QueryExpr>, String> {
        let mut parts = Vec::new();
        loop {
            self.skip_ws();
            if self.is_done() {
                break;
            }
            // Stop at OR — let the OR parser drive.
            if self.looking_at_keyword("OR") {
                break;
            }
            match self.parse_atom()? {
                Some(a) => parts.push(a),
                None => break,
            }
        }
        Ok(match parts.len() {
            0 => None,
            1 => Some(parts.into_iter().next().unwrap()),
            _ => Some(QueryExpr::And(parts)),
        })
    }

    fn parse_atom(&mut self) -> Result<Option<QueryExpr>, String> {
        self.skip_ws();
        let Some(ch) = self.peek() else {
            return Ok(None);
        };
        if ch == '"' {
            return self.parse_phrase().map(Some);
        }
        // NEAR(...) — case-insensitive keyword followed by '('.
        if (ch == 'n' || ch == 'N') && self.looking_at_near() {
            return self.parse_near().map(Some);
        }
        // Identifier / wildcard.
        if is_term_start(ch) {
            return self.parse_term_or_prefix().map(Some);
        }
        Err(format!(
            "fts5 query: unexpected character {:?} at offset {}",
            ch, self.pos
        ))
    }

    fn parse_phrase(&mut self) -> Result<QueryExpr, String> {
        debug_assert_eq!(self.peek(), Some('"'));
        self.pos += 1;
        let start = self.pos;
        while self.pos < self.bytes.len() && (self.bytes[self.pos] as char) != '"' {
            self.pos += 1;
        }
        if self.pos >= self.bytes.len() {
            return Err("fts5 query: unterminated phrase".into());
        }
        let raw = &self.src[start..self.pos];
        self.pos += 1; // consume closing quote
        let tokens: Vec<String> = raw
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(clean_token)
            .filter(|s| !s.is_empty())
            .collect();
        if tokens.is_empty() {
            return Err("fts5 query: empty phrase".into());
        }
        Ok(QueryExpr::Phrase(tokens))
    }

    fn parse_term_or_prefix(&mut self) -> Result<QueryExpr, String> {
        let start = self.pos;
        while self.pos < self.bytes.len() {
            let c = self.bytes[self.pos] as char;
            if !is_term_continue(c) {
                break;
            }
            self.pos += 1;
        }
        let raw = &self.src[start..self.pos];
        let cleaned = clean_token(raw);
        if cleaned.is_empty() {
            return Err(format!("fts5 query: empty term at offset {}", start));
        }
        // Optional `*` for prefix match.
        if self.peek() == Some('*') {
            self.pos += 1;
            return Ok(QueryExpr::Prefix(cleaned));
        }
        Ok(QueryExpr::Term(cleaned))
    }

    fn looking_at_near(&self) -> bool {
        let bs = self.bytes;
        if self.pos + 4 > bs.len() {
            return false;
        }
        let head = &bs[self.pos..self.pos + 4];
        if !(head[0].eq_ignore_ascii_case(&b'N')
            && head[1].eq_ignore_ascii_case(&b'E')
            && head[2].eq_ignore_ascii_case(&b'A')
            && head[3].eq_ignore_ascii_case(&b'R'))
        {
            return false;
        }
        // Next non-ws char must be '('.
        let mut k = self.pos + 4;
        while k < bs.len() && (bs[k] as char).is_whitespace() {
            k += 1;
        }
        k < bs.len() && bs[k] == b'('
    }

    fn parse_near(&mut self) -> Result<QueryExpr, String> {
        // Caller verified.
        self.pos += 4;
        self.skip_ws();
        if self.peek() != Some('(') {
            return Err("fts5 query: expected '(' after NEAR".into());
        }
        self.pos += 1;
        let mut terms = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(')') {
                self.pos += 1;
                break;
            }
            if self.peek() == Some(',') {
                self.pos += 1;
                self.skip_ws();
                let n = self.parse_unsigned()?;
                self.skip_ws();
                if self.peek() != Some(')') {
                    return Err("fts5 query: expected ')' after NEAR distance".into());
                }
                self.pos += 1;
                if terms.is_empty() {
                    return Err("fts5 query: NEAR requires at least 2 terms".into());
                }
                return Ok(QueryExpr::Near(terms, n));
            }
            let t = self.read_word()?;
            terms.push(t);
        }
        // Reached `)` with no `,` — default distance.
        if terms.len() < 2 {
            return Err("fts5 query: NEAR requires at least 2 terms".into());
        }
        Ok(QueryExpr::Near(terms, 10))
    }

    fn read_word(&mut self) -> Result<String, String> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.bytes.len() {
            let c = self.bytes[self.pos] as char;
            if !is_term_continue(c) {
                break;
            }
            self.pos += 1;
        }
        if self.pos == start {
            return Err(format!("fts5 query: expected word at offset {}", start));
        }
        let raw = &self.src[start..self.pos];
        let cleaned = clean_token(raw);
        if cleaned.is_empty() {
            return Err(format!("fts5 query: empty word at offset {}", start));
        }
        Ok(cleaned)
    }

    fn parse_unsigned(&mut self) -> Result<u32, String> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.bytes.len() && (self.bytes[self.pos] as char).is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(format!("fts5 query: expected number at offset {}", start));
        }
        self.src[start..self.pos]
            .parse::<u32>()
            .map_err(|e| format!("fts5 query: bad number: {e}"))
    }

    fn looking_at_keyword(&self, kw: &str) -> bool {
        let bs = self.bytes;
        let kbs = kw.as_bytes();
        if self.pos + kbs.len() > bs.len() {
            return false;
        }
        for i in 0..kbs.len() {
            if !bs[self.pos + i].eq_ignore_ascii_case(&kbs[i]) {
                return false;
            }
        }
        // Must be at a word boundary (next char must not extend the
        // identifier).
        match bs.get(self.pos + kbs.len()).map(|b| *b as char) {
            None => true,
            Some(c) => !is_term_continue(c),
        }
    }

    fn consume_keyword(&mut self, kw: &str) -> bool {
        if self.looking_at_keyword(kw) {
            self.pos += kw.len();
            true
        } else {
            false
        }
    }
}

fn is_term_start(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn is_term_continue(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

// ── Evaluator ────────────────────────────────────────────────────────

/// Evaluate a query against an inverted index. Returns matching rowids
/// paired with the *raw* matched positions (for ranking) — the
/// position list is the union of positions from every contributing
/// term, sorted ascending.
pub fn eval(expr: &QueryExpr, idx: &InvertedIndex) -> Vec<(RowId, Vec<u32>)> {
    let mut map = eval_inner(expr, idx);
    let mut out: Vec<(RowId, Vec<u32>)> = map
        .iter_mut()
        .map(|(r, p)| {
            p.sort_unstable();
            p.dedup();
            (*r, std::mem::take(p))
        })
        .collect();
    out.sort_by_key(|(r, _)| *r);
    out
}

fn eval_inner(expr: &QueryExpr, idx: &InvertedIndex) -> BTreeMap<RowId, Vec<u32>> {
    match expr {
        QueryExpr::Term(t) => {
            let mut out = BTreeMap::new();
            for (rid, pl) in idx.postings(t) {
                out.insert(rid, pl.positions.clone());
            }
            out
        }
        QueryExpr::Prefix(p) => {
            let mut out: BTreeMap<RowId, Vec<u32>> = BTreeMap::new();
            for (_term, postings) in idx.prefix_postings(p) {
                for (rid, pl) in postings {
                    out.entry(rid).or_default().extend(pl.positions);
                }
            }
            out
        }
        QueryExpr::Phrase(words) => phrase_match(words, idx),
        QueryExpr::Near(words, distance) => near_match(words, *distance, idx),
        QueryExpr::And(parts) => {
            if parts.is_empty() {
                return BTreeMap::new();
            }
            let mut iter = parts.iter();
            let first = eval_inner(iter.next().unwrap(), idx);
            iter.fold(first, |acc, p| {
                let next = eval_inner(p, idx);
                let common: BTreeSet<RowId> = acc
                    .keys()
                    .filter(|k| next.contains_key(*k))
                    .copied()
                    .collect();
                let mut merged = BTreeMap::new();
                for r in common {
                    let mut positions = acc.get(&r).cloned().unwrap_or_default();
                    if let Some(p) = next.get(&r) {
                        positions.extend(p);
                    }
                    merged.insert(r, positions);
                }
                merged
            })
        }
        QueryExpr::Or(parts) => {
            let mut acc: BTreeMap<RowId, Vec<u32>> = BTreeMap::new();
            for p in parts {
                for (r, ps) in eval_inner(p, idx) {
                    acc.entry(r).or_default().extend(ps);
                }
            }
            acc
        }
    }
}

fn phrase_match(words: &[String], idx: &InvertedIndex) -> BTreeMap<RowId, Vec<u32>> {
    let mut out = BTreeMap::new();
    if words.is_empty() {
        return out;
    }
    // Per-doc per-word positions.
    let lists: Vec<Vec<(RowId, &PostingList)>> = words.iter().map(|w| idx.postings(w)).collect();
    if lists.iter().any(|l| l.is_empty()) {
        return out;
    }
    // Index each word's postings by rowid for quick lookup.
    let by_doc: Vec<BTreeMap<RowId, &PostingList>> =
        lists.into_iter().map(|v| v.into_iter().collect()).collect();
    let candidate_rowids: Vec<RowId> = by_doc[0].keys().copied().collect();
    for rid in candidate_rowids {
        let mut all_have = true;
        for m in &by_doc[1..] {
            if !m.contains_key(&rid) {
                all_have = false;
                break;
            }
        }
        if !all_have {
            continue;
        }
        // Find positions p in word[0] such that p+i is in word[i] for all i.
        let p0 = &by_doc[0][&rid].positions;
        let mut hits = Vec::new();
        for &p in p0 {
            let mut ok = true;
            for (i, m) in by_doc.iter().enumerate().skip(1) {
                let target = p + i as u32;
                let pl = &m[&rid];
                if pl.positions.binary_search(&target).is_err() {
                    ok = false;
                    break;
                }
            }
            if ok {
                for i in 0..by_doc.len() {
                    hits.push(p + i as u32);
                }
            }
        }
        if !hits.is_empty() {
            out.insert(rid, hits);
        }
    }
    out
}

fn near_match(words: &[String], distance: u32, idx: &InvertedIndex) -> BTreeMap<RowId, Vec<u32>> {
    let mut out = BTreeMap::new();
    if words.len() < 2 {
        return out;
    }
    let lists: Vec<Vec<(RowId, &PostingList)>> = words.iter().map(|w| idx.postings(w)).collect();
    if lists.iter().any(|l| l.is_empty()) {
        return out;
    }
    let by_doc: Vec<BTreeMap<RowId, &PostingList>> =
        lists.into_iter().map(|v| v.into_iter().collect()).collect();
    // Iterate rowids that appear in *every* word's postings.
    let candidates: Vec<RowId> = by_doc[0].keys().copied().collect();
    for rid in candidates {
        let mut all_have = true;
        for m in &by_doc[1..] {
            if !m.contains_key(&rid) {
                all_have = false;
                break;
            }
        }
        if !all_have {
            continue;
        }
        // Pick one position per word and check that
        // max - min <= distance (distance is "tokens between").
        // Greedy approach: for each position of word[0], find the
        // closest position in each other word and check the spread.
        let p0 = &by_doc[0][&rid].positions;
        let mut hits = Vec::new();
        for &start in p0 {
            let mut min_pos = start;
            let mut max_pos = start;
            let mut all_found = true;
            for m in &by_doc[1..] {
                let pl = &m[&rid].positions;
                // Binary search for the closest position to `start`.
                let idx = pl.partition_point(|p| *p < start);
                let mut best: Option<u32> = None;
                let mut best_dist = u32::MAX;
                if idx < pl.len() {
                    let p = pl[idx];
                    let d = p.saturating_sub(start);
                    if d <= distance && d < best_dist {
                        best = Some(p);
                        best_dist = d;
                    }
                }
                if idx > 0 {
                    let p = pl[idx - 1];
                    let d = start.saturating_sub(p);
                    if d <= distance && d < best_dist {
                        best = Some(p);
                    }
                }
                match best {
                    Some(p) => {
                        if p < min_pos {
                            min_pos = p;
                        }
                        if p > max_pos {
                            max_pos = p;
                        }
                    }
                    None => {
                        all_found = false;
                        break;
                    }
                }
            }
            if all_found && max_pos.saturating_sub(min_pos) <= distance {
                hits.push(start);
            }
        }
        if !hits.is_empty() {
            out.insert(rid, hits);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vtab::fts5::tokenizer::tokenize;

    #[test]
    fn parse_simple_term() {
        let q = parse("hello").unwrap().unwrap();
        assert_eq!(q, QueryExpr::Term("hello".into()));
    }

    #[test]
    fn parse_and_implicit() {
        let q = parse("foo bar").unwrap().unwrap();
        assert_eq!(
            q,
            QueryExpr::And(vec![
                QueryExpr::Term("foo".into()),
                QueryExpr::Term("bar".into())
            ])
        );
    }

    #[test]
    fn parse_or_explicit() {
        let q = parse("a OR b").unwrap().unwrap();
        assert_eq!(
            q,
            QueryExpr::Or(vec![
                QueryExpr::Term("a".into()),
                QueryExpr::Term("b".into())
            ])
        );
    }

    #[test]
    fn parse_phrase() {
        let q = parse("\"the quick\"").unwrap().unwrap();
        assert_eq!(q, QueryExpr::Phrase(vec!["the".into(), "quick".into()]));
    }

    #[test]
    fn parse_prefix() {
        let q = parse("qu*").unwrap().unwrap();
        assert_eq!(q, QueryExpr::Prefix("qu".into()));
    }

    #[test]
    fn parse_near() {
        let q = parse("NEAR(quick fox, 3)").unwrap().unwrap();
        assert_eq!(q, QueryExpr::Near(vec!["quick".into(), "fox".into()], 3));
    }

    #[test]
    fn empty_query_is_none() {
        assert!(parse("").unwrap().is_none());
        assert!(parse("   ").unwrap().is_none());
    }

    #[test]
    fn evaluator_term_match() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("the quick brown fox"));
        idx.upsert(2, &tokenize("lazy dog"));
        let q = parse("quick").unwrap().unwrap();
        let r = eval(&q, &idx);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, 1);
    }

    #[test]
    fn evaluator_phrase_match() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("the quick brown fox"));
        idx.upsert(2, &tokenize("quick fox"));
        // "the quick" only matches doc 1.
        let q = parse("\"the quick\"").unwrap().unwrap();
        let r = eval(&q, &idx);
        let ids: Vec<RowId> = r.iter().map(|(r, _)| *r).collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn evaluator_prefix_match() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("quick quack"));
        idx.upsert(2, &tokenize("apple"));
        let q = parse("qu*").unwrap().unwrap();
        let r = eval(&q, &idx);
        let ids: Vec<RowId> = r.iter().map(|(r, _)| *r).collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn evaluator_or_union() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("alpha"));
        idx.upsert(2, &tokenize("beta"));
        idx.upsert(3, &tokenize("gamma"));
        let q = parse("alpha OR gamma").unwrap().unwrap();
        let r = eval(&q, &idx);
        let mut ids: Vec<RowId> = r.iter().map(|(r, _)| *r).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn evaluator_near_match() {
        let mut idx = InvertedIndex::new();
        idx.upsert(1, &tokenize("the quick brown fox jumps"));
        idx.upsert(2, &tokenize("quick the lazy slow fat brown fox"));
        // "quick" near "fox" within 3: doc 1 (positions 1, 3, gap=2 OK).
        let q = parse("NEAR(quick fox, 3)").unwrap().unwrap();
        let r = eval(&q, &idx);
        let ids: Vec<RowId> = r.iter().map(|(r, _)| *r).collect();
        assert_eq!(ids, vec![1]);
    }
}
