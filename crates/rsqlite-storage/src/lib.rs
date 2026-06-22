// Cursor `next()` methods deliberately mirror Iterator naming; index loops in
// the B-tree page codec are clearer than iterator chains.
#![allow(clippy::should_implement_trait, clippy::needless_range_loop)]

pub mod btree;
pub mod codec;
pub mod error;
pub mod header;
pub mod pager;
pub mod varint;
