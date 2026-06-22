// Pre-existing stylistic lints in long-lived engine code; kept readable as-is.
#![allow(
    clippy::if_same_then_else,
    clippy::collapsible_if,
    clippy::too_many_arguments,
    clippy::manual_strip,
    clippy::needless_range_loop
)]

//! # rsqlite-core
//!
//! Core engine: catalog, query planner, executor, and the public
//! [`database::Database`] handle.
//!
//! The engine is a tree-walking interpreter. SQL is parsed via
//! [`rsqlite_parser`], converted to a [`planner::Plan`] tree, then evaluated
//! by the [`executor`] against pages served by [`rsqlite_storage`].
//!
//! Most callers use this crate via the [`rsqlite`] facade rather than
//! directly. See `LIMITATIONS.md` in the repo root for the deferred-feature
//! inventory.

pub mod catalog;
pub mod database;
pub(crate) mod datetime;
pub mod error;
pub(crate) mod eval_helpers;
pub mod executor;
pub(crate) mod json;
pub mod planner;
pub mod types;
pub mod udf;
pub mod vtab;
