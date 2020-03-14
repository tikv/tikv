// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Implementation of engine_traits for RocksDB
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! TiKV to persist its data.
//!
//! The module structure here mirrors that in engine_traits where possible.
//!
//! Because there are so many similarly named types across the TiKV codebase,
//! and so much "import renaming", this crate consistently explicitly names type
//! that implement a trait as `RocksTraitname`, to avoid the need for import
//! renaming and make it obvious what type any particular module is working with.
//!
//! Please read the engine_trait crate docs before hacking.

#![cfg_attr(test, feature(test))]

#[allow(unused_extern_crates)]
extern crate tikv_alloc;
#[macro_use]
extern crate tikv_util;
#[macro_use]
extern crate slog_global;

#[cfg(test)]
extern crate test;

mod cf_handle;
pub use crate::cf_handle::*;
mod cf_names;
pub use crate::cf_names::*;
mod cf_options;
pub use crate::cf_options::*;
mod compact;
pub use crate::compact::*;
mod db_options;
pub use crate::db_options::*;
mod db_vector;
pub use crate::db_vector::*;
mod engine;
pub use crate::engine::*;
mod import;
pub use crate::import::*;
mod misc;
pub use crate::misc::*;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod table_properties;
pub use crate::table_properties::*;
mod write_batch;
pub use crate::write_batch::*;

mod engine_iterator;
pub use crate::engine_iterator::*;

mod options;
pub mod util;

mod compat;
pub use compat::*;

mod compact_listener;
pub use compact_listener::*;

pub mod properties;
pub use properties::*;

pub mod metrics_flusher;
pub use metrics_flusher::*;

pub mod rocks_metrics;
pub use rocks_metrics::*;

pub mod rocks_metrics_defs;
pub use rocks_metrics_defs::*;

pub mod event_listener;
pub use event_listener::*;
