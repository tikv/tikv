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

#[cfg(test)]
extern crate test;

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
mod logger;
pub use crate::logger::*;
mod misc;
pub use crate::misc::*;
pub mod range_properties;
mod snapshot;
pub use crate::range_properties::*;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod sst_partitioner;
pub use crate::sst_partitioner::*;
mod table_properties;
pub use crate::table_properties::*;
mod write_batch;
pub use crate::write_batch::*;
pub mod mvcc_properties;
pub use crate::mvcc_properties::*;
pub mod perf_context;
pub use crate::perf_context::*;
mod perf_context_impl;
mod perf_context_metrics;

mod engine_iterator;
pub use crate::engine_iterator::*;

mod options;
pub mod raw_util;
pub mod util;

mod compat;
pub use compat::*;

mod compact_listener;
pub use compact_listener::*;

pub mod decode_properties;
pub use decode_properties::*;
pub mod properties;
pub use properties::*;

pub mod rocks_metrics;
pub use rocks_metrics::*;

pub mod rocks_metrics_defs;
pub use rocks_metrics_defs::*;

pub mod event_listener;
pub use event_listener::*;

pub mod flow_listener;
pub use flow_listener::*;

pub mod config;
pub use config::*;

pub mod ttl_properties;
pub use ttl_properties::*;

pub mod encryption;

pub mod file_system;

mod raft_engine;

pub use rocksdb::{set_perf_flags, set_perf_level, PerfContext, PerfFlag, PerfFlags, PerfLevel};

pub mod flow_control_factors;
pub use flow_control_factors::*;

pub mod raw;

pub fn get_env(
    key_manager: Option<std::sync::Arc<::encryption::DataKeyManager>>,
    limiter: Option<std::sync::Arc<::file_system::IORateLimiter>>,
) -> std::result::Result<std::sync::Arc<raw::Env>, String> {
    let env = encryption::get_env(None /*base_env*/, key_manager)?;
    file_system::get_env(Some(env), limiter)
}
