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
//! renaming and make it obvious what type any particular module is working
//! with.
//!
//! Please read the engine_trait crate docs before hacking.

#![cfg_attr(test, feature(test))]
#![feature(let_chains)]
#![feature(option_get_or_insert_default)]
#![feature(path_file_prefix)]

#[cfg(test)]
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use ::encryption::DataKeyManager;
pub use compact_listener::*;
pub use config::*;
pub use decode_properties::*;
pub use event_listener::*;
pub use flow_listener::*;
pub use properties::*;
pub use rocks_metrics::*;
pub use rocks_metrics_defs::*;
pub use rocksdb::{
    set_perf_flags, set_perf_level, PerfContext, PerfFlag, PerfFlags, PerfLevel,
    Statistics as RocksStatistics,
};
pub use ttl_properties::*;

pub use crate::{
    cf_options::*,
    checkpoint::*,
    db_options::*,
    db_vector::*,
    engine::*,
    engine_iterator::*,
    import::*,
    logger::*,
    misc::*,
    mvcc_properties::*,
    perf_context::*,
    perf_context_impl::{
        PerfStatisticsInstant, ReadPerfContext, ReadPerfInstant, WritePerfContext, WritePerfInstant,
    },
    snapshot::*,
    sst::*,
    sst_partitioner::*,
    status::*,
    table_properties::*,
    write_batch::*,
};

mod cf_names;

mod cf_options;
mod checkpoint;
mod compact;

mod db_options;
mod db_vector;
mod engine;
mod import;
mod logger;
mod misc;
pub mod mvcc_properties;
pub mod perf_context;
mod perf_context_impl;
mod perf_context_metrics;
pub mod range_properties;
mod snapshot;
mod sst;
mod sst_partitioner;
mod status;
mod table_properties;
mod write_batch;

mod engine_iterator;
pub mod options;
pub mod util;

mod compact_listener;
pub mod config;
pub mod decode_properties;
pub mod encryption;
pub mod event_listener;
pub mod flow_listener;
pub mod properties;
pub mod rocks_metrics;
pub mod rocks_metrics_defs;
pub mod ttl_properties;

pub mod file_system;

mod raft_engine;

pub mod flow_control_factors;
pub mod raw;

pub fn get_env(
    key_manager: Option<std::sync::Arc<DataKeyManager>>,
    limiter: Option<std::sync::Arc<::file_system::IoRateLimiter>>,
) -> engine_traits::Result<std::sync::Arc<raw::Env>> {
    let env = encryption::get_env(None /* base_env */, key_manager)?;
    file_system::get_env(env, limiter)
}
