// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use(slog_error, slog_warn)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod util;

mod errors;
pub use crate::errors::*;
mod peekable;
pub use crate::peekable::*;
mod iterable;
pub use crate::iterable::*;
mod mutable;
pub use crate::mutable::*;
mod cf;
pub use crate::cf::*;
mod engine;
pub use crate::engine::*;
mod options;
pub use crate::options::*;
use engine_rocksdb::{WriteBatch, DB};

use std::sync::Arc;

pub const DATA_KEY_PREFIX_LEN: usize = 1;

// In our tests, we found that if the batch size is too large, running delete_all_in_range will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

#[derive(Clone, Debug)]
pub struct KvEngines<E> {
    pub kv: E,
    pub raft: E,
    pub shared_block_cache: bool,
}

pub type Engines = KvEngines<Arc<DB>>;

impl<E: KvEngine> KvEngines<E> {
    pub fn new(kv_engine: E, raft_engine: E, shared_block_cache: bool) -> Self {
        KvEngines {
            kv: kv_engine,
            raft: raft_engine,
            shared_block_cache,
        }
    }

    pub fn write_kv(&self, wb: &E::Batch) -> Result<()> {
        self.kv.write(wb)
    }

    pub fn write_kv_opt(&self, wb: &E::Batch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(opts, wb)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }

    pub fn write_raft(&self, wb: &E::Batch) -> Result<()> {
        self.raft.write(wb)
    }

    pub fn write_raft_opt(&self, wb: &E::Batch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(opts, wb)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync()
    }
}

impl Engines {
    pub fn new(kv_engine: Arc<DB>, raft_engine: Arc<DB>, shared_block_cache: bool) -> Self {
        KvEngines {
            kv: kv_engine,
            raft: raft_engine,
            shared_block_cache,
        }
    }

    pub fn write_kv(&self, wb: &RawWriteBatch) -> Result<()> {
        self.kv.write(wb).map_err(Error::Engine)
    }

    pub fn write_kv_opt(&self, wb: &RawWriteBatch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(wb, &opts.into()).map_err(Error::Engine)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync_wal().map_err(Error::Engine)
    }

    pub fn write_raft(&self, wb: &RawWriteBatch) -> Result<()> {
        self.raft.write(wb).map_err(Error::Engine)
    }

    pub fn write_raft_opt(&self, wb: &RawWriteBatch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(wb, &opts.into()).map_err(Error::Engine)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync_wal().map_err(Error::Engine)
    }
}
