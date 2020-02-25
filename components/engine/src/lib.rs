// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

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

use std::sync::Arc;

pub mod util;

pub mod rocks;
pub use crate::rocks::{
    CFHandle, DBIterator, DBVector, Range, ReadOptions, WriteBatch, WriteOptions, DB,
};
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

pub const DATA_KEY_PREFIX_LEN: usize = 1;

#[derive(Clone, Debug)]
pub struct Engines {
    pub kv: Arc<DB>,
    pub raft: Arc<DB>,
    pub shared_block_cache: bool,
}

impl Engines {
    pub fn new(kv_engine: Arc<DB>, raft_engine: Arc<DB>, shared_block_cache: bool) -> Engines {
        Engines {
            kv: kv_engine,
            raft: raft_engine,
            shared_block_cache,
        }
    }

    pub fn write_kv(&self, wb: &WriteBatch) -> Result<()> {
        self.kv.write(wb).map_err(Error::RocksDb)
    }

    pub fn write_kv_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync_wal().map_err(Error::RocksDb)
    }

    pub fn write_raft(&self, wb: &WriteBatch) -> Result<()> {
        self.raft.write(wb).map_err(Error::RocksDb)
    }

    pub fn write_raft_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync_wal().map_err(Error::RocksDb)
    }
}
