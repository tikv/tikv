// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use(
    kv,
    slog_kv,
    slog_error,
    slog_warn,
    slog_record,
    slog_b,
    slog_log,
    slog_record_static
)]
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

use std::sync::Arc;

pub mod util;

mod errors;
pub mod rocks;
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

pub const DATA_KEY_PREFIX_LEN: usize = 1;
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

#[derive(Clone, Debug)]
pub struct Engines<E: KVEngine> {
    pub kv: Arc<E>,
    pub raft: Arc<E>,
    pub shared_block_cache: bool,
}

impl<E: KVEngine> Engines<E> {
    pub fn new(kv_engine: Arc<E>, raft_engine: Arc<E>, shared_block_cache: bool) -> Self {
        Engines {
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
