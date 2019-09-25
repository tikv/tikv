// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use]
extern crate quick_error;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

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
mod util;

pub const DATA_KEY_PREFIX_LEN: usize = 1;

// In our tests, we found that if the batch size is too large, running delete_all_in_range will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

#[derive(Clone, Debug)]
pub struct KvEngines<K, R> {
    pub kv: K,
    pub raft: R,
    pub shared_block_cache: bool,
}

impl<K: KvEngine, R: KvEngine> KvEngines<K, R> {
    pub fn new(kv_engine: K, raft_engine: R, shared_block_cache: bool) -> Self {
        KvEngines {
            kv: kv_engine,
            raft: raft_engine,
            shared_block_cache,
        }
    }

    pub fn write_kv(&self, wb: &K::Batch) -> Result<()> {
        self.kv.write(wb)
    }

    pub fn write_kv_opt(&self, wb: &K::Batch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(opts, wb)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }

    pub fn write_raft(&self, wb: &R::Batch) -> Result<()> {
        self.raft.write(wb)
    }

    pub fn write_raft_opt(&self, wb: &R::Batch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(opts, wb)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync()
    }
}
