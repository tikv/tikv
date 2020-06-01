// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::sync::Arc;

pub mod rocks;
pub use crate::rocks::{CFHandle, DBIterator, Env, Range, ReadOptions, WriteOptions, DB};
mod errors;
pub use crate::errors::*;

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

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync_wal().map_err(Error::Engine)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync_wal().map_err(Error::Engine)
    }
}
