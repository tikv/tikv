// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::KvEngine;
use crate::errors::Result;
use crate::options::WriteOptions;

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

    pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
        self.kv.write(wb)
    }

    pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(wb, opts)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }

    pub fn write_raft(&self, wb: &R::WriteBatch) -> Result<()> {
        self.raft.write(wb)
    }

    pub fn write_raft_opt(&self, wb: &R::WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(wb, opts)
    }

    pub fn sync_raft(&self) -> Result<()> {
        self.raft.sync()
    }
}
