// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    engine::KvEngine, errors::Result, options::WriteOptions, raft_engine::RaftEngine,
    write_batch::WriteBatch,
};

#[derive(Clone, Debug)]
pub struct Engines<K, R> {
    // kv can be either global kv store, or the tablet in multirocks version.
    pub kv: K,
    pub raft: R,
}

impl<K: KvEngine, R: RaftEngine> Engines<K, R> {
    pub fn new(kv_engine: K, raft_engine: R) -> Self {
        Engines {
            kv: kv_engine,
            raft: raft_engine,
        }
    }

    pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
        wb.write()
    }

    pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
        wb.write_opt(opts)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }
}
