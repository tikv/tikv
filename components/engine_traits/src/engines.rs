// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::KvEngine;

pub trait KvEngines: Clone + Send + Sync {
    type Kv: KvEngine;
    type Raft: KvEngine;
    fn kv(&self) -> &Self::Kv;
    fn raft(&self) -> &Self::Raft;
    // TODO: it's only used for metrics flusher, remove it.
    fn shared_block_cache(&self) -> bool;
}
