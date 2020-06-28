// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine::{Engines, DB};
use engine_traits::KvEngines;
use std::sync::Arc;

/// A trait to enter the world of engine traits from a raw `Arc<DB>`
/// with as little syntax as possible.
///
/// This will be used during the transition from RocksDB to the
/// `KvEngine` abstraction and then discarded.
pub trait Compat {
    type Other;

    fn c(&self) -> &Self::Other;
}

impl Compat for Arc<DB> {
    type Other = RocksEngine;

    #[inline]
    fn c(&self) -> &RocksEngine {
        RocksEngine::from_ref(self)
    }
}

/// Like `Compat` but creates a new instance
pub trait CloneCompat {
    type Other;

    fn c(&self) -> Self::Other;
}

impl CloneCompat for Engines {
    type Other = KvEngines<RocksEngine, RocksEngine>;

    fn c(&self) -> Self::Other {
        KvEngines::new(
            self.kv.c().clone(),
            self.raft.c().clone(),
            self.shared_block_cache,
        )
    }
}
