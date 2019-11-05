// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use engine::DB;
use std::sync::Arc;

/// A trait to enter the world of engine traits from a raw `Arc<DB>`
/// with as little syntax as possible.
///
/// This will be used during the transition from RocksDB to the
/// `KvEngine` abstraction and then discarded.
pub trait EngineCompat {
    fn c(&self) -> &RocksEngine;
}

impl EngineCompat for Arc<DB> {
    #[inline]
    fn c(&self) -> &RocksEngine {
        RocksEngine::from_ref(self)
    }
}
