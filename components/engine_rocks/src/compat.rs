// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::snapshot::RocksSnapshot;
use engine::Snapshot as RawSnapshot;
use engine::DB;
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

impl Compat for RawSnapshot {
    type Other = RocksSnapshot;

    #[inline]
    fn c(&self) -> &RocksSnapshot {
        RocksSnapshot::from_ref(self)
    }
}
