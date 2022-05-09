// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::{engine::RocksEngine, raw::DB};

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
