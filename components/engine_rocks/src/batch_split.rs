// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::BatchSplit;

use crate::RocksEngine;

#[derive(Debug)]
pub struct DummySplitResult;

impl BatchSplit for RocksEngine {
    type SplitResult = DummySplitResult;

    fn batch_split(&self, _: &Vec<Vec<u8>>) -> Self::SplitResult {
        DummySplitResult {}
    }

    fn on_batch_split(&self, _: Self::SplitResult) {}
}
