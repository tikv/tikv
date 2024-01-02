// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::BatchSplit;

use crate::PanicEngine;

#[derive(Debug)]
pub struct PanicSplitResult;

impl BatchSplit for PanicEngine {
    type SplitResult = PanicSplitResult;
    fn batch_split(&self, region_id: u64, keys: &Vec<Vec<u8>>) -> Self::SplitResult {
        panic!()
    }

    fn on_batch_split(&self, region_id: u64, split_result: Self::SplitResult) {
        panic!()
    }
}
