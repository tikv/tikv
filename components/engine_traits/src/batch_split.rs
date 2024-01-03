// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

pub trait BatchSplit {
    type SplitResult: Send + Debug;
    fn batch_split(
        &self,
        region_id: u64,
        splitted_region_id: Vec<u64>,
        keys: Vec<Vec<u8>>,
    ) -> Self::SplitResult;

    fn on_batch_split(&self, region_id: u64, split_result: Self::SplitResult);
}
