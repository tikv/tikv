// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

pub trait BatchSplit {
    type SplitResult: Send + Debug;

    // "splitted_region_ids" include all regions' id including the dervied where
    // the derived region id cloud be at front and end depending on whether
    // right_derive is enabled.
    // "keys" should be [region_start_key, split keys...]
    fn batch_split(
        &self,
        region_id: u64,
        splitted_region_ids: Vec<u64>,
        keys: Vec<Vec<u8>>,
    ) -> Self::SplitResult;

    fn on_batch_split(&self, region_id: u64, split_result: Self::SplitResult);
}
