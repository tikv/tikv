// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{BatchSplit, KvEngine, RegionCacheEngine};

use crate::HybridEngine;

impl<EK, EC> BatchSplit for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type SplitResult = EC::SplitResult;
    fn batch_split(
        &self,
        region_id: u64,
        splitted_region_ids: Vec<u64>,
        keys: Vec<Vec<u8>>,
    ) -> Self::SplitResult {
        self.region_cache_engine()
            .batch_split(region_id, splitted_region_ids, keys)
    }

    fn on_batch_split(&self, region_id: u64, split_result: Self::SplitResult) {
        self.region_cache_engine()
            .on_batch_split(region_id, split_result)
    }
}
