// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{BatchSplit, KvEngine, RegionCacheEngine};

use crate::HybridEngine;

impl<EK, EC> BatchSplit for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type SplitResult = EC::Snapshot;
    fn batch_split(&self, region_id: u64, keys: Vec<Vec<u8>>) -> Self::SplitResult {
        unimplemented!()
    }

    fn on_batch_split(&self, region_id: u64, split_result: Self::SplitResult) {
        unimplemented!()
    }
}
