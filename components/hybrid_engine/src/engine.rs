// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{FailedReason, KvEngine, RangeCacheEngine, SnapshotContext, SnapshotMiscExt};

use crate::{
    metrics::{
        RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC, SNAPSHOT_TYPE_COUNT_STATIC,
    },
    snapshot::HybridEngineSnapshot,
};

/// This engine is structured with both a disk engine and an region cache
/// engine. The disk engine houses the complete database data, whereas the
/// region cache engine functions as a region cache, selectively caching certain
/// regions (in a better performance storage device such as NVME or RAM) to
/// enhance read performance. For the regions that are cached, region cache
/// engine retains all data that has not been garbage collected.
#[derive(Clone, Debug)]
pub struct HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    disk_engine: EK,
    range_cache_engine: EC,
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    pub fn disk_engine(&self) -> &EK {
        &self.disk_engine
    }

    pub fn mut_disk_engine(&mut self) -> &mut EK {
        &mut self.disk_engine
    }

    pub fn range_cache_engine(&self) -> &EC {
        &self.range_cache_engine
    }

    pub fn mut_range_cache_engine(&mut self) -> &mut EC {
        &mut self.range_cache_engine
    }

    pub fn new_in_memory_snapshot(
        &self,
        sequence_number: u64,
        ctx: Option<SnapshotContext>,
    ) -> Option<EC::Snapshot> {
        if let Some(ctx) = ctx {
            match self
                .range_cache_engine
                .snapshot(ctx.range.unwrap(), ctx.read_ts, sequence_number)
            {
                Ok(snap) => {
                    SNAPSHOT_TYPE_COUNT_STATIC.range_cache_engine.inc();
                    Some(snap)
                }
                Err(FailedReason::TooOldRead) => {
                    RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                        .too_old_read
                        .inc();
                    SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
                    None
                }
                Err(FailedReason::NotCached) => {
                    RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                        .not_cached
                        .inc();
                    SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
                    None
                }
            }
        } else {
            RANGE_CACHEN_SNAPSHOT_ACQUIRE_FAILED_REASON_COUNT_STAIC
                .no_read_ts
                .inc();
            SNAPSHOT_TYPE_COUNT_STATIC.rocksdb.inc();
            None
        }
    }
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    pub fn new(disk_engine: EK, range_cache_engine: EC) -> Self {
        Self {
            disk_engine,
            range_cache_engine,
        }
    }

    pub fn snapshot(&self, ctx: Option<SnapshotContext>) -> HybridEngineSnapshot<EK, EC> {
        let disk_snap = self.disk_engine.snapshot();
        let range_cache_snap = self.new_in_memory_snapshot(disk_snap.sequence_number(), ctx);
        HybridEngineSnapshot::new(disk_snap, range_cache_snap)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use engine_rocks::util::new_engine;
    use engine_traits::{CacheRange, SnapshotContext, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use range_cache_memory_engine::{
        config::RangeCacheConfigManager, RangeCacheEngineConfig, RangeCacheEngineContext,
        RangeCacheMemoryEngine,
    };
    use tempfile::Builder;
    use tikv_util::config::VersionTrack;

    use crate::HybridEngine;

    #[test]
    fn test_engine() {
        let path = Builder::new().prefix("temp").tempdir().unwrap();
        let disk_engine = new_engine(
            path.path().to_str().unwrap(),
            &[CF_DEFAULT, CF_LOCK, CF_WRITE],
        )
        .unwrap();
        let config = Arc::new(VersionTrack::new(RangeCacheEngineConfig::config_for_test()));
        let memory_engine =
            RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(config.clone()));

        let range = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        memory_engine.new_range(range.clone());
        {
            let mut core = memory_engine.core().write();
            core.mut_range_manager().set_safe_point(&range, 10);
        }

        let hybrid_engine = HybridEngine::new(disk_engine, memory_engine.clone());
        let s: crate::HybridEngineSnapshot<engine_rocks::RocksEngine, RangeCacheMemoryEngine> =
            hybrid_engine.snapshot(None);
        assert!(!s.is_range_cache_snapshot_available());

        let mut snap_ctx = SnapshotContext {
            read_ts: 15,
            range: Some(range.clone()),
        };
        let s = hybrid_engine.snapshot(Some(snap_ctx.clone()));
        assert!(s.is_range_cache_snapshot_available());

        snap_ctx.read_ts = 5;
        let s = hybrid_engine.snapshot(Some(snap_ctx.clone()));
        assert!(!s.is_range_cache_snapshot_available());

        let mut config_manager = RangeCacheConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enabled"), ConfigValue::Bool(false));
        config_manager.dispatch(config_change).unwrap();
        assert!(!config.value().enabled);
        snap_ctx.read_ts = 15;
        let s = hybrid_engine.snapshot(Some(snap_ctx));
        assert!(!s.is_range_cache_snapshot_available());
    }
}
