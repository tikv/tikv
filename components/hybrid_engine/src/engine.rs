// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CacheRegion, KvEngine, RegionCacheEngine};

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
    EC: RegionCacheEngine,
{
    disk_engine: EK,
    region_cache_engine: EC,
}

impl<EK, EC> HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    pub fn new(disk_engine: EK, region_cache_engine: EC) -> Self {
        Self {
            disk_engine,
            region_cache_engine,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_snapshot(
        &self,
        ctx: Option<SnapshotContext>,
    ) -> crate::HybridEngineSnapshot<EK, EC> {
        use engine_traits::SnapshotMiscExt;
        let disk_snap = self.disk_engine.snapshot();
        let region_cache_snap = if !self.region_cache_engine.enabled() {
            None
        } else if let Some(ctx) = ctx {
            self.region_cache_engine
                .snapshot(
                    ctx.region.unwrap(),
                    ctx.read_ts,
                    disk_snap.sequence_number(),
                )
                .ok()
        } else {
            None
        };
        crate::HybridEngineSnapshot::new(disk_snap, region_cache_snap)
    }

    pub(crate) fn disk_engine(&self) -> &EK {
        &self.disk_engine
    }

    pub fn region_cache_engine(&self) -> &EC {
        &self.region_cache_engine
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotContext {
    pub region: Option<CacheRegion>,
    pub read_ts: u64,
}

impl SnapshotContext {
    pub fn set_region(&mut self, region: CacheRegion) {
        assert!(self.region.is_none());
        self.region = Some(region);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_rocks::util::new_engine;
    use engine_traits::{CacheRegion, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use in_memory_engine::{
        config::InMemoryEngineConfigManager, test_util::new_region, InMemoryEngineConfig,
        InMemoryEngineContext, RegionCacheMemoryEngine,
    };
    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use tempfile::Builder;
    use tikv_util::config::VersionTrack;

    use super::*;
    use crate::HybridEngine;

    #[test]
    fn test_engine() {
        let path = Builder::new().prefix("temp").tempdir().unwrap();
        let disk_engine = new_engine(
            path.path().to_str().unwrap(),
            &[CF_DEFAULT, CF_LOCK, CF_WRITE],
        )
        .unwrap();
        let config = Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test()));
        let memory_engine =
            RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(config.clone()));

        let region = new_region(1, b"k00", b"k10");
        let range = CacheRegion::from_region(&region);
        memory_engine.new_region(region.clone());
        memory_engine
            .core()
            .region_manager()
            .set_safe_point(region.id, 10);

        let hybrid_engine = HybridEngine::new(disk_engine, memory_engine.clone());
        let s = hybrid_engine.new_snapshot(None);
        assert!(!s.region_cache_snapshot_available());

        let mut snap_ctx = SnapshotContext {
            read_ts: 15,
            region: Some(range.clone()),
        };
        let s = hybrid_engine.new_snapshot(Some(snap_ctx.clone()));
        assert!(s.region_cache_snapshot_available());

        snap_ctx.read_ts = 5;
        let s = hybrid_engine.new_snapshot(Some(snap_ctx.clone()));
        assert!(!s.region_cache_snapshot_available());

        let mut config_manager = InMemoryEngineConfigManager(config.clone());
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from("enable"), ConfigValue::Bool(false));
        config_manager.dispatch(config_change).unwrap();
        assert!(!config.value().enable);
        snap_ctx.read_ts = 15;
        let s = hybrid_engine.new_snapshot(Some(snap_ctx));
        assert!(!s.region_cache_snapshot_available());
    }
}
