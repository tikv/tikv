use engine_traits::KvEngine;
use raftstore::store::{RegionCache, RegionCacheBuilder, RegionCacheBuilderFactory, RegionSnapshot};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use txn_types::{Key, Value};
use tikv_util::collections::HashMap;
use kvproto::metapb;
use crate::storage::kv::CursorBuilder;

pub struct TxnRegionCache {
    valid: AtomicBool,
    data: HashMap<Key, Value>,
    region_id: u64,
}

impl TxnRegionCache {
    pub fn new(data: HashMap<Key, Value>, region_id: u64) -> TxnRegionCache {
        TxnRegionCache {
            data,
            region_id,
            valid: AtomicBool::new(true)
        }
    }
}

impl RegionCache for TxnRegionCache {
    fn get(&self, key: &Key) -> Option<Value> {
        self.data.get(key).map(|v| v.clone())
    }
    fn valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
    fn set_valid(&self, v: bool) {
        self.valid.store(v, Ordering::Release);
    }
    fn region_id(&self) -> u64 {
        0
    }
}

pub struct TxnRegionCacheBuilder {
    region: metapb::Region
}

impl<E> RegionCacheBuilder<E> for TxnRegionCacheBuilder
where
    E: KvEngine + 'static,
{
    fn build(&self, snap: E::Snapshot) -> Arc<dyn RegionCache> {
        let mut data = HashMap::default();
        let region_snapshot = RegionSnapshot::from_snapshot(snap.into_sync(), self.region.clone());
        let cursor = CursorBuilder::new(&self.snapshot, cf)
            .range(Some(self.region.get_start_key()), Some(self.region.get_end_key()))
            .fill_cache(self.fill_cache)
            .scan_mode(scan_mode)
            .hint_min_ts(hint_min_ts)
            .hint_max_ts(hint_max_ts)
            .build()?;
        Ok(cursor)

        Arc::new(TxnRegionCache::new(data, self.region_id))
    }

    fn region_id(&self) -> u64 {
        self.region.get_id()
    }
}

pub struct TxnRegionCacheBuilderFactory {}

impl TxnRegionCacheBuilderFactory {
    pub fn new() -> TxnRegionCacheBuilderFactory {
        Self {}
    }
}

impl<E> RegionCacheBuilderFactory<E> for TxnRegionCacheBuilderFactory
where
    E: KvEngine + 'static,
{
    fn create_builder(&self, region: metapb::Region) -> Box<dyn RegionCacheBuilder<E>> {
        Box::new(TxnRegionCacheBuilder{ region })
    }
}
