use txn_types::{Key, Value};
use engine_traits::{KvEngine};
use std::sync::Arc;

pub trait RegionCache {
    fn get(&self, key: &Key) -> Option<Value>;
    fn check_consistency<E: KvEngine>(&self, snap: &E::Snapshot) -> bool;
    fn region_id(&self) -> u64;
}

pub trait RegionCacheBuilder: Send + Sync {
    fn build<E: KvEngine>(&self, snap: &E::Snapshot) -> Arc<dyn RegionCache>;
    fn region_id(&self) -> u64;
    fn from(region_id: u64) -> Option<Box<dyn RegionCacheBuilder>);
}

pub struct NoneRegionCache {}

impl RegionCache for NoneRegionCache {
    fn get(&self, _: &Key) -> Option<Value> {
        None
    }
    fn check_consistency<E: KvEngine>(&self, _: &E::Snapshot) -> bool {
        false
    }
    fn region_id(&self) -> u64 {
        0
    }
}

pub struct NoneRegionCacheBuilder {}


impl RegionCacheBuilder for NoneRegionCacheBuilder {
    fn build<E: KvEngine>(&self, _: &E::Snapshot) -> Arc<dyn RegionCache> {
        Arc::new(NoneRegionCache{})
    }

    fn region_id(&self) -> u64 { 0 }

    fn from(region_id: u64) -> Option<Box<dyn RegionCacheBuilder>> {
        None
    }
}


