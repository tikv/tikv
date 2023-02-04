// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry as MapEntry,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

use collections::HashMap;
use tikv_util::{error, info};

const CACHED_REGION_INFO_SLOT_COUNT: usize = 256;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Default)]
pub struct CachedRegionInfo {
    pub replicated_or_created: AtomicBool,
    // TiKV assumes a region's learner peer is added through snapshot.
    // If this field is false, will try fast path when meet MsgAppend.
    // If this field is true, it means this peer is inited or will be inited by a TiKV snapshot.
    // NOTE If we want a fallback, then we must set inited_or_fallback to true,
    // Otherwise, a normal snapshot will be neglect in `post_apply_snapshot` and cause data loss.
    pub inited_or_fallback: AtomicBool,
    pub snapshot_inflight: portable_atomic::AtomicU128,
    pub fast_add_peer_start: portable_atomic::AtomicU128,
}

pub type CachedRegionInfoMap = HashMap<u64, Arc<CachedRegionInfo>>;

pub struct CachedRegionInfoManager {
    pub cached_region_info: Arc<Vec<RwLock<CachedRegionInfoMap>>>,
}

impl CachedRegionInfoManager {
    // Credit: [splitmix64 algorithm](https://xorshift.di.unimi.it/splitmix64.c)
    #[inline]
    fn hash_u64(mut i: u64) -> u64 {
        i = (i ^ (i >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        i = (i ^ (i >> 27)).wrapping_mul(0x94d049bb133111eb);
        i ^ (i >> 31)
    }

    #[allow(dead_code)]
    #[inline]
    fn unhash_u64(mut i: u64) -> u64 {
        i = (i ^ (i >> 31) ^ (i >> 62)).wrapping_mul(0x319642b2d24d8ec3);
        i = (i ^ (i >> 27) ^ (i >> 54)).wrapping_mul(0x96de1b173f119089);
        i ^ (i >> 30) ^ (i >> 60)
    }

    pub fn new() -> Self {
        let mut cached_region_info = Vec::with_capacity(CACHED_REGION_INFO_SLOT_COUNT);
        for _ in 0..CACHED_REGION_INFO_SLOT_COUNT {
            cached_region_info.push(RwLock::new(HashMap::default()));
        }
        Self {
            cached_region_info: Arc::new(cached_region_info),
        }
    }

    #[inline]
    fn slot_index(id: u64) -> usize {
        debug_assert!(CACHED_REGION_INFO_SLOT_COUNT.is_power_of_two());
        Self::hash_u64(id) as usize & (CACHED_REGION_INFO_SLOT_COUNT - 1)
    }

    pub fn access_cached_region_info_mut<F: FnMut(MapEntry<u64, Arc<CachedRegionInfo>>)>(
        &self,
        region_id: u64,
        mut f: F,
    ) -> Result<()> {
        let slot_id = Self::slot_index(region_id);
        let mut guard = match self.cached_region_info.get(slot_id).unwrap().write() {
            Ok(g) => g,
            Err(_) => return Err("access_cached_region_info_mut poisoned".into()),
        };
        f(guard.entry(region_id));
        Ok(())
    }

    pub fn access_cached_region_info<F: FnMut(Arc<CachedRegionInfo>)>(
        &self,
        region_id: u64,
        mut f: F,
    ) {
        let slot_id = Self::slot_index(region_id);
        let guard = match self.cached_region_info.get(slot_id).unwrap().read() {
            Ok(g) => g,
            Err(_) => panic!("access_cached_region_info poisoned!"),
        };
        match guard.get(&region_id) {
            Some(g) => f(g.clone()),
            None => (),
        }
    }

    pub fn get_inited_or_fallback(&self, region_id: u64) -> Option<bool> {
        let mut result: Option<bool> = None;
        let f = |info: Arc<CachedRegionInfo>| {
            result = Some(info.inited_or_fallback.load(Ordering::SeqCst));
        };
        self.access_cached_region_info(region_id, f);
        result
    }

    pub fn remove_cached_region_info(&self, region_id: u64) {
        let slot_id = Self::slot_index(region_id);
        if let Ok(mut g) = self.cached_region_info.get(slot_id).unwrap().write() {
            info!(
                "remove_cached_region_info";
                "region_id" => region_id,
            );
            let _ = g.remove(&region_id);
        }
    }

    pub fn set_inited_or_fallback(&self, region_id: u64, v: bool) -> Result<()> {
        self.access_cached_region_info_mut(
            region_id,
            |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                MapEntry::Occupied(mut o) => {
                    o.get_mut().inited_or_fallback.store(v, Ordering::SeqCst);
                }
                MapEntry::Vacant(_) => {
                    tikv_util::safe_panic!("not inited!");
                }
            },
        )
    }

    pub fn set_snapshot_inflight(&self, region_id: u64, v: u128) -> Result<()> {
        self.access_cached_region_info_mut(
            region_id,
            |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                MapEntry::Occupied(mut o) => {
                    o.get_mut().snapshot_inflight.store(v, Ordering::SeqCst);
                }
                MapEntry::Vacant(_) => {
                    tikv_util::safe_panic!("not inited!");
                }
            },
        )
    }

    pub fn fallback_to_slow_path(&self, region_id: u64) {
        // TODO clean local, and prepare to request snapshot from TiKV as a trivial
        // procedure.
        fail::fail_point!("fallback_to_slow_path_not_allow", |_| {});
        if self.set_inited_or_fallback(region_id, true).is_err() {
            tikv_util::safe_panic!("set_inited_or_fallback");
        }
    }
}
