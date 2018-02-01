// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::option::Option;
// use std::sync::Mutex;
use spin::Mutex;
use linked_hash_map::LinkedHashMap;
use super::metrics::*;

type DistSQLCacheKey = String;
pub type SQLCache = Mutex<DistSQLCache>;

const DISTSQL_CACHE_ENTRY_ADDITION_SIZE: usize = 40;

pub struct DistSQLCacheEntry {
    region_id: u64,
    version: u64,
    value: Vec<u8>,
    // size track each query result cached size in bytes. It calculate by:
    //  len(value) + DISTSQL_CACHE_ENTRY_ADDITION_SIZE + 2 * len(key)
    // cache key will stored in DistSQLCache.map and DistSQLCache.regions so
    // mutilply 2
    size: usize,
    // Transaction Start TS for this cache entry.
    // Cache must not available for any transaction before this.
    start_ts: u64,
}

impl DistSQLCacheEntry {
    pub fn new(
        region_id: u64,
        version: u64,
        key_size: usize,
        value: Vec<u8>,
        start_ts: u64,
    ) -> DistSQLCacheEntry {
        let size = value.len() + DISTSQL_CACHE_ENTRY_ADDITION_SIZE + 2 * key_size;
        DistSQLCacheEntry {
            region_id: region_id,
            version: version,
            value: value,
            size: size,
            start_ts: start_ts,
        }
    }

    pub fn update(
        &mut self,
        region_id: u64,
        version: u64,
        key_size: usize,
        value: Vec<u8>,
        start_ts: u64,
    ) -> usize {
        let old_size = self.size;
        self.size = value.len() + (key_size * 2) + DISTSQL_CACHE_ENTRY_ADDITION_SIZE;
        self.version = version;
        self.value = value;
        self.start_ts = start_ts;
        self.region_id = region_id;
        self.size - old_size
    }
}

// RegionDistSQLCacheEntry track Region's cache items key.
// So we can quickly get a cache entry list by given a region id.
// That can make evict_region operation more faster than scan whole
// cache items and get the remove item list.
pub struct RegionDistSQLCacheEntry {
    // version will trace current region's write times. If region's write
    // finished, the version will increase. And when DistSQLCache.get called,
    // it will flush entrys which version is not match current version.
    version: u64,
    // If enable is false mean's current region is in written. So before
    // enable change to true we should not cache anything or get any result
    // from DistSQLCache related to this region.
    enable: bool,
    // cached_items track cached DistSQL keys. So we can iterate keys
    // for evict a region. And if we delete a cache entry we can use
    // HashMap's remove function to fast remove a cached DistSQL key.
    cached_items: HashMap<DistSQLCacheKey, u8>,
}

impl Default for RegionDistSQLCacheEntry {
    fn default() -> RegionDistSQLCacheEntry {
        RegionDistSQLCacheEntry {
            version: 0,
            enable: true,
            cached_items: HashMap::default(),
        }
    }
}

#[derive(Default)]
pub struct DistSQLCache {
    regions: HashMap<u64, RegionDistSQLCacheEntry>,
    max_size: usize,
    map: LinkedHashMap<DistSQLCacheKey, DistSQLCacheEntry>,
    size: usize,
}

impl DistSQLCache {
    // capacity is memory size unit is byte
    pub fn new(capacity: usize) -> DistSQLCache {
        DistSQLCache {
            max_size: capacity,
            ..Default::default()
        }
    }

    pub fn put(
        &mut self,
        region_id: u64,
        k: DistSQLCacheKey,
        version: u64,
        res: Vec<u8>,
        start_ts: u64,
    ) {
        if !self.is_region_cache_enabled(region_id) {
            return;
        }

        let key_size = k.len();
        if self.map.contains_key(&k) {
            let entry = self.map.get_mut(&k).unwrap();
            // If current transaction before cached transaction, Do not cache it.
            if entry.start_ts <= start_ts {
                let size_diff = entry.update(region_id, version, key_size, res, start_ts);
                self.size += size_diff;
            }
        } else {
            let entry = DistSQLCacheEntry::new(region_id, version, key_size, res, start_ts);
            self.size += entry.size;
            self.map.insert(k.clone(), entry);
            self.update_regions(region_id, k);
        }

        // Remove entry until cache size is less or equals than capacity
        while self.size() > self.capacity() {
            self.remove_lru();
        }

        CORP_DISTSQL_CACHE_SIZE_GAUGE_VEC
            .with_label_values(&["size"])
            .set(self.size() as f64);

        CORP_DISTSQL_CACHE_SIZE_GAUGE_VEC
            .with_label_values(&["count"])
            .set(self.len() as f64);
    }

    fn check_evict_key(&mut self, region_id: u64, k: &str) {
        // remove expired cache entries.
        self.map
            .get(k)
            .and_then(|entry| {
                self.regions
                    .get(&region_id)
                    .and_then(|rentry| Some(entry.version != rentry.version))
            })
            .and_then(|need_remove| {
                if need_remove {
                    self.remove(k);
                }
                Some(())
            });
    }

    pub fn get_region_version_and_cache_entry(
        &mut self,
        region_id: u64,
        k: &str,
        start_ts: u64,
    ) -> (u64, Option<&Vec<u8>>) {
        let mut region_version = 0;
        if let Some(entry) = self.regions.get(&region_id) {
            region_version = entry.version;
            if !entry.enable {
                return (region_version, None);
            }
        }

        self.check_evict_key(region_id, k);
        // Check transaction is after cache entry's transaction.
        let data = self.map.get_refresh(k).and_then(|entry| {
            if start_ts >= entry.start_ts {
                Some(&entry.value)
            } else {
                None
            }
        });
        (region_version, data)
    }

    pub fn get_region_version(&self, region_id: u64) -> u64 {
        self.regions.get(&region_id).map_or(0, |item| item.version)
    }

    pub fn get(&mut self, region_id: u64, k: &str, start_ts: u64) -> Option<&Vec<u8>> {
        if !self.is_region_cache_enabled(region_id) {
            return None;
        }

        self.check_evict_key(region_id, k);
        // Check transaction is after cache entry's transaction.
        self.map.get_refresh(k).and_then(|entry| {
            if start_ts >= entry.start_ts {
                Some(&entry.value)
            } else {
                None
            }
        })
    }

    pub fn remove(&mut self, k: &str) {
        let regions = &mut self.regions;
        if let Some(entry) = self.map.remove(k) {
            let region_id: u64 = entry.region_id;
            if let Some(node) = regions.get_mut(&region_id) {
                // Delete from region cache entry list
                // We should not delete self.regions item.
                // In some coner case delete self.regions item may cause
                // cache no need data and then cause an error result.
                node.cached_items.remove(k);
            }
            self.size -= entry.size;
        }
    }

    fn evict_region_with_exist_region(&mut self, region_id: u64) {
        self.regions
            .get_mut(&region_id)
            .and_then(|region| {
                region.version += 1;
                Some(region.cached_items.clone())
            })
            .and_then(|cached_items| {
                for (key, _) in (&cached_items).iter() {
                    self.remove(key);
                }
                Some(())
            });
    }

    fn evict_region_with_new_region(&mut self, region_id: u64) {
        // If there's no region version data in cache, get_version will
        // return 0 and after that some thread call evict_region, we should set
        // version to 1 to prevent cache unused data.
        let entry = RegionDistSQLCacheEntry {
            version: 1,
            ..Default::default()
        };
        self.regions.insert(region_id, entry);
    }

    pub fn evict_region(&mut self, region_id: u64) {
        if self.regions.contains_key(&region_id) {
            self.evict_region_with_exist_region(region_id);
        } else {
            self.evict_region_with_new_region(region_id);
        }

        CORP_DISTSQL_CACHE_SIZE_GAUGE_VEC
            .with_label_values(&["size"])
            .set(self.size() as f64);

        CORP_DISTSQL_CACHE_SIZE_GAUGE_VEC
            .with_label_values(&["count"])
            .set(self.len() as f64);
    }

    pub fn evict_region_and_enable(&mut self, region_id: u64) {
        self.evict_region(region_id);
        self.enable_region_cache(region_id);
    }

    pub fn capacity(&self) -> usize {
        self.max_size
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }

    pub fn update_capacity(&mut self, capacity: usize) {
        self.max_size = capacity;

        // Remove entry untile cache size is less or equals than capacity
        while self.size() > self.capacity() {
            self.remove_lru();
        }
    }

    pub fn disable_region_cache(&mut self, region_id: u64) {
        let entry = self.regions
            .entry(region_id)
            .or_insert_with(Default::default);
        entry.enable = false;
    }

    pub fn enable_region_cache(&mut self, region_id: u64) {
        self.regions.get_mut(&region_id).map(|e| e.enable = true);
    }

    fn is_region_cache_enabled(&self, region_id: u64) -> bool {
        self.regions.get(&region_id).map_or(true, |e| e.enable)
    }

    fn update_regions(&mut self, region_id: u64, k: DistSQLCacheKey) {
        let opt = self.regions
            .entry(region_id)
            .or_insert_with(Default::default);
        opt.cached_items.insert(k, 1);
    }

    #[inline]
    fn remove_lru(&mut self) {
        if let Some((_, entry)) = self.map.pop_front() {
            self.size -= entry.size;
        }
    }
}

// DistSQL cache size unit is byte, default is 256MB
pub const DEFAULT_DISTSQL_CACHE_SIZE: usize = 256 * 1024 * 1024;
// DistSQL cache entry max size unit is byte, default is 5MB
pub const DEFAULT_DISTSQL_CACHE_ENTRY_MAX_SIZE: usize = 5 * 1204 * 1024;

pub fn new_mutex_cache(capacity: usize) -> SQLCache {
    Mutex::new(DistSQLCache::new(capacity))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distsql_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone(), 0);
        assert_eq!(1, cache.len());
        match cache.get(10, &key, 0) {
            None => (assert!(false)),
            Some(value) => {
                assert_eq!(&result, value);
            }
        }
    }

    #[test]
    fn test_distsql_cache_flush_lru_when_reach_capacity() {
        let result: Vec<u8> = vec![1, 2, 3, 4, 5];
        let mut cache: DistSQLCache = DistSQLCache::new(110);
        let key1: DistSQLCacheKey = "test1".to_string();
        let version1 = cache.get_region_version(10);
        cache.put(10, key1.clone(), version1, result.clone(), 0);
        assert_eq!(1, cache.len());
        let key2: DistSQLCacheKey = "test2".to_string();
        let version2 = cache.get_region_version(11);
        cache.put(11, key2.clone(), version2, result.clone(), 0);
        assert_eq!(2, cache.len());
        let key3: DistSQLCacheKey = "test3".to_string();
        let version3 = cache.get_region_version(12);
        cache.put(12, key3.clone(), version3, result.clone(), 0);
        assert_eq!(2, cache.len());
        assert!(cache.get(10, &key1, 0).is_none());
    }

    #[test]
    fn test_distsql_cache_put_after_evict_region() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.evict_region(10);
        cache.put(10, key.clone(), version, result.clone(), 0);
        assert_eq!(1, cache.len());
        assert!(cache.get(10, &key, 0).is_none());
        assert_eq!(0, cache.len());
    }

    #[test]
    fn test_distsql_cache_evict_region() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let key2: DistSQLCacheKey = "test2".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let result2: Vec<u8> = vec![103, 104, 105];
        let version = cache.get_region_version(10);
        let version2 = cache.get_region_version(11);
        cache.put(10, key.clone(), version, result.clone(), 0);
        cache.put(11, key2.clone(), version2, result2.clone(), 0);
        cache.evict_region(10);
        assert_eq!(1, cache.len());
        assert!(cache.get(10, &key, 0).is_none());
        match cache.get(11, &key2, 0) {
            None => (assert!(false)),
            Some(value) => {
                assert_eq!(&result2, value);
            }
        }
    }

    #[test]
    fn test_distsql_cache_should_be_evict() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.evict_region(10);
        cache.put(10, key.clone(), version, result.clone(), 0);
        assert_eq!(1, cache.len());
        assert!(cache.get(10, &key, 0).is_none());
    }

    #[test]
    fn test_mutex_distsql_cache() {
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let cache = new_mutex_cache(DEFAULT_DISTSQL_CACHE_SIZE);
        let version = cache.lock().get_region_version(10);
        cache
            .lock()
            .put(10, key.clone(), version, result.clone(), 0);
        let value = cache.lock().get(10, &key, 0).unwrap().clone();
        assert_eq!(result, value);
    }

    #[test]
    fn test_disable_region_cache_distsql_cache_should_not_hit_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone(), 0);
        cache.disable_region_cache(10);
        assert!(cache.get(10, &key, 0).is_none());
        cache.enable_region_cache(10);
        match cache.get(10, &key, 0) {
            None => (assert!(false)),
            Some(value) => {
                assert_eq!(&result, value);
            }
        }
    }

    #[test]
    fn test_disable_region_cache_distsql_cache_should_not_cache_entry() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.disable_region_cache(10);
        cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone(), 0);
        cache.enable_region_cache(10);
        assert!(cache.get(10, &key, 0).is_none());
    }

    #[test]
    fn test_disable_region_cache_with_different_region_cache_should_cache_entry() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.disable_region_cache(11);
        cache.put(10, key.clone(), version, result.clone(), 0);
        assert!(cache.get(10, &key, 0).is_some());
    }

    #[test]
    fn test_get_region_version_and_cache_entry_with_no_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let (version, entry) = cache.get_region_version_and_cache_entry(10, &key, 0);
        assert_eq!(version, 0);
        assert!(entry.is_none());
    }

    #[test]
    fn test_get_region_version_and_cache_entry_with_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone(), 0);
        let (version2, entry) = cache.get_region_version_and_cache_entry(10, &key, 0);
        assert_eq!(version2, version);
        assert!(entry.is_some());
    }

    #[test]
    fn test_get_region_version_and_cache_entry_with_evict_region() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let version = cache.get_region_version(10);
        cache.evict_region(10);
        let (version2, entry) = cache.get_region_version_and_cache_entry(10, &key, 0);
        assert!(version2 > version);
        assert!(entry.is_none());
    }

    #[test]
    fn test_get_region_version_and_cache_entry_with_disable_region_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone(), 0);
        cache.disable_region_cache(10);
        let (version2, entry) = cache.get_region_version_and_cache_entry(10, &key, 0);
        assert_eq!(version2, version);
        assert!(entry.is_none());
    }

    #[test]
    fn test_get_cache_with_older_start_ts() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone(), 10);
        let (version2, entry) = cache.get_region_version_and_cache_entry(10, &key, 0);
        assert_eq!(version2, version);
        assert!(entry.is_none());
    }

    #[test]
    fn test_put_with_older_start_ts_should_not_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone(), 10);
        let result2: Vec<u8> = vec![200, 201, 202];
        cache.put(10, key.clone(), version, result2.clone(), 9);
        let (_, entry) = cache.get_region_version_and_cache_entry(10, &key, 10);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap(), &result);
    }
}
