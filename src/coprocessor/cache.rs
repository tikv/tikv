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
use std::sync::{Arc, Mutex};
use linked_hash_map::LinkedHashMap;
use super::metrics::*;

type DistSQLCacheKey = String;

const DISTSQL_CACHE_ENTRY_ADDITION_SIZE: usize = 40;

pub struct DistSQLCacheEntry {
    region_id: u64,
    version: u64,
    result: Vec<u8>,
    size: usize,
}

impl DistSQLCacheEntry {
    pub fn new(region_id: u64, version: u64, key_size: usize, r: Vec<u8>) -> DistSQLCacheEntry {
        let size = r.len() + DISTSQL_CACHE_ENTRY_ADDITION_SIZE + 2 * key_size;
        DistSQLCacheEntry {
            region_id: region_id,
            version: version,
            result: r,
            size: size,
        }
    }

    pub fn update(&mut self, region_id: u64, version: u64, key_size: usize, res: Vec<u8>) -> usize {
        let old_size = self.size;
        self.size = res.len() + (key_size * 2) + DISTSQL_CACHE_ENTRY_ADDITION_SIZE;
        self.version = version;
        self.result = res;
        self.region_id = region_id;
        self.size - old_size
    }
}

// RegionDistSQLCacheEntry track Region's cache items key.
// So we can quickly get a cache entry list by given a region id.
// That can make evict_region operation more faster than scan whole
// cache items and get the remove item list.
pub struct RegionDistSQLCacheEntry {
    version: u64,
    enable: bool,
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

    pub fn put(&mut self, region_id: u64, k: DistSQLCacheKey, version: u64, res: Vec<u8>) {
        if !self.is_region_cache_enabled(region_id) {
            return;
        }

        let key_size = k.len();
        if self.map.contains_key(&k) {
            let mut entry = self.map.get_mut(&k).unwrap();
            let size_diff = entry.update(region_id, version, key_size, res);
            self.size += size_diff;
        } else {
            let entry = DistSQLCacheEntry::new(region_id, version, key_size, res);
            self.size += entry.size;
            self.map.insert(k.clone(), entry);
            self.update_regions(region_id, k);
        }

        // Remove entry untile cache size is less or equals than capacity
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
        let mut need_remove = false;
        if let Some(entry) = self.map.get(k) {
            if let Some(rentry) = self.regions.get(&region_id) {
                // Region's version is not same as cached evict it
                if entry.version != rentry.version {
                    need_remove = true;
                }
            }
        }
        if need_remove {
            self.remove(k);
        }
    }

    pub fn get_region_version_and_cache_entry(
        &mut self,
        region_id: u64,
        k: &str,
    ) -> (u64, Option<&Vec<u8>>) {
        (self.get_region_version(region_id), self.get(region_id, k))
    }

    pub fn get_region_version(&self, region_id: u64) -> u64 {
        match self.regions.get(&region_id) {
            None => 0,
            Some(item) => item.version,
        }
    }

    pub fn get(&mut self, region_id: u64, k: &str) -> Option<&Vec<u8>> {
        if !self.is_region_cache_enabled(region_id) {
            return None;
        }

        self.check_evict_key(region_id, k);
        if let Some(entry) = self.map.get_refresh(k) {
            Some(&entry.result)
        } else {
            None
        }
    }

    pub fn remove(&mut self, k: &str) {
        let regions = &mut self.regions;
        if let Some(entry) = self.map.remove(k) {
            let region_id: u64 = entry.region_id;
            let mut need_remove_region = false;
            if let Some(node) = regions.get_mut(&region_id) {
                // Delete from region cache entry list
                node.cached_items.remove(k);
                if !node.cached_items.is_empty() && node.version != 1 {
                    need_remove_region = true;
                }
            }
            if need_remove_region {
                regions.remove(&region_id);
            };
            self.size -= entry.size;
        }
    }

    fn evict_region_with_exist_region(&mut self, region_id: u64) {
        let items = match self.regions.get_mut(&region_id) {
            Some(region) => {
                region.version += 1;
                Some(region.cached_items.clone())
            }
            None => {
                return;
            }
        };
        if let Some(cached_items) = items {
            for (key, _) in (&cached_items).iter() {
                self.remove(key);
            }
        }
    }

    fn evict_region_with_new_region(&mut self, region_id: u64) {
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
        let opt = match self.regions.get_mut(&region_id) {
            Some(entry) => {
                entry.enable = false;
                return;
            }
            None => {
                let rmap = HashMap::new();
                Some(rmap)
            }
        };
        if let Some(rmap) = opt {
            let entry = RegionDistSQLCacheEntry {
                version: 0,
                enable: false,
                cached_items: rmap,
            };
            self.regions.insert(region_id, entry);
        }
    }

    pub fn enable_region_cache(&mut self, region_id: u64) {
        if let Some(entry) = self.regions.get_mut(&region_id) {
            entry.enable = true;
        }
    }

    fn is_region_cache_enabled(&self, region_id: u64) -> bool {
        match self.regions.get(&region_id) {
            None => true,
            Some(entry) => entry.enable,
        }
    }

    fn update_regions(&mut self, region_id: u64, k: DistSQLCacheKey) {
        let mut opt = self.regions
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

lazy_static! {
    pub static ref DISTSQL_CACHE: Arc<Mutex<DistSQLCache>> =
        Arc::new(Mutex::new(DistSQLCache::new(DEFAULT_DISTSQL_CACHE_SIZE)));
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
        cache.put(10, key.clone(), version, result.clone());
        assert_eq!(1, cache.len());
        match cache.get(10, &key) {
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
        cache.put(10, key1.clone(), version1, result.clone());
        assert_eq!(1, cache.len());
        let key2: DistSQLCacheKey = "test2".to_string();
        let version2 = cache.get_region_version(11);
        cache.put(11, key2.clone(), version2, result.clone());
        assert_eq!(2, cache.len());
        let key3: DistSQLCacheKey = "test3".to_string();
        let version3 = cache.get_region_version(12);
        cache.put(12, key3.clone(), version3, result.clone());
        assert_eq!(2, cache.len());
        match cache.get(10, &key1) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_distsql_cache_put_after_evict_region() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.evict_region(10);
        cache.put(10, key.clone(), version, result.clone());
        assert_eq!(1, cache.len());
        match cache.get(10, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
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
        cache.put(10, key.clone(), version, result.clone());
        cache.put(11, key2.clone(), version2, result2.clone());
        cache.evict_region(10);
        assert_eq!(1, cache.len());
        match cache.get(10, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
        match cache.get(11, &key2) {
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
        cache.put(10, key.clone(), version, result.clone());
        assert_eq!(1, cache.len());
        match cache.get(10, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_global_distsql_cache() {
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = DISTSQL_CACHE.lock().unwrap().get_region_version(10);
        DISTSQL_CACHE
            .lock()
            .unwrap()
            .put(10, key.clone(), version, result.clone());
        match DISTSQL_CACHE.lock().unwrap().get(10, &key) {
            None => (assert!(false)),
            Some(value) => {
                assert_eq!(&result, value);
            }
        }
    }

    #[test]
    fn test_disable_region_cache_distsql_cache_should_not_hit_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, key.clone(), version, result.clone());
        cache.disable_region_cache(10);
        match cache.get(10, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
        cache.enable_region_cache(10);
        match cache.get(10, &key) {
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
        cache.put(10, key.clone(), version, result.clone());
        cache.enable_region_cache(10);
        match cache.get(10, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_disable_region_cache_with_different_region_cache_should_cache_entry() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.get_region_version(11);
        cache.put(10, key.clone(), version, result.clone());
        match cache.get(10, &key) {
            None => {
                assert!(false);
            }
            Some(_) => (),
        }
    }
}
