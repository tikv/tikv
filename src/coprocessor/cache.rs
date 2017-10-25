// Copyright 2016 PingCAP, Inc.
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

extern crate linked_hash_map;

use std::collections::HashMap;
use std::option::Option;
use std::boxed::Box;
use std::sync::{Arc, Mutex};
use kvproto::metapb::RegionEpoch;
use self::linked_hash_map::LinkedHashMap;
use super::metrics::*;

type DistSQLCacheKey = String;

const DISTSQL_CACHE_ENTRY_ADDITION_SIZE: usize = 40;

pub struct DistSQLCacheEntry {
    region_id: u64,
    region_epoch: RegionEpoch,
    version: u64,
    result: Vec<u8>,
    size: usize,
}

impl DistSQLCacheEntry {
    pub fn new(
        region_id: u64,
        epoch: RegionEpoch,
        version: u64,
        key_size: usize,
        r: Vec<u8>,
    ) -> DistSQLCacheEntry {
        let size = r.len() + DISTSQL_CACHE_ENTRY_ADDITION_SIZE + 2 * key_size;
        DistSQLCacheEntry {
            region_id: region_id,
            region_epoch: epoch,
            version: version,
            result: r,
            size: size,
        }
    }
}

pub struct RegionDistSQLCacheEntry {
    version: u64,
    cached_items: HashMap<DistSQLCacheKey, u8>,
}

pub struct DistSQLCache {
    regions: HashMap<u64, RegionDistSQLCacheEntry>,
    max_size: usize,
    map: LinkedHashMap<DistSQLCacheKey, Box<DistSQLCacheEntry>>,
    size: usize,
}

impl DistSQLCache {
    // capacity is memory size unit is byte
    pub fn new(capacity: usize) -> DistSQLCache {
        DistSQLCache {
            regions: HashMap::new(),
            map: LinkedHashMap::new(),
            max_size: capacity,
            size: 0,
        }
    }

    pub fn get_region_version(&self, region_id: u64) -> u64 {
        match self.regions.get(&region_id) {
            None => 0,
            Some(item) => item.version,
        }
    }

    pub fn put(
        &mut self,
        region_id: u64,
        epoch: RegionEpoch,
        k: DistSQLCacheKey,
        version: u64,
        res: Vec<u8>,
    ) {
        let key_size = k.len();
        let option = match self.map.get_mut(&k) {
            Some(entry) => {
                let old_size = entry.size;
                entry.size = res.len() + (key_size * 2) + DISTSQL_CACHE_ENTRY_ADDITION_SIZE;
                entry.version = version;
                entry.result = res;
                entry.region_id = region_id;
                entry.region_epoch = epoch;
                self.size = self.size - old_size + entry.size;
                None
            }
            None => {
                let entry = box DistSQLCacheEntry::new(region_id, epoch, version, key_size, res);
                self.size += entry.size;
                Some(entry)
            }
        };

        match option {
            None => (),
            Some(entry) => {
                self.map.insert(k.clone(), entry);
                self.update_regions(region_id, k);
            }
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

    fn check_evict_key(&mut self, region_id: u64, epoch: &RegionEpoch, k: &str) {
        let opt = match self.map.get(k) {
            None => None,
            Some(entry) => if !validate_epoch(entry, region_id, epoch) {
                Some(())
            } else {
                match self.regions.get(&region_id) {
                    None => None,
                    Some(rentry) => {
                        // Region's version is not same as cached evict it
                        if rentry.version != entry.version {
                            Some(())
                        } else {
                            None
                        }
                    }
                }
            },
        };
        if opt.is_some() {
            self.remove(k);
        }
    }

    pub fn get(&mut self, region_id: u64, epoch: &RegionEpoch, k: &str) -> Option<&Vec<u8>> {
        self.check_evict_key(region_id, epoch, k);
        if let Some(entry) = self.map.get_refresh(k) {
            Some(&entry.result)
        } else {
            None
        }
    }

    pub fn remove(&mut self, k: &str) {
        let regions = &mut self.regions;
        let option = self.map.remove(k);
        match option {
            None => (),
            Some(entry) => {
                let region_id: u64 = entry.region_id;
                let opt = match regions.get_mut(&region_id) {
                    None => None,
                    Some(node) => {
                        // Delete from region cache entry list
                        node.cached_items.remove(k);
                        if !node.cached_items.is_empty() && node.version != 1 {
                            Some(())
                        } else {
                            None
                        }
                    }
                };
                if opt.is_some() {
                    regions.remove(&region_id);
                };
                self.size -= entry.size;
            }
        };
    }

    pub fn evict_region(&mut self, region_id: u64) {
        debug!("Evict Region: {}", region_id);
        let keys = match self.regions.get_mut(&region_id) {
            None => None,
            Some(region) => {
                region.version += 1;
                let mut keys: Vec<DistSQLCacheKey> = Vec::new();
                for (key, _) in (&region.cached_items).iter() {
                    keys.push(key.to_string());
                }
                Some(keys)
            }
        };
        match keys {
            None => {
                let entry = RegionDistSQLCacheEntry {
                    version: 1,
                    cached_items: HashMap::new(),
                };
                self.regions.insert(region_id, entry);
            }
            Some(keys) => for i in keys {
                self.remove(&i);
            },
        };
        CORP_DISTSQL_CACHE_SIZE_GAUGE_VEC
            .with_label_values(&["size"])
            .set(self.size() as f64);

        CORP_DISTSQL_CACHE_SIZE_GAUGE_VEC
            .with_label_values(&["count"])
            .set(self.len() as f64);
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

    fn update_regions(&mut self, region_id: u64, k: DistSQLCacheKey) {
        let opt = match self.regions.get_mut(&region_id) {
            Some(entry) => {
                entry.cached_items.insert(k, 1);
                None
            }
            None => {
                let mut rmap = HashMap::new();
                rmap.insert(k, 1);
                Some(rmap)
            }
        };
        if let Some(rmap) = opt {
            let entry = RegionDistSQLCacheEntry {
                version: 0,
                cached_items: rmap,
            };
            self.regions.insert(region_id, entry);
        }
    }

    #[inline]
    fn remove_lru(&mut self) {
        match self.map.pop_front() {
            None => (),
            Some((_, entry)) => {
                self.size -= entry.size;
            }
        };
    }
}

fn validate_epoch(entry: &DistSQLCacheEntry, region_id: u64, epoch: &RegionEpoch) -> bool {
    if entry.region_id != region_id {
        return false;
    }
    if entry.region_epoch.get_conf_ver() != epoch.get_conf_ver() {
        return false;
    }
    if entry.region_epoch.get_version() != epoch.get_version() {
        return false;
    }
    true
}

// DistSQL Cache Size unit is byte, for now just use 256MB
pub const DISTSQL_CACHE_SIZE: usize = 256 * 1024 * 1024;

lazy_static! {
    pub static ref DISTSQL_CACHE: Arc<Mutex<DistSQLCache>> =
        Arc::new(Mutex::new(DistSQLCache::new(DISTSQL_CACHE_SIZE)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::metapb::RegionEpoch;

    fn create_epoch(version: u64, conf_ver: u64) -> RegionEpoch {
        let mut ret: RegionEpoch = RegionEpoch::new();
        ret.set_conf_ver(conf_ver);
        ret.set_version(version);
        ret
    }

    #[test]
    fn test_distsql_cache() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let result: Vec<u8> = vec![100, 101, 102];
        let epoch: RegionEpoch = create_epoch(1, 2);
        let version = cache.get_region_version(10);
        cache.put(10, epoch.clone(), key.clone(), version, result.clone());
        assert_eq!(1, cache.len());
        match cache.get(10, &epoch, &key) {
            None => (assert!(false)),
            Some(value) => {
                assert_eq!(&result, value);
            }
        }
    }

    #[test]
    fn test_distsql_cache_evict_entry_by_stale_epoch() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let epoch: RegionEpoch = create_epoch(1, 2);
        let epoch2: RegionEpoch = create_epoch(1, 3);
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.put(10, epoch.clone(), key.clone(), version, result.clone());
        assert_eq!(1, cache.len());
        match cache.get(10, &epoch2, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
        assert_eq!(0, cache.len());
    }

    #[test]
    fn test_distsql_cache_flush_lru_when_reach_capacity() {
        let result: Vec<u8> = vec![1,2,3,4,5];
        let mut cache: DistSQLCache = DistSQLCache::new(110);
        let key1: DistSQLCacheKey = "test1".to_string();
        let epoch1: RegionEpoch = create_epoch(1, 2);
        let version1 = cache.get_region_version(10);
        cache.put(10, epoch1.clone(), key1.clone(), version1, result.clone());
        assert_eq!(1, cache.len());
        let key2: DistSQLCacheKey = "test2".to_string();
        let epoch2: RegionEpoch = create_epoch(1, 2);
        let version2 = cache.get_region_version(11);
        cache.put(11, epoch2.clone(), key2.clone(), version2, result.clone());
        assert_eq!(2, cache.len());
        let key3: DistSQLCacheKey = "test3".to_string();
        let epoch3: RegionEpoch = create_epoch(1, 2);
        let version3 = cache.get_region_version(12);
        cache.put(12, epoch3.clone(), key3.clone(), version3, result.clone());
        assert_eq!(2, cache.len());
        match cache.get(10, &epoch1, &key1) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_distsql_cache_evict_region() {
        let mut cache: DistSQLCache = DistSQLCache::new(200);
        let key: DistSQLCacheKey = "test1".to_string();
        let key2: DistSQLCacheKey = "test2".to_string();
        let epoch: RegionEpoch = create_epoch(1, 2);
        let epoch2: RegionEpoch = create_epoch(1, 2);
        let result: Vec<u8> = vec![100, 101, 102];
        let result2: Vec<u8> = vec![103, 104, 105];
        let version = cache.get_region_version(10);
        let version2 = cache.get_region_version(11);
        cache.put(10, epoch.clone(), key.clone(), version, result.clone());
        cache.put(11, epoch2.clone(), key2.clone(), version2, result2.clone());
        cache.evict_region(10);
        assert_eq!(1, cache.len());
        match cache.get(10, &epoch, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
        match cache.get(11, &epoch2, &key2) {
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
        let epoch: RegionEpoch = create_epoch(1, 2);
        let result: Vec<u8> = vec![100, 101, 102];
        let version = cache.get_region_version(10);
        cache.evict_region(10);
        cache.put(10, epoch.clone(), key.clone(), version, result.clone());
        assert_eq!(1, cache.len());
        match cache.get(10, &epoch, &key) {
            None => (),
            Some(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_global_distsql_cache() {
        let key: DistSQLCacheKey = "test1".to_string();
        let epoch: RegionEpoch = create_epoch(1, 2);
        let result: Vec<u8> = vec![100, 101, 102];
        let version = DISTSQL_CACHE.lock().unwrap().get_region_version(10);
        DISTSQL_CACHE
            .lock()
            .unwrap()
            .put(10, epoch.clone(), key.clone(), version, result.clone());
        match DISTSQL_CACHE.lock().unwrap().get(10, &epoch, &key) {
            None => (assert!(false)),
            Some(value) => {
                assert_eq!(&result, value);
            }
        }
    }
}
