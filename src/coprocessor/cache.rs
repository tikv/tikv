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

type DistSQLCacheKey = String;

pub struct DistSQLCacheEntry {
    region_id: u64,
    region_epoch: RegionEpoch,
    version: u64,
    result: Vec<u8>,
}

impl DistSQLCacheEntry {
    pub fn new(region_id: u64, epoch: RegionEpoch, version: u64, r: Vec<u8>) -> DistSQLCacheEntry {
        DistSQLCacheEntry {
            region_id: region_id,
            region_epoch: epoch,
            version: version,
            result: r,
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
}

impl DistSQLCache {
    pub fn new(capacity: usize) -> DistSQLCache {
        DistSQLCache {
            regions: HashMap::new(),
            map: LinkedHashMap::new(),
            max_size: capacity,
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
        let option = match self.map.get_mut(&k) {
            Some(entry) => {
                entry.version = version;
                entry.result = res;
                entry.region_id = region_id;
                entry.region_epoch = epoch;
                None
            }
            None => {
                let entry = box DistSQLCacheEntry::new(region_id, epoch, version, res);
                Some(entry)
            }
        };

        match option {
            None => (),
            Some(entry) => {
                self.map.insert(k.clone(), entry);
                self.update_regions(region_id, k);
                if self.len() > self.capacity() {
                    self.remove_lru();
                }
            }
        }
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
                        if !node.cached_items.is_empty() {
                            Some(1)
                        } else {
                            None
                        }
                    }
                };
                if opt.is_some() {
                    regions.remove(&region_id);
                };
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
                for (key, _) in region.cached_items.iter() {
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
        }

    }

    pub fn capacity(&self) -> usize {
        self.max_size
    }

    pub fn len(&self) -> usize {
        self.map.len()
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
        self.map.pop_front();
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

pub const DISTSQL_CACHE_SIZE: usize = 1000;

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
        let mut cache: DistSQLCache = DistSQLCache::new(100);
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
        let mut cache: DistSQLCache = DistSQLCache::new(100);
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
    fn test_distsql_cache_evict_region() {
        let mut cache: DistSQLCache = DistSQLCache::new(100);
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
        let mut cache: DistSQLCache = DistSQLCache::new(100);
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
