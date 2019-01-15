// Copyright 2018 PingCAP, Inc.
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

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crc::crc32::{self, Hasher32};
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::metapb::*;
use rocksdb::{ColumnFamilyOptions, EnvOptions, SstFileWriter, DB};
use uuid::Uuid;

use pd::RegionInfo;
use raftstore::store::keys;

use super::client::*;
use super::common::*;
use super::Result;
use util::collections::HashMap;

pub fn calc_data_crc32(data: &[u8]) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write(data);
    digest.sum32()
}

pub fn check_db_range(db: &DB, range: (u8, u8)) {
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        assert_eq!(db.get(&k).unwrap().unwrap(), &[i]);
    }
}

pub fn gen_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SSTMeta, Vec<u8>) {
    let env_opt = EnvOptions::new();
    let cf_opt = ColumnFamilyOptions::new();
    let mut w = SstFileWriter::new(env_opt, cf_opt);

    w.open(path.as_ref().to_str().unwrap()).unwrap();
    for i in range.0..range.1 {
        let k = keys::data_key(&[i]);
        w.put(&k, &[i]).unwrap();
    }
    w.finish().unwrap();

    read_sst_file(path, range)
}

pub fn read_sst_file<P: AsRef<Path>>(path: P, range: (u8, u8)) -> (SSTMeta, Vec<u8>) {
    let mut data = Vec::new();
    File::open(path).unwrap().read_to_end(&mut data).unwrap();
    let crc32 = calc_data_crc32(&data);

    let mut meta = SSTMeta::new();
    meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    meta.mut_range().set_start(vec![range.0]);
    meta.mut_range().set_end(vec![range.1]);
    meta.set_crc32(crc32);
    meta.set_length(data.len() as u64);
    meta.set_cf_name("default".to_owned());

    (meta, data)
}

#[derive(Clone)]
pub struct MockClient {
    counter: Arc<AtomicUsize>,
    regions: Arc<Mutex<HashMap<u64, Region>>>,
    scatter_regions: Arc<Mutex<HashMap<u64, Region>>>,
}

impl MockClient {
    pub fn new() -> MockClient {
        MockClient {
            counter: Arc::new(AtomicUsize::new(1)),
            regions: Arc::new(Mutex::new(HashMap::new())),
            scatter_regions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn alloc_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) as u64
    }

    pub fn add_region_range(&mut self, start: &[u8], end: &[u8]) {
        let mut r = Region::new();
        r.set_id(self.alloc_id());
        r.set_start_key(start.to_owned());
        r.set_end_key(end.to_owned());
        let mut peer = Peer::new();
        peer.set_id(self.alloc_id());
        peer.set_store_id(self.alloc_id());
        r.mut_peers().push(peer);
        let mut regions = self.regions.lock().unwrap();
        regions.insert(r.get_id(), r);
    }

    pub fn get_scatter_region(&self, id: u64) -> Option<RegionInfo> {
        let regions = self.scatter_regions.lock().unwrap();
        regions.get(&id).map(|r| RegionInfo::new(r.clone(), None))
    }
}

impl ImportClient for MockClient {
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        let mut found = None;
        for region in self.regions.lock().unwrap().values() {
            if inside_region(key, region) {
                found = Some(region.clone());
                break;
            }
        }
        Ok(RegionInfo::new(found.unwrap(), None))
    }

    fn split_region(&self, _: &RegionInfo, split_key: &[u8]) -> Result<SplitRegionResponse> {
        let mut regions = self.regions.lock().unwrap();

        let region = regions
            .iter()
            .map(|(_, r)| r)
            .find(|r| {
                split_key >= r.get_start_key()
                    && (split_key < r.get_end_key() || r.get_end_key().is_empty())
            })
            .unwrap()
            .clone();

        regions.remove(&region.get_id());

        let mut left = region.clone();
        left.set_id(self.alloc_id());
        left.set_end_key(split_key.to_vec());
        regions.insert(left.get_id(), left.clone());

        let mut right = region.clone();
        right.set_start_key(split_key.to_vec());
        regions.insert(right.get_id(), right.clone());

        let mut resp = SplitRegionResponse::new();
        resp.set_left(left);
        resp.set_right(right);
        Ok(resp)
    }

    fn scatter_region(&self, region: &RegionInfo) -> Result<()> {
        let mut regions = self.scatter_regions.lock().unwrap();
        regions.insert(region.get_id(), region.region.clone());
        Ok(())
    }
}
