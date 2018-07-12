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

use rocksdb::DB;

use util::config::ReadableSize;

use kvproto::metapb::Region;
use raftstore::store::util as raftstore_util;
use super::super::{Coprocessor, ObserverContext, SplitCheckObserver, SplitChecker};
use super::super::error::Result;
use super::Host;

const BUCKET_NUMBER_LIMIT: usize = 1024;
const BUCKET_SIZE_LIMIT_MB: u64 = 512;

pub struct Checker {
    buckets: Vec<Vec<u8>>,
    cur_bucket_size: u64,
    each_bucket_size: u64,
}

impl Checker {
    fn new(each_bucket_size: u64) -> Checker {
        Checker {
            each_bucket_size,
            cur_bucket_size: 0,
            buckets: vec![],
        }
    }
}

impl SplitChecker for Checker {
    fn on_kv(&mut self, _: &mut ObserverContext, key: &[u8], value_size: u64) -> bool {
        if self.buckets.is_empty() || self.cur_bucket_size >= self.each_bucket_size {
            self.buckets.push(key.to_vec());
            self.cur_bucket_size = 0;
        }
        self.cur_bucket_size += key.len() as u64 + value_size;
        false
    }

    fn split_key(&mut self) -> Option<Vec<u8>> {
        let mid = self.buckets.len() / 2;
        if mid == 0 {
            None
        } else {
            Some(self.buckets.swap_remove(mid))
        }
    }

    fn approximate_split_key(&self, region: &Region, engine: &DB) -> Result<Option<Vec<u8>>> {
        Ok(box_try!(raftstore_util::get_region_approximate_middle(engine, region)))
    }
}

pub struct HalfCheckObserver {
    half_split_bucket_size: u64,
}

impl HalfCheckObserver {
    pub fn new(region_size_limit: u64) -> HalfCheckObserver {
        let mut half_split_bucket_size = region_size_limit / BUCKET_NUMBER_LIMIT as u64;
        let bucket_size_limit = ReadableSize::mb(BUCKET_SIZE_LIMIT_MB).0;
        if half_split_bucket_size == 0 {
            half_split_bucket_size = 1;
        } else if half_split_bucket_size > bucket_size_limit {
            half_split_bucket_size = bucket_size_limit;
        }
        HalfCheckObserver {
            half_split_bucket_size,
        }
    }
}

impl Coprocessor for HalfCheckObserver {}

impl SplitCheckObserver for HalfCheckObserver {
    fn add_checker(&self, _: &mut ObserverContext, host: &mut Host, _: &DB) {
        if host.auto_split() {
            return;
        }
        host.add_checker(Box::new(Checker::new(self.half_split_bucket_size)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::Arc;

    use kvproto::metapb::Peer;
    use kvproto::metapb::Region;
    use kvproto::pdpb::CheckPolicy;
    use rocksdb::Writable;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use tempdir::TempDir;

    use raftstore::store::{keys, Msg, SplitCheckRunner, SplitCheckTask};
    use storage::{Key, CF_DEFAULT, ALL_CFS};
    use util::config::ReadableSize;
    use util::escape;
    use util::properties::SizePropertiesCollectorFactory;
    use util::rocksdb::{new_engine_opt, CFOptions};
    use util::transport::RetryableSendCh;
    use util::worker::Runnable;

    use super::*;
    use raftstore::coprocessor::{Config, CoprocessorHost};

    #[test]
    fn test_split_check() {
        let path = TempDir::new("test-raftstore").unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut cf_opts = ColumnFamilyOptions::new();
                let f = Box::new(SizePropertiesCollectorFactory::default());
                cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);
           
        let (tx, rx) = mpsc::sync_channel(100);
        let ch = RetryableSendCh::new(tx, "test-split");
        let mut cfg = Config::default();
        cfg.region_max_size = ReadableSize(BUCKET_NUMBER_LIMIT as u64);
        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            ch.clone(),
            Arc::new(CoprocessorHost::new(cfg, ch.clone())),
        );

        // so split key will be z0005
        let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).encoded());
            engine.put_cf(cf_handle, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }

        let check = || {
            loop {
                match rx.try_recv() {
                    Ok(Msg::SplitRegion {
                        region_id,
                        region_epoch,
                        split_key,
                        ..
                    }) => {
                        assert_eq!(region_id, region.get_id());
                        assert_eq!(&region_epoch, region.get_region_epoch());
                        let split_key = Key::from_encoded(split_key)
                        .raw()
                        .unwrap();
                        assert_eq!(escape(&split_key), "0005");
                        break;
                    } 
                    Ok(Msg::RegionApproximateSize { region_id, .. }) => {
                        assert_eq!(region_id, region.get_id());
                        continue;
                    }
                    others => panic!("expect split check result, but got {:?}", others),
                }
            }
        };
        runnable.run(SplitCheckTask::new(region.clone(), false, CheckPolicy::SCAN));
        check();     
        runnable.run(SplitCheckTask::new(region.clone(), false, CheckPolicy::APPROXIMATE));
        check();
    }
}
