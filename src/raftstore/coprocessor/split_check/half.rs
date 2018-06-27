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

use super::super::{Coprocessor, ObserverContext, SplitCheckObserver, SplitChecker};
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
    use rocksdb::Writable;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use tempdir::TempDir;

    use raftstore::store::{keys, Msg, SplitCheckRunner, SplitCheckTask};
    use storage::ALL_CFS;
    use util::config::ReadableSize;
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
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
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

        // so split key will be z0006
        for i in 0..12 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }

        runnable.run(SplitCheckTask::new(region.clone(), false));
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
                    assert_eq!(split_key, b"0006");
                    break;
                }
                Ok(Msg::RegionApproximateStat { region_id, .. }) => {
                    assert_eq!(region_id, region.get_id());
                    continue;
                }
                others => panic!("expect split check result, but got {:?}", others),
            }
        }
    }
}
