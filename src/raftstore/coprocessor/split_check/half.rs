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

use super::super::{Coprocessor, ObserverContext, SplitCheckObserver};
use super::Status;

#[derive(Default)]
pub struct HalfStatus {
    buckets: Vec<Vec<u8>>,
    current_size: u64,
}

impl HalfStatus {
    pub fn push_data(&mut self, key: &[u8], value_size: u64, bucket_size: u64) {
        let current_len = key.len() as u64 + value_size;
        if self.buckets.is_empty() || self.current_size > bucket_size {
            self.buckets.push(key.to_vec());
            self.current_size = 0;
        }
        self.current_size += current_len;
    }

    pub fn split_key(self) -> Option<Vec<u8>> {
        let mid = self.buckets.len() / 2;
        if mid == 0 {
            None
        } else {
            self.buckets.get(mid).cloned()
        }
    }
}

pub struct HalfCheckObserver {
    half_split_bucket_size: u64,
}

impl HalfCheckObserver {
    pub fn new(half_split_bucket_size: u64) -> HalfCheckObserver {
        HalfCheckObserver {
            half_split_bucket_size: half_split_bucket_size,
        }
    }
}

impl Coprocessor for HalfCheckObserver {}

impl SplitCheckObserver for HalfCheckObserver {
    fn new_split_check_status(
        &self,
        _ctx: &mut ObserverContext,
        status: &mut Status,
        _engine: &DB,
    ) {
        if status.auto_split {
            return;
        }
        status.half = Some(HalfStatus::default());
    }

    fn on_split_check(
        &self,
        _: &mut ObserverContext,
        status: &mut Status,
        key: &[u8],
        value_size: u64,
    ) -> Option<Vec<u8>> {
        if let Some(status) = status.half.as_mut() {
            status.push_data(key, value_size, self.half_split_bucket_size);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;

    use tempdir::TempDir;
    use rocksdb::Writable;
    use kvproto::metapb::Peer;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use kvproto::metapb::Region;

    use storage::ALL_CFS;
    use raftstore::store::{keys, Msg, SplitCheckRunner, SplitCheckTask, SplitType};
    use util::rocksdb::{new_engine_opt, CFOptions};
    use util::worker::Runnable;
    use util::transport::RetryableSendCh;
    use util::properties::SizePropertiesCollectorFactory;
    use util::config::ReadableSize;

    use raftstore::coprocessor::{Config, CoprocessorHost};

    #[test]
    fn test_split_check() {
        let path = TempDir::new("test-raftstore").unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        let f = Box::new(SizePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
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
        cfg.half_split_bucket_size = ReadableSize(1);
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
        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush(true).unwrap();

        runnable.run(SplitCheckTask::new(&region, SplitType::HalfSplit));
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
                // This may sent by SizeCheckObserver
                Ok(Msg::ApproximateRegionSize { region_id, .. }) => {
                    assert_eq!(region_id, region.get_id());
                }
                others => panic!("expect split check result, but got {:?}", others),
            }
        }
        drop(rx);
    }
}
