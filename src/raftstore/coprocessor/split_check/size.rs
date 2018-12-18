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

use std::mem;

use super::super::error::Result;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use raftstore::store::util as raftstore_util;
use raftstore::store::{keys, util, Msg};
use rocksdb::DB;
use util::transport::{RetryableSendCh, Sender};

use super::super::metrics::*;
use super::super::{Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker};
use super::Host;

pub struct Checker {
    max_size: u64,
    split_size: u64,
    current_size: u64,
    split_keys: Vec<Vec<u8>>,
    batch_split_limit: u64,
    policy: CheckPolicy,
}

impl Checker {
    pub fn new(
        max_size: u64,
        split_size: u64,
        batch_split_limit: u64,
        policy: CheckPolicy,
    ) -> Checker {
        Checker {
            max_size,
            split_size,
            current_size: 0,
            split_keys: Vec::with_capacity(1),
            batch_split_limit,
            policy,
        }
    }
}

impl SplitChecker for Checker {
    fn on_kv(&mut self, _: &mut ObserverContext, entry: &KeyEntry) -> bool {
        let size = entry.entry_size() as u64;
        self.current_size += size;

        let mut over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        if self.current_size > self.split_size && !over_limit {
            self.split_keys.push(keys::origin_key(entry.key()).to_vec());
            // if for previous on_kv() self.current_size == self.split_size,
            // the split key would be pushed this time, but the entry size for this time should not be ignored.
            self.current_size = if self.current_size - size == self.split_size {
                size
            } else {
                0
            };
            over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        }

        // For a large region, scan over the range maybe cost too much time,
        // so limit the number of produced split_key for one batch.
        // Also need to scan over self.max_size for last part.
        over_limit && self.current_size + self.split_size >= self.max_size
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        // make sure not to split when less than max_size for last part
        if self.current_size + self.split_size < self.max_size {
            self.split_keys.pop();
        }
        if !self.split_keys.is_empty() {
            mem::replace(&mut self.split_keys, vec![])
        } else {
            vec![]
        }
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }

    fn approximate_split_keys(&mut self, region: &Region, engine: &DB) -> Result<Vec<Vec<u8>>> {
        Ok(box_try!(raftstore_util::get_region_approximate_split_keys(
            engine,
            region,
            self.split_size,
            self.max_size,
            self.batch_split_limit,
        )))
    }
}

pub struct SizeCheckObserver<C> {
    region_max_size: u64,
    split_size: u64,
    split_limit: u64,
    ch: RetryableSendCh<Msg, C>,
}

impl<C: Sender<Msg>> SizeCheckObserver<C> {
    pub fn new(
        region_max_size: u64,
        split_size: u64,
        split_limit: u64,
        ch: RetryableSendCh<Msg, C>,
    ) -> SizeCheckObserver<C> {
        SizeCheckObserver {
            region_max_size,
            split_size,
            split_limit,
            ch,
        }
    }
}

impl<C> Coprocessor for SizeCheckObserver<C> {}

impl<C: Sender<Msg> + Send> SplitCheckObserver for SizeCheckObserver<C> {
    fn add_checker(
        &self,
        ctx: &mut ObserverContext,
        host: &mut Host,
        engine: &DB,
        mut policy: CheckPolicy,
    ) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_size = match util::get_region_approximate_size(engine, region) {
            Ok(size) => size,
            Err(e) => {
                warn!(
                    "[region {}] failed to get approximate stat: {}",
                    region_id, e
                );
                // Need to check size.
                host.add_checker(Box::new(Checker::new(
                    self.region_max_size,
                    self.split_size,
                    self.split_limit,
                    policy,
                )));
                return;
            }
        };

        // send it to rafastore to update region approximate size
        let res = Msg::RegionApproximateSize {
            region_id,
            size: region_size,
        };
        if let Err(e) = self.ch.try_send(res) {
            warn!(
                "[region {}] failed to send approximate region size: {}",
                region_id, e
            );
        }

        REGION_SIZE_HISTOGRAM.observe(region_size as f64);
        if region_size >= self.region_max_size {
            info!(
                "[region {}] approximate size {} >= {}, need to do split check",
                region.get_id(),
                region_size,
                self.region_max_size
            );
            // when meet large region use approximate way to produce split keys
            if region_size >= self.region_max_size * self.split_limit * 2 {
                policy = CheckPolicy::APPROXIMATE
            }
            // Need to check size.
            host.add_checker(Box::new(Checker::new(
                self.region_max_size,
                self.split_size,
                self.split_limit,
                policy,
            )));
        } else {
            // Does not need to check size.
            debug!(
                "[region {}] approximate size {} < {}, does not need to do split check",
                region.get_id(),
                region_size,
                self.region_max_size
            );
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::mpsc;
    use std::sync::Arc;

    use kvproto::metapb::Peer;
    use kvproto::metapb::Region;
    use kvproto::pdpb::CheckPolicy;
    use rocksdb::Writable;
    use rocksdb::{ColumnFamilyOptions, DBOptions};
    use tempdir::TempDir;

    use super::Checker;
    use raftstore::coprocessor::{Config, CoprocessorHost, ObserverContext, SplitChecker};
    use raftstore::store::{keys, KeyEntry, Msg, SplitCheckRunner, SplitCheckTask};
    use storage::{ALL_CFS, CF_WRITE};
    use util::config::ReadableSize;
    use util::properties::RangePropertiesCollectorFactory;
    use util::rocksdb::{new_engine_opt, CFOptions};
    use util::transport::RetryableSendCh;
    use util::worker::Runnable;

    pub fn must_split_at(
        rx: &mpsc::Receiver<Msg>,
        exp_region: &Region,
        exp_split_keys: Vec<Vec<u8>>,
    ) {
        loop {
            match rx.try_recv() {
                Ok(Msg::RegionApproximateSize { region_id, .. })
                | Ok(Msg::RegionApproximateKeys { region_id, .. }) => {
                    assert_eq!(region_id, exp_region.get_id());
                }
                Ok(Msg::SplitRegion {
                    region_id,
                    region_epoch,
                    split_keys,
                    ..
                }) => {
                    assert_eq!(region_id, exp_region.get_id());
                    assert_eq!(&region_epoch, exp_region.get_region_epoch());
                    assert_eq!(split_keys, exp_split_keys);
                    break;
                }
                others => panic!("expect split check result, but got {:?}", others),
            }
        }
    }

    #[test]
    fn test_split_check() {
        let path = TempDir::new("test-raftstore").unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let mut region = Region::new();
        region.set_id(1);
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        region.mut_peers().push(Peer::new());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let ch = RetryableSendCh::new(tx, "test-batch-split");
        let mut cfg = Config::default();
        cfg.region_max_size = ReadableSize(100);
        cfg.region_split_size = ReadableSize(60);
        cfg.batch_split_limit = 5;

        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            ch.clone(),
            Arc::new(CoprocessorHost::new(cfg, ch.clone())),
        );

        // so split key will be [z0006]
        for i in 0..7 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }

        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        // size has not reached the max_size 100 yet.
        match rx.try_recv() {
            Ok(Msg::RegionApproximateSize { region_id, .. }) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        for i in 7..11 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }

        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush(true).unwrap();

        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        must_split_at(&rx, &region, vec![b"0006".to_vec()]);

        // so split keys will be [z0006, z0012]
        for i in 11..19 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }
        engine.flush(true).unwrap();
        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        must_split_at(&rx, &region, vec![b"0006".to_vec(), b"0012".to_vec()]);

        // for test batch_split_limit
        // so split kets will be [z0006, z0012, z0018, z0024, z0030]
        for i in 19..51 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put(&s, &s).unwrap();
        }
        engine.flush(true).unwrap();
        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        must_split_at(
            &rx,
            &region,
            vec![
                b"0006".to_vec(),
                b"0012".to_vec(),
                b"0018".to_vec(),
                b"0024".to_vec(),
                b"0030".to_vec(),
            ],
        );

        drop(rx);
        // It should be safe even the result can't be sent back.
        runnable.run(SplitCheckTask::new(region, true, CheckPolicy::SCAN));
    }

    #[test]
    fn test_checker_with_same_max_and_split_size() {
        let mut checker = Checker::new(24, 24, 1, CheckPolicy::SCAN);
        let region = Region::default();
        let mut ctx = ObserverContext::new(&region);
        loop {
            let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 4, CF_WRITE);
            if checker.on_kv(&mut ctx, &data) {
                break;
            }
        }

        assert!(!checker.split_keys().is_empty());
    }

    #[test]
    fn test_checker_with_max_twice_bigger_than_split_size() {
        let mut checker = Checker::new(20, 10, 1, CheckPolicy::SCAN);
        let region = Region::default();
        let mut ctx = ObserverContext::new(&region);
        for _ in 0..2 {
            let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 5, CF_WRITE);
            if checker.on_kv(&mut ctx, &data) {
                break;
            }
        }

        assert!(!checker.split_keys().is_empty());
    }
}
