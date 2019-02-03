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

use crate::raftstore::store::{keys, util, Msg, PeerMsg};
use crate::util::transport::{RetryableSendCh, Sender};
use kvproto::pdpb::CheckPolicy;
use rocksdb::DB;
use std::mem;
use std::sync::Mutex;

use super::super::metrics::*;
use super::super::{Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker};
use super::Host;

pub struct Checker {
    max_keys_count: u64,
    split_threshold: u64,
    current_count: u64,
    split_keys: Vec<Vec<u8>>,
    batch_split_limit: u64,
    policy: CheckPolicy,
}

impl Checker {
    pub fn new(
        max_keys_count: u64,
        split_threshold: u64,
        batch_split_limit: u64,
        policy: CheckPolicy,
    ) -> Checker {
        Checker {
            max_keys_count,
            split_threshold,
            current_count: 0,
            split_keys: Vec::with_capacity(1),
            batch_split_limit,
            policy,
        }
    }
}

impl SplitChecker for Checker {
    fn on_kv(&mut self, _: &mut ObserverContext, key: &KeyEntry) -> bool {
        if !key.is_commit_version() {
            return false;
        }
        self.current_count += 1;

        let mut over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        if self.current_count > self.split_threshold && !over_limit {
            self.split_keys.push(keys::origin_key(key.key()).to_vec());
            // if for previous on_kv() self.current_count == self.split_threshold,
            // the split key would be pushed this time, but the entry for this time should not be ignored.
            self.current_count = 1;
            over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        }

        // For a large region, scan over the range maybe cost too much time,
        // so limit the number of produced split_key for one batch.
        // Also need to scan over self.max_keys_count for last part.
        over_limit && self.current_count + self.split_threshold >= self.max_keys_count
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        // make sure not to split when less than max_keys_count for last part
        if self.current_count + self.split_threshold < self.max_keys_count {
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
}

pub struct KeysCheckObserver<C> {
    region_max_keys: u64,
    split_keys: u64,
    batch_split_limit: u64,
    ch: Mutex<RetryableSendCh<Msg, C>>,
}

impl<C: Sender<Msg>> KeysCheckObserver<C> {
    pub fn new(
        region_max_keys: u64,
        split_keys: u64,
        batch_split_limit: u64,
        ch: RetryableSendCh<Msg, C>,
    ) -> KeysCheckObserver<C> {
        KeysCheckObserver {
            region_max_keys,
            split_keys,
            batch_split_limit,
            ch: Mutex::new(ch),
        }
    }
}

impl<C> Coprocessor for KeysCheckObserver<C> {}

impl<C: Sender<Msg> + Send> SplitCheckObserver for KeysCheckObserver<C> {
    fn add_checker(
        &self,
        ctx: &mut ObserverContext,
        host: &mut Host,
        engine: &DB,
        policy: CheckPolicy,
    ) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_keys = match util::get_region_approximate_keys(engine, region) {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    "[region {}] failed to get approximate keys: {}",
                    region_id, e
                );
                // Need to check keys.
                host.add_checker(Box::new(Checker::new(
                    self.region_max_keys,
                    self.split_keys,
                    self.batch_split_limit,
                    policy,
                )));
                return;
            }
        };

        let res = Msg::PeerMsg(PeerMsg::RegionApproximateKeys {
            region_id,
            keys: region_keys,
        });
        if let Err(e) = self.ch.lock().unwrap().try_send(res) {
            warn!(
                "[region {}] failed to send approximate region keys: {}",
                region_id, e
            );
        }

        REGION_KEYS_HISTOGRAM.observe(region_keys as f64);
        if region_keys >= self.region_max_keys {
            info!(
                "[region {}] approximate keys {} >= {}, need to do split check",
                region.get_id(),
                region_keys,
                self.region_max_keys
            );
            // Need to check keys.
            host.add_checker(Box::new(Checker::new(
                self.region_max_keys,
                self.split_keys,
                self.batch_split_limit,
                policy,
            )));
        } else {
            // Does not need to check keys.
            debug!(
                "[region {}] approximate keys {} < {}, does not need to do split check",
                region.get_id(),
                region_keys,
                self.region_max_keys
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp;
    use std::sync::{mpsc, Arc};

    use kvproto::metapb::{Peer, Region};
    use kvproto::pdpb::CheckPolicy;
    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable, DB};
    use tempdir::TempDir;

    use crate::raftstore::store::{keys, Msg, PeerMsg, SplitCheckRunner, SplitCheckTask};
    use crate::storage::mvcc::{Write, WriteType};
    use crate::storage::{Key, ALL_CFS, CF_DEFAULT, CF_WRITE};
    use crate::util::rocksdb_util::{
        new_engine_opt, properties::RangePropertiesCollectorFactory, CFOptions,
    };
    use crate::util::transport::RetryableSendCh;
    use crate::util::worker::Runnable;

    use crate::raftstore::coprocessor::{Config, CoprocessorHost};

    use super::super::size::tests::must_split_at;

    fn put_data(engine: &DB, mut start_idx: u64, end_idx: u64, fill_short_value: bool) {
        let write_cf = engine.cf_handle(CF_WRITE).unwrap();
        let default_cf = engine.cf_handle(CF_DEFAULT).unwrap();
        let write_value = if fill_short_value {
            Write::new(WriteType::Put, 0, Some(b"shortvalue".to_vec()))
        } else {
            Write::new(WriteType::Put, 0, None)
        }
        .to_bytes();

        while start_idx < end_idx {
            let batch_idx = cmp::min(start_idx + 20, end_idx);
            for i in start_idx..batch_idx {
                let key = keys::data_key(
                    Key::from_raw(format!("{:04}", i).as_bytes())
                        .append_ts(2)
                        .as_encoded(),
                );
                engine.put_cf(write_cf, &key, &write_value).unwrap();
                engine.put_cf(default_cf, &key, &[0; 1024]).unwrap();
            }
            // Flush to generate SST files, so that properties can be utilized.
            engine.flush_cf(write_cf, true).unwrap();
            engine.flush_cf(default_cf, true).unwrap();
            start_idx = batch_idx;
        }
    }

    #[test]
    fn test_split_check() {
        let path = TempDir::new("test-raftstore").unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);

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
        cfg.region_max_keys = 100;
        cfg.region_split_keys = 80;
        cfg.batch_split_limit = 5;

        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            ch.clone(),
            Arc::new(CoprocessorHost::new(cfg, ch.clone())),
        );

        // so split key will be z0080
        put_data(&engine, 0, 90, false);
        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        // keys has not reached the max_keys 100 yet.
        match rx.try_recv() {
            Ok(Msg::PeerMsg(PeerMsg::RegionApproximateSize { region_id, .. }))
            | Ok(Msg::PeerMsg(PeerMsg::RegionApproximateKeys { region_id, .. })) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        put_data(&engine, 90, 160, true);
        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        must_split_at(
            &rx,
            &region,
            vec![Key::from_raw(b"0080").append_ts(2).into_encoded()],
        );

        put_data(&engine, 160, 300, false);
        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        must_split_at(
            &rx,
            &region,
            vec![
                Key::from_raw(b"0080").append_ts(2).into_encoded(),
                Key::from_raw(b"0160").append_ts(2).into_encoded(),
                Key::from_raw(b"0240").append_ts(2).into_encoded(),
            ],
        );

        put_data(&engine, 300, 500, false);
        runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::SCAN));
        must_split_at(
            &rx,
            &region,
            vec![
                Key::from_raw(b"0080").append_ts(2).into_encoded(),
                Key::from_raw(b"0160").append_ts(2).into_encoded(),
                Key::from_raw(b"0240").append_ts(2).into_encoded(),
                Key::from_raw(b"0320").append_ts(2).into_encoded(),
                Key::from_raw(b"0400").append_ts(2).into_encoded(),
            ],
        );

        drop(rx);
        // It should be safe even the result can't be sent back.
        runnable.run(SplitCheckTask::new(region, true, CheckPolicy::SCAN));
    }
}
