// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::{CasualMessage, CasualRouter};
use engine::rocks::DB;
use engine::rocks::{self, Range};
use engine_rocks::{Compat, RocksEngine};
use engine_traits::CF_WRITE;
use engine_traits::{CFHandleExt, TableProperties, TablePropertiesCollection, TablePropertiesExt};
use kvproto::{metapb::Region, pdpb::CheckPolicy};
use std::mem;
use std::sync::{Arc, Mutex};

use super::super::error::Result;
use super::super::metrics::*;
use super::super::properties::{get_range_entries_and_versions, RangeProperties};
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
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, key: &KeyEntry) -> bool {
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

#[derive(Clone)]
pub struct KeysCheckObserver<C> {
    router: Arc<Mutex<C>>,
}

impl<C: CasualRouter<RocksEngine>> KeysCheckObserver<C> {
    pub fn new(router: C) -> KeysCheckObserver<C> {
        KeysCheckObserver {
            router: Arc::new(Mutex::new(router)),
        }
    }
}

impl<C: Send> Coprocessor for KeysCheckObserver<C> {}

impl<C: CasualRouter<RocksEngine> + Send> SplitCheckObserver for KeysCheckObserver<C> {
    fn add_checker(
        &self,
        ctx: &mut ObserverContext<'_>,
        host: &mut Host,
        engine: &Arc<DB>,
        policy: CheckPolicy,
    ) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_keys = match get_region_approximate_keys(engine, region) {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    "failed to get approximate keys";
                    "region_id" => region_id,
                    "err" => %e,
                );
                // Need to check keys.
                host.add_checker(Box::new(Checker::new(
                    host.cfg.region_max_keys,
                    host.cfg.region_split_keys,
                    host.cfg.batch_split_limit,
                    policy,
                )));
                return;
            }
        };

        let res = CasualMessage::RegionApproximateKeys { keys: region_keys };
        if let Err(e) = self.router.lock().unwrap().send(region_id, res) {
            warn!(
                "failed to send approximate region keys";
                "region_id" => region_id,
                "err" => %e,
            );
        }

        REGION_KEYS_HISTOGRAM.observe(region_keys as f64);
        if region_keys >= host.cfg.region_max_keys {
            info!(
                "approximate keys over threshold, need to do split check";
                "region_id" => region.get_id(),
                "keys" => region_keys,
                "threshold" => host.cfg.region_max_keys,
            );
            // Need to check keys.
            host.add_checker(Box::new(Checker::new(
                host.cfg.region_max_keys,
                host.cfg.region_split_keys,
                host.cfg.batch_split_limit,
                policy,
            )));
        } else {
            // Does not need to check keys.
            debug!(
                "approximate keys less than threshold, does not need to do split check";
                "region_id" => region.get_id(),
                "keys" => region_keys,
                "threshold" => host.cfg.region_max_keys,
            );
        }
    }
}

/// Get the approximate number of keys in the range.
pub fn get_region_approximate_keys(db: &Arc<DB>, region: &Region) -> Result<u64> {
    // try to get from RangeProperties first.
    match get_region_approximate_keys_cf(db, CF_WRITE, region) {
        Ok(v) => {
            return Ok(v);
        }
        Err(e) => debug!(
            "failed to get keys from RangeProperties";
            "err" => ?e,
        ),
    }

    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let cf = box_try!(db.c().cf_handle(CF_WRITE));
    let (_, keys) = get_range_entries_and_versions(db.c(), cf, &start, &end).unwrap_or_default();
    Ok(keys)
}

pub fn get_region_approximate_keys_cf(db: &Arc<DB>, cfname: &str, region: &Region) -> Result<u64> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let cf = box_try!(rocks::util::get_cf_handle(db, cfname));
    let range = Range::new(&start_key, &end_key);
    let (mut keys, _) = db.get_approximate_memtable_stats_cf(cf, &range);

    let collection = box_try!(db.c().get_range_properties_cf(cfname, &start_key, &end_key));
    for (_, v) in collection.iter() {
        let props = box_try!(RangeProperties::decode(&v.user_collected_properties()));
        keys += props.get_approximate_keys_in_range(&start_key, &end_key);
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::super::size::tests::must_split_at;
    use crate::coprocessor::properties::{
        MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory,
    };
    use crate::coprocessor::{Config, CoprocessorHost};
    use crate::store::{CasualMessage, SplitCheckRunner, SplitCheckTask};
    use engine::rocks;
    use engine::rocks::util::{new_engine_opt, CFOptions};
    use engine::rocks::{ColumnFamilyOptions, DBOptions, Writable};
    use engine::DB;
    use engine_traits::{ALL_CFS, CF_DEFAULT, CF_WRITE, LARGE_CFS};
    use kvproto::metapb::{Peer, Region};
    use kvproto::pdpb::CheckPolicy;
    use std::cmp;
    use std::sync::{mpsc, Arc};
    use std::u64;
    use tempfile::Builder;
    use tikv_util::worker::Runnable;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;

    fn put_data(engine: &DB, mut start_idx: u64, end_idx: u64, fill_short_value: bool) {
        let write_cf = engine.cf_handle(CF_WRITE).unwrap();
        let default_cf = engine.cf_handle(CF_DEFAULT).unwrap();
        let write_value = if fill_short_value {
            Write::new(
                WriteType::Put,
                TimeStamp::zero(),
                Some(b"shortvalue".to_vec()),
            )
        } else {
            Write::new(WriteType::Put, TimeStamp::zero(), None)
        }
        .as_ref()
        .to_bytes();

        while start_idx < end_idx {
            let batch_idx = cmp::min(start_idx + 20, end_idx);
            for i in start_idx..batch_idx {
                let key = keys::data_key(
                    Key::from_raw(format!("{:04}", i).as_bytes())
                        .append_ts(2.into())
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
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
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

        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let mut cfg = Config::default();
        cfg.region_max_keys = 100;
        cfg.region_split_keys = 80;
        cfg.batch_split_limit = 5;

        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            tx.clone(),
            CoprocessorHost::new(tx),
            cfg,
        );

        // so split key will be z0080
        put_data(&engine, 0, 90, false);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
        ));
        // keys has not reached the max_keys 100 yet.
        match rx.try_recv() {
            Ok((region_id, CasualMessage::RegionApproximateSize { .. }))
            | Ok((region_id, CasualMessage::RegionApproximateKeys { .. })) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        put_data(&engine, 90, 160, true);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(
            &rx,
            &region,
            vec![Key::from_raw(b"0080").append_ts(2.into()).into_encoded()],
        );

        put_data(&engine, 160, 300, false);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(
            &rx,
            &region,
            vec![
                Key::from_raw(b"0080").append_ts(2.into()).into_encoded(),
                Key::from_raw(b"0160").append_ts(2.into()).into_encoded(),
                Key::from_raw(b"0240").append_ts(2.into()).into_encoded(),
            ],
        );

        put_data(&engine, 300, 500, false);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(
            &rx,
            &region,
            vec![
                Key::from_raw(b"0080").append_ts(2.into()).into_encoded(),
                Key::from_raw(b"0160").append_ts(2.into()).into_encoded(),
                Key::from_raw(b"0240").append_ts(2.into()).into_encoded(),
                Key::from_raw(b"0320").append_ts(2.into()).into_encoded(),
                Key::from_raw(b"0400").append_ts(2.into()).into_encoded(),
            ],
        );

        drop(rx);
        // It should be safe even the result can't be sent back.
        runnable.run(SplitCheckTask::split_check(region, true, CheckPolicy::Scan));
    }

    #[test]
    fn test_region_approximate_keys() {
        let path = Builder::new()
            .prefix("_test_region_approximate_keys")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = Arc::new(rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        for &(key, vlen) in &cases {
            let key = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(2.into())
                    .as_encoded(),
            );
            let write_v = Write::new(WriteType::Put, TimeStamp::zero(), None)
                .as_ref()
                .to_bytes();
            let write_cf = db.cf_handle(CF_WRITE).unwrap();
            db.put_cf(write_cf, &key, &write_v).unwrap();
            db.flush_cf(write_cf, true).unwrap();

            let default_v = vec![0; vlen as usize];
            let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
            db.put_cf(default_cf, &key, &default_v).unwrap();
            db.flush_cf(default_cf, true).unwrap();
        }

        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let range_keys = get_region_approximate_keys(&db, &region).unwrap();
        assert_eq!(range_keys, cases.len() as u64);
    }

    #[test]
    fn test_region_approximate_keys_sub_region() {
        let path = Builder::new()
            .prefix("_test_region_approximate_keys_sub_region")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = Arc::new(rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let write_cf = db.cf_handle(CF_WRITE).unwrap();
        let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
        // size >= 4194304 will insert a new point in range properties
        // 3 points will be inserted into range properties
        let cases = [("a", 4194304), ("b", 4194304), ("c", 4194304)];
        for &(key, vlen) in &cases {
            let key = keys::data_key(
                Key::from_raw(key.as_bytes())
                    .append_ts(2.into())
                    .as_encoded(),
            );
            let write_v = Write::new(WriteType::Put, TimeStamp::zero(), None)
                .as_ref()
                .to_bytes();
            db.put_cf(write_cf, &key, &write_v).unwrap();

            let default_v = vec![0; vlen as usize];
            db.put_cf(default_cf, &key, &default_v).unwrap();
        }
        // only flush once, so that mvcc properties will insert one point only
        db.flush_cf(write_cf, true).unwrap();
        db.flush_cf(default_cf, true).unwrap();

        // range properties get 0, mvcc properties get 3
        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(b"b1".to_vec());
        region.set_end_key(b"b2".to_vec());
        region.mut_peers().push(Peer::default());
        let range_keys = get_region_approximate_keys(&db, &region).unwrap();
        assert_eq!(range_keys, 0);

        // range properties get 1, mvcc properties get 3
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"c".to_vec());
        let range_keys = get_region_approximate_keys(&db, &region).unwrap();
        assert_eq!(range_keys, 1);
    }
}
