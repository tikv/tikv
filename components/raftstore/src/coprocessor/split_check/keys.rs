// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use engine_traits::{KvEngine, Range};
use error_code::ErrorCodeExt;
use kvproto::{metapb::Region, pdpb::CheckPolicy};
use tikv_util::{box_try, debug, info, warn};

use super::{
    super::{
        error::Result, metrics::*, Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver,
        SplitChecker,
    },
    calc_split_keys_count,
    size::get_approximate_split_keys,
    Host,
};
use crate::store::{CasualMessage, CasualRouter};

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

impl<E> SplitChecker<E> for Checker
where
    E: KvEngine,
{
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
            std::mem::take(&mut self.split_keys)
        } else {
            vec![]
        }
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }

    fn approximate_split_keys(&mut self, region: &Region, engine: &E) -> Result<Vec<Vec<u8>>> {
        if self.batch_split_limit != 0 {
            let region_key_count = get_region_approximate_keys(
                engine,
                region,
                self.max_keys_count * self.batch_split_limit,
            )?;
            let split_keys_count = calc_split_keys_count(
                region_key_count,
                self.split_threshold,
                self.max_keys_count,
                self.batch_split_limit,
            );
            if split_keys_count >= 1 {
                return Ok(box_try!(get_approximate_split_keys(
                    engine,
                    region,
                    split_keys_count,
                )));
            }
        }
        Ok(vec![])
    }
}

#[derive(Clone)]
pub struct KeysCheckObserver<C, E> {
    router: Arc<Mutex<C>>,
    _phantom: PhantomData<E>,
}

impl<C: CasualRouter<E>, E> KeysCheckObserver<C, E>
where
    E: KvEngine,
{
    pub fn new(router: C) -> KeysCheckObserver<C, E> {
        KeysCheckObserver {
            router: Arc::new(Mutex::new(router)),
            _phantom: PhantomData,
        }
    }
}

impl<C: Send, E: Send> Coprocessor for KeysCheckObserver<C, E> {}

impl<C: CasualRouter<E> + Send, E> SplitCheckObserver<E> for KeysCheckObserver<C, E>
where
    E: KvEngine,
{
    fn add_checker(
        &self,
        ctx: &mut ObserverContext<'_>,
        host: &mut Host<'_, E>,
        engine: &E,
        policy: CheckPolicy,
    ) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_keys = match get_region_approximate_keys(
            engine,
            region,
            host.cfg.region_max_keys() * host.cfg.batch_split_limit,
        ) {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    "failed to get approximate keys";
                    "region_id" => region_id,
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                // Need to check keys.
                host.add_checker(Box::new(Checker::new(
                    host.cfg.region_max_keys(),
                    host.cfg.region_split_keys(),
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
                "error_code" => %e.error_code(),
            );
        }

        REGION_KEYS_HISTOGRAM.observe(region_keys as f64);
        // if bucket checker using scan is added, to utilize the scan,
        // add keys checker as well for free
        // It has the assumption that the size's checker is before the keys's check in the host
        let need_split_region = region_keys >= host.cfg.region_max_keys();
        if need_split_region {
            info!(
                "approximate keys over threshold, need to do split check";
                "region_id" => region.get_id(),
                "keys" => region_keys,
                "threshold" => host.cfg.region_max_keys(),
            );
            // Need to check keys.
            host.add_checker(Box::new(Checker::new(
                host.cfg.region_max_keys(),
                host.cfg.region_split_keys(),
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
pub fn get_region_approximate_keys(
    db: &impl KvEngine,
    region: &Region,
    large_threshold: u64,
) -> Result<u64> {
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    Ok(box_try!(
        db.get_range_approximate_keys(range, large_threshold)
    ))
}

#[cfg(test)]
mod tests {
    use std::{cmp, sync::mpsc, u64};

    use engine_test::ctor::{CFOptions, ColumnFamilyOptions, DBOptions};
    use engine_traits::{KvEngine, MiscExt, SyncMutable, ALL_CFS, CF_DEFAULT, CF_WRITE, LARGE_CFS};
    use kvproto::{
        metapb::{Peer, Region},
        pdpb::CheckPolicy,
    };
    use tempfile::Builder;
    use tikv_util::{config::ReadableSize, worker::Runnable};
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::{
        super::{
            calc_split_keys_count,
            size::{
                get_region_approximate_size,
                tests::{must_split_at, must_split_with},
            },
        },
        *,
    };
    use crate::{
        coprocessor::{Config, CoprocessorHost},
        store::{CasualMessage, SplitCheckRunner, SplitCheckTask},
    };

    fn put_data(engine: &impl KvEngine, mut start_idx: u64, end_idx: u64, fill_short_value: bool) {
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
                engine.put_cf(CF_WRITE, &key, &write_value).unwrap();
                engine.put_cf(CF_DEFAULT, &key, &[0; 1024]).unwrap();
            }
            // Flush to generate SST files, so that properties can be utilized.
            engine.flush_cf(CF_WRITE, true).unwrap();
            engine.flush_cf(CF_DEFAULT, true).unwrap();
            start_idx = batch_idx;
        }
    }

    #[test]
    fn test_split_check() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cf_opts = ColumnFamilyOptions::new();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_keys: Some(100),
            region_split_keys: Some(80),
            batch_split_limit: 5,
            ..Default::default()
        };

        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        // so split key will be z0080
        put_data(&engine, 0, 90, false);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
            None,
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
            None,
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
            None,
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
            None,
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
        runnable.run(SplitCheckTask::split_check(
            region,
            true,
            CheckPolicy::Scan,
            None,
        ));
    }

    #[test]
    fn test_split_check_by_approximate_keys() {
        let path = Builder::new()
            .prefix("_test_split_check_by_approximate_keys")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cf_opts = ColumnFamilyOptions::new();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_keys: Some(100),
            region_split_keys: Some(80),
            batch_split_limit: 5,
            region_size_threshold_for_approximate: ReadableSize(1), // use approximate anyway
            ..Default::default()
        };

        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        put_data(&engine, 0, 90, false);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Approximate,
            None,
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
            CheckPolicy::Approximate,
            None,
        ));
        must_split_with(&rx, &region, 1);

        drop(rx);
    }

    #[test]
    fn test_region_approximate_keys() {
        let path = Builder::new()
            .prefix("_test_region_approximate_keys")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

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
            db.put_cf(CF_WRITE, &key, &write_v).unwrap();
            db.flush_cf(CF_WRITE, true).unwrap();

            let default_v = vec![0; vlen as usize];
            db.put_cf(CF_DEFAULT, &key, &default_v).unwrap();
            db.flush_cf(CF_DEFAULT, true).unwrap();
        }

        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let range_keys = get_region_approximate_keys(&db, &region, 0).unwrap();
        assert_eq!(range_keys, cases.len() as u64);
    }

    fn test_calc_split_keys_count_impl(
        split_size: ReadableSize,
        max_region_size: ReadableSize,
        split_limit: u64,
    ) {
        let region_sizes = vec![
            ReadableSize::mb(max_region_size.as_mb() + 1),
            ReadableSize::mb(split_size.as_mb() * 2),
            ReadableSize::mb(split_size.as_mb() * 2 + 1),
            ReadableSize::mb(split_size.as_mb() * split_limit),
            ReadableSize::mb(split_size.as_mb() * split_limit + 1),
            ReadableSize::mb(split_size.as_mb() * (split_limit + 1)),
        ];
        for region_size in region_sizes {
            let split_count =
                calc_split_keys_count(region_size.0, split_size.0, max_region_size.0, split_limit);
            let avg_split_size = region_size.0 / (split_count + 1);
            let diff = if avg_split_size >= split_size.0 {
                avg_split_size - split_size.0
            } else {
                split_size.0 - avg_split_size
            };
            // the diff of actual split size and split_size is less than 1/3 split_size
            assert!(
                diff <= split_size.0 / 3,
                "region_size={}mb, {} {}",
                region_size.as_mb(),
                avg_split_size,
                split_size.0
            );

            // splitted size should be no more than max_region_size
            assert!(
                avg_split_size <= max_region_size.0,
                "region_size={}mb, {} {}",
                region_size.as_mb(),
                avg_split_size,
                max_region_size.0
            );
        }
    }
    #[test]
    fn test_calc_split_keys_count() {
        // test under default settings
        let split_size = ReadableSize::mb(96);
        let max_region_size = ReadableSize::mb(144);
        let split_limit = 10;
        test_calc_split_keys_count_impl(split_size, max_region_size, split_limit);

        // test under 256MB region size
        let split_size = ReadableSize::mb(256);
        let max_region_size = ReadableSize::mb(392);
        let split_limit = 4;
        test_calc_split_keys_count_impl(split_size, max_region_size, split_limit);
        let split_size = ReadableSize::gb(1);
        let max_region_size = ReadableSize::mb(1500);
        let split_keys_count = calc_split_keys_count(
            ReadableSize::mb(2300).0,
            split_size.0,
            max_region_size.0,
            split_limit,
        );
        assert_eq!(split_keys_count, 1);
        let split_keys_count = calc_split_keys_count(
            ReadableSize::mb(3000).0,
            split_size.0,
            max_region_size.0,
            split_limit,
        );
        assert_eq!(split_keys_count, 2);
    }

    #[test]
    fn test_check_split_region_with_scan_keys_and_bucket_enabled() {
        let path = Builder::new()
            .prefix("_test_check_split_region_with_scan_keys_and_bucket_enabled")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cf_opts = ColumnFamilyOptions::new();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(vec![]);
        region.set_end_key(vec![]);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let mut cfg = Config {
            // to make the expected split key generated very late as total count is 160
            region_max_keys: Some(159),
            region_split_keys: Some(80),
            batch_split_limit: 5,
            enable_region_bucket: true,
            // need check split region buckets, but region size does not exceed the split threshold
            region_bucket_size: ReadableSize(100),
            ..Default::default()
        };

        let mut runnable = SplitCheckRunner::new(
            engine.clone(),
            tx.clone(),
            CoprocessorHost::new(tx.clone(), cfg.clone()),
        );

        put_data(&engine, 0, 90, false);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
            None,
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
        let region_size =
            get_region_approximate_size(&engine, &region, ReadableSize::mb(1000).0).unwrap();
        // to make the region_max_size < region_split_size + region_size
        // The split by keys should still work. But if the bug in on_kv() in size.rs exists,
        // it will result in split by keys failed.
        cfg.region_max_size = Some(ReadableSize(region_size * 6 / 5));
        cfg.region_split_size = ReadableSize(region_size * 4 / 5);
        runnable = SplitCheckRunner::new(engine, tx.clone(), CoprocessorHost::new(tx, cfg));
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
            None,
        ));
        must_split_with(&rx, &region, 1);

        drop(rx);
    }

    #[test]
    fn test_region_approximate_keys_sub_region() {
        let path = Builder::new()
            .prefix("_test_region_approximate_keys_sub_region")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

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
            db.put_cf(CF_WRITE, &key, &write_v).unwrap();

            let default_v = vec![0; vlen as usize];
            db.put_cf(CF_DEFAULT, &key, &default_v).unwrap();
        }
        // only flush once, so that mvcc properties will insert one point only
        db.flush_cf(CF_WRITE, true).unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // range properties get 0, mvcc properties get 3
        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(b"b1".to_vec());
        region.set_end_key(b"b2".to_vec());
        region.mut_peers().push(Peer::default());
        let range_keys = get_region_approximate_keys(&db, &region, 0).unwrap();
        assert_eq!(range_keys, 0);

        // range properties get 1, mvcc properties get 3
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"c".to_vec());
        let range_keys = get_region_approximate_keys(&db, &region, 0).unwrap();
        assert_eq!(range_keys, 1);
    }
}
