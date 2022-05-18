// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

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
    Host,
};
use crate::store::{CasualMessage, CasualRouter};

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

impl<E> SplitChecker<E> for Checker
where
    E: KvEngine,
{
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, entry: &KeyEntry) -> bool {
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
            return Ok(box_try!(get_approximate_split_keys(
                engine,
                region,
                self.batch_split_limit,
            )));
        }
        Ok(vec![])
    }
}

#[derive(Clone)]
pub struct SizeCheckObserver<C, E> {
    router: Arc<Mutex<C>>,
    _phantom: PhantomData<E>,
}

impl<C: CasualRouter<E>, E> SizeCheckObserver<C, E>
where
    E: KvEngine,
{
    pub fn new(router: C) -> SizeCheckObserver<C, E> {
        SizeCheckObserver {
            router: Arc::new(Mutex::new(router)),
            _phantom: PhantomData,
        }
    }
}

impl<C: Send, E: Send> Coprocessor for SizeCheckObserver<C, E> {}

impl<C: CasualRouter<E> + Send, E> SplitCheckObserver<E> for SizeCheckObserver<C, E>
where
    E: KvEngine,
{
    fn add_checker(
        &self,
        ctx: &mut ObserverContext<'_>,
        host: &mut Host<'_, E>,
        engine: &E,
        mut policy: CheckPolicy,
    ) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_size = match get_region_approximate_size(
            engine,
            region,
            host.cfg.region_max_size().0 * host.cfg.batch_split_limit,
        ) {
            Ok(size) => size,
            Err(e) => {
                warn!(
                    "failed to get approximate stat";
                    "region_id" => region_id,
                    "err" => %e,
                    "error_code" => %e.error_code(),
                );
                // Need to check size.
                host.add_checker(Box::new(Checker::new(
                    host.cfg.region_max_size().0,
                    host.cfg.region_split_size.0,
                    host.cfg.batch_split_limit,
                    policy,
                )));
                return;
            }
        };

        // send it to raftstore to update region approximate size
        let res = CasualMessage::RegionApproximateSize { size: region_size };
        if let Err(e) = self.router.lock().unwrap().send(region_id, res) {
            warn!(
                "failed to send approximate region size";
                "region_id" => region_id,
                "err" => %e,
                "error_code" => %e.error_code(),
            );
        }

        REGION_SIZE_HISTOGRAM.observe(region_size as f64);
        if region_size >= host.cfg.region_max_size().0
            || host.cfg.enable_region_bucket && region_size >= 2 * host.cfg.region_bucket_size.0
        {
            info!(
                "approximate size over threshold, need to do split check";
                "region_id" => region.get_id(),
                "size" => region_size,
                "threshold" => host.cfg.region_max_size().0,
            );
            // when meet large region use approximate way to produce split keys
            let batch_split_limit = if region_size >= host.cfg.region_max_size().0 {
                host.cfg.batch_split_limit
            } else {
                0
            };
            if region_size >= host.cfg.region_max_size().0 * host.cfg.batch_split_limit
                || region_size >= host.cfg.region_size_threshold_for_approximate.0
            {
                policy = CheckPolicy::Approximate;
            }
            // Need to check size.
            host.add_checker(Box::new(Checker::new(
                host.cfg.region_max_size().0,
                host.cfg.region_split_size.0,
                batch_split_limit,
                policy,
            )));
        } else {
            // Does not need to check size.
            debug!(
                "approximate size less than threshold, does not need to do split check";
                "region_id" => region.get_id(),
                "size" => region_size,
                "threshold" => host.cfg.region_max_size().0,
            );
        }
    }
}

/// Get the approximate size of the range.
pub fn get_region_approximate_size(
    db: &impl KvEngine,
    region: &Region,
    large_threshold: u64,
) -> Result<u64> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let range = Range::new(&start_key, &end_key);
    Ok(box_try!(
        db.get_range_approximate_size(range, large_threshold)
    ))
}

/// Get region approximate split keys based on default, write and lock cf.
fn get_approximate_split_keys(
    db: &impl KvEngine,
    region: &Region,
    batch_split_limit: u64,
) -> Result<Vec<Vec<u8>>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let range = Range::new(&start_key, &end_key);
    Ok(box_try!(db.get_range_approximate_split_keys(
        range,
        batch_split_limit as usize
    )))
}

#[cfg(test)]
pub mod tests {
    use std::{iter, sync::mpsc, u64};

    use collections::HashSet;
    use engine_test::{
        ctor::{CFOptions, ColumnFamilyOptions, DBOptions},
        kv::KvTestEngine,
    };
    use engine_traits::{
        CfName, MiscExt, SyncMutable, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE, LARGE_CFS,
    };
    use kvproto::{
        metapb::{Peer, Region},
        pdpb::CheckPolicy,
    };
    use tempfile::Builder;
    use tikv_util::{config::ReadableSize, worker::Runnable};
    use txn_types::{Key, TimeStamp};

    use super::{Checker, *};
    use crate::{
        coprocessor::{Config, CoprocessorHost, ObserverContext, SplitChecker},
        store::{BucketRange, CasualMessage, KeyEntry, SplitCheckRunner, SplitCheckTask},
    };

    fn must_split_at_impl(
        rx: &mpsc::Receiver<(u64, CasualMessage<KvTestEngine>)>,
        exp_region: &Region,
        exp_split_keys: Vec<Vec<u8>>,
        ignore_split_keys: bool,
    ) {
        loop {
            match rx.try_recv() {
                Ok((region_id, CasualMessage::RegionApproximateSize { .. }))
                | Ok((region_id, CasualMessage::RegionApproximateKeys { .. })) => {
                    assert_eq!(region_id, exp_region.get_id());
                }
                Ok((
                    region_id,
                    CasualMessage::SplitRegion {
                        region_epoch,
                        split_keys,
                        ..
                    },
                )) => {
                    assert_eq!(region_id, exp_region.get_id());
                    assert_eq!(&region_epoch, exp_region.get_region_epoch());
                    if !ignore_split_keys {
                        assert_eq!(split_keys, exp_split_keys);
                    }
                    break;
                }
                Ok((_region_id, CasualMessage::RefreshRegionBuckets { .. })) => {}
                others => panic!("expect split check result, but got {:?}", others),
            }
        }
    }

    pub fn must_split_at(
        rx: &mpsc::Receiver<(u64, CasualMessage<KvTestEngine>)>,
        exp_region: &Region,
        exp_split_keys: Vec<Vec<u8>>,
    ) {
        must_split_at_impl(rx, exp_region, exp_split_keys, false)
    }

    pub fn must_generate_buckets(
        rx: &mpsc::Receiver<(u64, CasualMessage<KvTestEngine>)>,
        exp_buckets_keys: &[Vec<u8>],
    ) {
        loop {
            if let Ok((
                _,
                CasualMessage::RefreshRegionBuckets {
                    region_epoch: _,
                    mut buckets,
                    bucket_ranges: _,
                    ..
                },
            )) = rx.try_recv()
            {
                let mut i = 0;
                if !exp_buckets_keys.is_empty() {
                    let bucket = buckets.pop().unwrap();
                    assert_eq!(bucket.keys.len(), exp_buckets_keys.len());
                    while i < bucket.keys.len() {
                        assert_eq!(bucket.keys[i], exp_buckets_keys[i]);
                        i += 1
                    }
                } else {
                    assert!(buckets.is_empty());
                }
                break;
            }
        }
    }

    pub fn must_generate_buckets_approximate(
        rx: &mpsc::Receiver<(u64, CasualMessage<KvTestEngine>)>,
        bucket_range: Option<BucketRange>,
        min_leap: i32,
        max_leap: i32,
        mvcc: bool,
    ) {
        loop {
            if let Ok((
                _,
                CasualMessage::RefreshRegionBuckets {
                    region_epoch: _,
                    mut buckets,
                    bucket_ranges: _,
                    ..
                },
            )) = rx.try_recv()
            {
                let bucket_keys = buckets.pop().unwrap().keys;
                if let Some(bucket_range) = bucket_range {
                    assert!(!bucket_keys.is_empty());
                    for i in 0..bucket_keys.len() {
                        assert!(bucket_keys[i] < bucket_range.1);
                        assert!(bucket_keys[i] >= bucket_range.0);
                    }
                }
                if bucket_keys.len() >= 2 {
                    for i in 0..bucket_keys.len() - 1 {
                        let start_vec = if !mvcc {
                            bucket_keys[i].clone()
                        } else {
                            Key::from_encoded(bucket_keys[i].clone())
                                .into_raw()
                                .unwrap()
                        };
                        let end_vec = if !mvcc {
                            bucket_keys[i + 1].clone()
                        } else {
                            Key::from_encoded(bucket_keys[i + 1].clone())
                                .into_raw()
                                .unwrap()
                        };
                        let start: i32 = std::str::from_utf8(&start_vec).unwrap().parse().unwrap();
                        let end: i32 = std::str::from_utf8(&end_vec).unwrap().parse().unwrap();
                        assert!(end - start >= min_leap && end - start < max_leap);
                    }
                }

                break;
            }
        }
    }

    fn test_split_check_impl(cfs_with_range_prop: &[CfName], data_cf: CfName) {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cfs_with_range_prop: HashSet<_> = cfs_with_range_prop.iter().cloned().collect();
        let mut cf_opt = ColumnFamilyOptions::new();
        cf_opt.set_no_range_properties(true);

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                if cfs_with_range_prop.contains(cf) {
                    CFOptions::new(cf, ColumnFamilyOptions::new())
                } else {
                    CFOptions::new(cf, cf_opt.clone())
                }
            })
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
            region_max_size: Some(ReadableSize(100)),
            region_split_size: ReadableSize(60),
            region_max_keys: Some(1000000),
            region_split_keys: Some(1000000),
            batch_split_limit: 5,
            ..Default::default()
        };

        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        // so split key will be [z0006]
        for i in 0..7 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(data_cf, &s, &s).unwrap();
        }

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
            None,
        ));
        // size has not reached the max_size 100 yet.
        match rx.try_recv() {
            Ok((region_id, CasualMessage::RegionApproximateSize { .. })) => {
                assert_eq!(region_id, region.get_id());
            }
            others => panic!("expect recv empty, but got {:?}", others),
        }

        for i in 7..11 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(data_cf, &s, &s).unwrap();
        }

        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush_cf(data_cf, true).unwrap();

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
            None,
        ));
        must_split_at(&rx, &region, vec![b"0006".to_vec()]);

        // so split keys will be [z0006, z0012]
        for i in 11..19 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(data_cf, &s, &s).unwrap();
        }
        engine.flush_cf(data_cf, true).unwrap();
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
            None,
        ));
        must_split_at(&rx, &region, vec![b"0006".to_vec(), b"0012".to_vec()]);

        // for test batch_split_limit
        // so split kets will be [z0006, z0012, z0018, z0024, z0030]
        for i in 19..41 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(data_cf, &s, &s).unwrap();
        }
        engine.flush_cf(data_cf, true).unwrap();
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
                b"0006".to_vec(),
                b"0012".to_vec(),
                b"0018".to_vec(),
                b"0024".to_vec(),
                b"0030".to_vec(),
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

    fn test_generate_bucket_impl(cfs_with_range_prop: &[CfName], data_cf: CfName, mvcc: bool) {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cfs_with_range_prop: HashSet<_> = cfs_with_range_prop.iter().cloned().collect();
        let mut cf_opt = ColumnFamilyOptions::new();
        cf_opt.set_no_range_properties(true);
        cf_opt.set_disable_auto_compactions(true);

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                if cfs_with_range_prop.contains(cf) {
                    let mut opt = ColumnFamilyOptions::new();
                    opt.set_disable_auto_compactions(true);
                    CFOptions::new(cf, opt)
                } else {
                    CFOptions::new(cf, cf_opt.clone())
                }
            })
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
            region_max_size: Some(ReadableSize(50000)),
            region_split_size: ReadableSize(50000),
            region_max_keys: Some(1000000),
            region_split_keys: Some(1000000),
            batch_split_limit: 5,
            enable_region_bucket: true,
            region_bucket_size: ReadableSize(3000),
            region_size_threshold_for_approximate: ReadableSize(50000),
            ..Default::default()
        };

        let key_gen = |bytes: &[u8], mvcc: bool, ts: TimeStamp| {
            if !mvcc {
                keys::data_key(bytes)
            } else {
                keys::data_key(Key::from_raw(bytes).append_ts(ts).as_encoded())
            }
        };
        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));
        for i in 0..2000 {
            // if not mvcc, kv size is (6+1)*2 = 14, given bucket size is 3000, expect each bucket has about 210 keys
            // if mvcc, kv size is about 18*2 = 36, expect each bucket has about 80 keys
            let s = key_gen(format!("{:04}00", i).as_bytes(), mvcc, i.into());
            engine.put_cf(data_cf, &s, &s).unwrap();
            if i % 10 == 0 && i > 0 {
                engine.flush_cf(data_cf, true).unwrap();
            }
        }

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Approximate,
            None,
        ));

        if !mvcc {
            must_generate_buckets_approximate(&rx, None, 15000, 45000, mvcc);
        } else {
            must_generate_buckets_approximate(&rx, None, 7000, 15000, mvcc);
        }

        let start = format!("{:04}", 0).into_bytes();
        let end = format!("{:04}", 20).into_bytes();

        // insert keys into 0000 ~ 0020 with 000000 ~ 002000
        for i in 0..2000 {
            // kv size is (6+1)*2 = 14, given bucket size is 3000, expect each bucket has about 210 keys
            // if mvcc, kv size is about 18*2 = 36, expect each bucket has about 80 keys
            let s = key_gen(format!("{:06}", i).as_bytes(), mvcc, i.into());
            engine.put_cf(data_cf, &s, &s).unwrap();
            if i % 10 == 0 {
                engine.flush_cf(data_cf, true).unwrap();
            }
        }

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Approximate,
            Some(vec![BucketRange(start.clone(), end.clone())]),
        ));

        if !mvcc {
            must_generate_buckets_approximate(&rx, Some(BucketRange(start, end)), 150, 450, mvcc);
        } else {
            must_generate_buckets_approximate(&rx, Some(BucketRange(start, end)), 70, 150, mvcc);
        }
        drop(rx);
    }

    #[test]
    fn test_split_check() {
        test_split_check_impl(&[CF_DEFAULT, CF_WRITE], CF_DEFAULT);
        test_split_check_impl(&[CF_DEFAULT, CF_WRITE], CF_WRITE);
        for cf in LARGE_CFS {
            test_split_check_impl(LARGE_CFS, cf);
        }
    }

    #[test]
    fn test_generate_bucket_by_approximate() {
        for cf in LARGE_CFS {
            test_generate_bucket_impl(LARGE_CFS, cf, false);
        }
    }

    #[test]
    fn test_generate_bucket_mvcc_by_approximate() {
        for cf in LARGE_CFS {
            test_generate_bucket_impl(LARGE_CFS, cf, true);
        }
    }

    #[test]
    fn test_cf_lock_without_range_prop() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let mut cf_opt = ColumnFamilyOptions::new();
        cf_opt.set_no_range_properties(true);

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                if cf != &CF_LOCK {
                    CFOptions::new(cf, ColumnFamilyOptions::new())
                } else {
                    CFOptions::new(cf, cf_opt.clone())
                }
            })
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
            region_max_size: Some(ReadableSize(100)),
            region_split_size: ReadableSize(60),
            region_max_keys: Some(1000000),
            region_split_keys: Some(1000000),
            batch_split_limit: 5,
            ..Default::default()
        };

        let mut runnable = SplitCheckRunner::new(
            engine.clone(),
            tx.clone(),
            CoprocessorHost::new(tx.clone(), cfg.clone()),
        );

        for cf in LARGE_CFS {
            for i in 0..7 {
                let s = keys::data_key(format!("{:04}", i).as_bytes());
                engine.put_cf(cf, &s, &s).unwrap();
            }
            engine.flush_cf(cf, true).unwrap();
        }

        for policy in &[CheckPolicy::Scan, CheckPolicy::Approximate] {
            runnable.run(SplitCheckTask::split_check(
                region.clone(),
                true,
                *policy,
                None,
            ));
            // Ignore the split keys. Only check whether it can split or not.
            must_split_at_impl(&rx, &region, vec![], true);
        }

        drop(engine);
        drop(runnable);

        // Reopen the engine and all cfs have range properties.
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut cf_opts = ColumnFamilyOptions::new();
                cf_opts.set_no_range_properties(true);
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let engine =
            engine_test::kv::new_engine_opt(path_str, DBOptions::default(), cfs_opts).unwrap();

        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        // Flush a sst of CF_LOCK with range properties.
        for i in 7..15 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(CF_LOCK, &s, &s).unwrap();
        }
        engine.flush_cf(CF_LOCK, true).unwrap();
        for policy in &[CheckPolicy::Scan, CheckPolicy::Approximate] {
            runnable.run(SplitCheckTask::split_check(
                region.clone(),
                true,
                *policy,
                None,
            ));
            // Ignore the split keys. Only check whether it can split or not.
            must_split_at_impl(&rx, &region, vec![], true);
        }
    }

    #[test]
    fn test_checker_with_same_max_and_split_size() {
        let mut checker = Checker::new(24, 24, 1, CheckPolicy::Scan);
        let region = Region::default();
        let mut ctx = ObserverContext::new(&region);
        loop {
            let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 4, CF_WRITE);
            if SplitChecker::<KvTestEngine>::on_kv(&mut checker, &mut ctx, &data) {
                break;
            }
        }

        assert!(!SplitChecker::<KvTestEngine>::split_keys(&mut checker).is_empty());
    }

    #[test]
    fn test_checker_with_max_twice_bigger_than_split_size() {
        let mut checker = Checker::new(20, 10, 1, CheckPolicy::Scan);
        let region = Region::default();
        let mut ctx = ObserverContext::new(&region);
        for _ in 0..2 {
            let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 5, CF_WRITE);
            if SplitChecker::<KvTestEngine>::on_kv(&mut checker, &mut ctx, &data) {
                break;
            }
        }

        assert!(!SplitChecker::<KvTestEngine>::split_keys(&mut checker).is_empty());
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
        let mut peer = Peer::default();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = Region::default();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    #[test]
    fn test_get_approximate_split_keys_error() {
        let tmp = Builder::new()
            .prefix("test_raftstore_util")
            .tempdir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::default();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        cf_opts.set_no_range_properties(true);

        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = engine_test::kv::new_engine_opt(path, db_opts, cfs_opts).unwrap();

        let region = make_region(1, vec![], vec![]);
        assert_eq!(
            get_approximate_split_keys(&engine, &region, 1).is_err(),
            true
        );

        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &big_value).unwrap();
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }
        assert_eq!(
            get_approximate_split_keys(&engine, &region, 1).is_err(),
            true
        );
    }

    fn test_get_approximate_split_keys_impl(data_cf: CfName) {
        let tmp = Builder::new()
            .prefix("test_raftstore_util")
            .tempdir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::default();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = engine_test::kv::new_engine_opt(path, db_opts, cfs_opts).unwrap();

        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));

        for i in 0..4 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(data_cf, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(data_cf, true).unwrap();
        }
        let region = make_region(1, vec![], vec![]);

        assert_eq!(
            get_approximate_split_keys(&engine, &region, 0)
                .unwrap()
                .into_iter()
                .map(|k| {
                    Key::from_encoded_slice(keys::origin_key(&k))
                        .into_raw()
                        .unwrap()
                })
                .next()
                .is_none(),
            true
        );
        for i in 4..5 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(data_cf, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(data_cf, true).unwrap();
        }
        let split_keys = get_approximate_split_keys(&engine, &region, 1)
            .unwrap()
            .into_iter()
            .map(|k| {
                Key::from_encoded_slice(keys::origin_key(&k))
                    .into_raw()
                    .unwrap()
            })
            .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_keys, vec![b"key_002".to_vec()]);

        for i in 5..10 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(data_cf, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(data_cf, true).unwrap();
        }
        let split_keys = get_approximate_split_keys(&engine, &region, 2)
            .unwrap()
            .into_iter()
            .map(|k| {
                Key::from_encoded_slice(keys::origin_key(&k))
                    .into_raw()
                    .unwrap()
            })
            .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_keys, vec![b"key_003".to_vec(), b"key_006".to_vec()]);

        for i in 10..20 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(data_cf, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(data_cf, true).unwrap();
        }
        let split_keys = get_approximate_split_keys(&engine, &region, 5)
            .unwrap()
            .into_iter()
            .map(|k| {
                Key::from_encoded_slice(keys::origin_key(&k))
                    .into_raw()
                    .unwrap()
            })
            .collect::<Vec<Vec<u8>>>();

        assert_eq!(
            split_keys,
            vec![
                b"key_003".to_vec(),
                b"key_006".to_vec(),
                b"key_010".to_vec(),
                b"key_013".to_vec(),
                b"key_016".to_vec(),
            ]
        );
    }

    #[test]
    fn test_get_approximate_split_keys() {
        for cf in LARGE_CFS {
            test_get_approximate_split_keys_impl(*cf);
        }
    }

    #[test]
    fn test_region_approximate_size() {
        let path = Builder::new()
            .prefix("_test_raftstore_region_approximate_size")
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
        let cf_size = 2 + 1024 + 2 + 2048 + 2 + 4096;
        for &(key, vlen) in &cases {
            for cfname in LARGE_CFS {
                let k1 = keys::data_key(key.as_bytes());
                let v1 = vec![0; vlen as usize];
                assert_eq!(k1.len(), 2);
                db.put_cf(cfname, &k1, &v1).unwrap();
                db.flush_cf(cfname, true).unwrap();
            }
        }

        let region = make_region(1, vec![], vec![]);
        let size = get_region_approximate_size(&db, &region, 0).unwrap();
        assert_eq!(size, cf_size * LARGE_CFS.len() as u64);
    }

    #[test]
    fn test_region_maybe_inaccurate_approximate_size() {
        let path = Builder::new()
            .prefix("_test_raftstore_region_maybe_inaccurate_approximate_size")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut cf_size = 0;
        for i in 0..100 {
            let k1 = keys::data_key(format!("k1{}", i).as_bytes());
            let k2 = keys::data_key(format!("k9{}", i).as_bytes());
            let v = vec![0; 4096];
            cf_size += k1.len() + k2.len() + v.len() * 2;
            db.put_cf(CF_DEFAULT, &k1, &v).unwrap();
            db.put_cf(CF_DEFAULT, &k2, &v).unwrap();
            db.flush_cf(CF_DEFAULT, true).unwrap();
        }

        let region = make_region(1, vec![], vec![]);
        let size = get_region_approximate_size(&db, &region, 0).unwrap();
        assert_eq!(size, cf_size as u64);

        let region = make_region(1, b"k2".to_vec(), b"k8".to_vec());
        let size = get_region_approximate_size(&db, &region, 0).unwrap();
        assert_eq!(size, 0);
    }

    use test::Bencher;

    #[bench]
    fn bench_get_region_approximate_size(b: &mut Bencher) {
        let path = Builder::new()
            .prefix("_bench_get_region_approximate_size")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut cf_size = 0;
        for i in 0..10 {
            let v = vec![0; 4096];
            for j in 10000 * i..10000 * (i + 1) {
                let k1 = keys::data_key(format!("k1{:0100}", j).as_bytes());
                let k2 = keys::data_key(format!("k9{:0100}", j).as_bytes());
                cf_size += k1.len() + k2.len() + v.len() * 2;
                db.put_cf(CF_DEFAULT, &k1, &v).unwrap();
                db.put_cf(CF_DEFAULT, &k2, &v).unwrap();
            }
            db.flush_cf(CF_DEFAULT, true).unwrap();
        }

        let region = make_region(1, vec![], vec![]);
        b.iter(|| {
            let size = get_region_approximate_size(&db, &region, 0).unwrap();
            assert_eq!(size, cf_size as u64);
        })
    }
}
