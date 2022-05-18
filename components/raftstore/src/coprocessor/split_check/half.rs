// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Range};
use kvproto::{metapb::Region, pdpb::CheckPolicy};
use tikv_util::{box_try, config::ReadableSize};

use super::{
    super::{
        error::Result, Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker,
    },
    Host,
};

const BUCKET_NUMBER_LIMIT: usize = 1024;
const BUCKET_SIZE_LIMIT_MB: u64 = 512;

pub struct Checker {
    buckets: Vec<Vec<u8>>,
    cur_bucket_size: u64,
    each_bucket_size: u64,
    policy: CheckPolicy,
}

impl Checker {
    fn new(each_bucket_size: u64, policy: CheckPolicy) -> Checker {
        Checker {
            each_bucket_size,
            cur_bucket_size: 0,
            buckets: vec![],
            policy,
        }
    }
}

impl<E> SplitChecker<E> for Checker
where
    E: KvEngine,
{
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, entry: &KeyEntry) -> bool {
        if self.buckets.is_empty() || self.cur_bucket_size >= self.each_bucket_size {
            self.buckets.push(entry.key().to_vec());
            self.cur_bucket_size = 0;
        }
        self.cur_bucket_size += entry.entry_size() as u64;
        false
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        let mid = self.buckets.len() / 2;
        if mid == 0 {
            vec![]
        } else {
            let data_key = self.buckets.swap_remove(mid);
            let key = keys::origin_key(&data_key).to_vec();
            vec![key]
        }
    }

    fn approximate_split_keys(&mut self, region: &Region, engine: &E) -> Result<Vec<Vec<u8>>> {
        let ks = box_try!(
            get_region_approximate_middle(engine, region)
                .map(|keys| keys.map_or(vec![], |key| vec![key]))
        );

        Ok(ks)
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

#[derive(Clone)]
pub struct HalfCheckObserver;

impl Coprocessor for HalfCheckObserver {}

impl<E> SplitCheckObserver<E> for HalfCheckObserver
where
    E: KvEngine,
{
    fn add_checker(
        &self,
        _: &mut ObserverContext<'_>,
        host: &mut Host<'_, E>,
        _: &E,
        policy: CheckPolicy,
    ) {
        if host.auto_split() {
            return;
        }
        host.add_checker(Box::new(Checker::new(
            half_split_bucket_size(host.cfg.region_max_size.0),
            policy,
        )))
    }
}

fn half_split_bucket_size(region_max_size: u64) -> u64 {
    let mut half_split_bucket_size = region_max_size / BUCKET_NUMBER_LIMIT as u64;
    let bucket_size_limit = ReadableSize::mb(BUCKET_SIZE_LIMIT_MB).0;
    if half_split_bucket_size == 0 {
        half_split_bucket_size = 1;
    } else if half_split_bucket_size > bucket_size_limit {
        half_split_bucket_size = bucket_size_limit;
    }
    half_split_bucket_size
}

/// Get region approximate middle key based on default and write cf size.
pub fn get_region_approximate_middle(
    db: &impl KvEngine,
    region: &Region,
) -> Result<Option<Vec<u8>>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let range = Range::new(&start_key, &end_key);
    Ok(box_try!(
        db.get_range_approximate_split_keys(range, 1)
            .map(|mut v| v.pop())
    ))
}

#[cfg(test)]
mod tests {
    use std::{iter, sync::mpsc};

    use engine_test::ctor::{CFOptions, ColumnFamilyOptions, DBOptions};
    use engine_traits::{MiscExt, SyncMutable, ALL_CFS, CF_DEFAULT, LARGE_CFS};
    use kvproto::{
        metapb::{Peer, Region},
        pdpb::CheckPolicy,
    };
    use tempfile::Builder;
    use tikv_util::{config::ReadableSize, escape, worker::Runnable};
    use txn_types::Key;

    use super::{
        super::size::tests::{must_generate_buckets, must_split_at},
        *,
    };
    use crate::{
        coprocessor::{Config, CoprocessorHost},
        store::{BucketRange, CasualMessage, SplitCheckRunner, SplitCheckTask},
    };

    #[test]
    fn test_split_check() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let cf_opts = ColumnFamilyOptions::new();
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let engine = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_size: ReadableSize(BUCKET_NUMBER_LIMIT as u64),
            ..Default::default()
        };
        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        // so split key will be z0005
        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
            None,
        ));
        let split_key = Key::from_raw(b"0005");
        must_split_at(&rx, &region, vec![split_key.clone().into_encoded()]);
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Approximate,
            None,
        ));
        must_split_at(&rx, &region, vec![split_key.into_encoded()]);
    }

    fn test_generate_region_bucket_impl(mvcc: bool) {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let cf_opts = ColumnFamilyOptions::new();
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let engine = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_size: ReadableSize(BUCKET_NUMBER_LIMIT as u64),
            enable_region_bucket: true,
            region_bucket_size: ReadableSize(20_u64), // so that each key below will form a bucket
            ..Default::default()
        };
        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        let key_gen = |k: &[u8], i: u64, mvcc: bool| {
            if !mvcc {
                keys::data_key(Key::from_raw(k).as_encoded())
            } else {
                keys::data_key(Key::from_raw(k).append_ts(i.into()).as_encoded())
            }
        };
        // so bucket key will be all these keys
        let mut exp_bucket_keys = vec![];
        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            exp_bucket_keys.push(Key::from_raw(&k).as_encoded().clone());
            let k = key_gen(&k, i, mvcc);
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
            None,
        ));
        must_generate_buckets(&rx, &exp_bucket_keys);

        exp_bucket_keys.clear();

        // now insert a few keys to grow the bucket 0001
        let start = format!("{:04}", 1).into_bytes();
        let end = format!("{:04}", 2).into_bytes();
        let bucket_range = BucketRange(
            Key::from_raw(&start).as_encoded().clone(),
            Key::from_raw(&end).as_encoded().clone(),
        );
        for i in 10..20 {
            let k = format!("{:05}", i).into_bytes();
            exp_bucket_keys.push(Key::from_raw(&k).as_encoded().clone());
            let k = key_gen(&k, i, mvcc);
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
            Some(vec![bucket_range]),
        ));

        must_generate_buckets(&rx, &exp_bucket_keys);

        // testing split bucket with end key ""
        exp_bucket_keys.clear();

        // now insert a few keys to grow the bucket 0010
        let start = format!("{:04}", 10).into_bytes();
        let bucket_range = BucketRange(Key::from_raw(&start).as_encoded().clone(), vec![]);
        for i in 11..20 {
            let k = format!("{:04}", i).into_bytes();
            exp_bucket_keys.push(Key::from_raw(&k).as_encoded().clone());
            let k = key_gen(&k, i, mvcc);
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
            Some(vec![bucket_range]),
        ));

        must_generate_buckets(&rx, &exp_bucket_keys);

        exp_bucket_keys.clear();
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
            Some(vec![]), // empty bucket, no buckets are expected
        ));

        must_generate_buckets(&rx, &exp_bucket_keys);
    }

    #[test]
    fn test_generate_region_bucket() {
        test_generate_region_bucket_impl(false);
    }

    #[test]
    fn test_generate_region_bucket_mvcc() {
        test_generate_region_bucket_impl(true);
    }

    #[test]
    fn test_generate_region_bucket_with_deleting_data() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::default();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let cf_opts = ColumnFamilyOptions::new();
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let engine = engine_test::kv::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_size: ReadableSize(BUCKET_NUMBER_LIMIT as u64),
            enable_region_bucket: true,
            region_bucket_size: ReadableSize(20_u64), // so that each key below will form a bucket
            ..Default::default()
        };
        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        // so bucket key will be all these keys
        let mut exp_bucket_keys = vec![];
        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            exp_bucket_keys.push(Key::from_raw(&k).as_encoded().clone());
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
            None,
        ));
        must_generate_buckets(&rx, &exp_bucket_keys);

        exp_bucket_keys.clear();

        // use non-existing bucket-range to simulate deleted data
        // [0000,0002] [00032, 00035], [0004,0006], [0012, 0015], [0016, 0017]
        //  non-empty       empty         non-empty     empty       empty
        let mut starts = vec![format!("{:04}", 0).into_bytes()];
        let mut ends = vec![format!("{:04}", 2).into_bytes()];
        starts.push(format!("{:05}", 32).into_bytes());
        ends.push(format!("{:05}", 35).into_bytes());
        starts.push(format!("{:04}", 4).into_bytes());
        ends.push(format!("{:04}", 6).into_bytes());
        starts.push(format!("{:04}", 12).into_bytes());
        ends.push(format!("{:04}", 15).into_bytes());
        starts.push(format!("{:04}", 16).into_bytes());
        ends.push(format!("{:04}", 17).into_bytes());
        let mut bucket_range_list = vec![BucketRange(
            Key::from_raw(&starts[0]).as_encoded().clone(),
            Key::from_raw(&ends[0]).as_encoded().clone(),
        )];
        for i in 1..starts.len() {
            bucket_range_list.push(BucketRange(
                Key::from_raw(&starts[i]).as_encoded().clone(),
                Key::from_raw(&ends[i]).as_encoded().clone(),
            ))
        }

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            false,
            CheckPolicy::Scan,
            Some(bucket_range_list),
        ));

        loop {
            if let Ok((
                _,
                CasualMessage::RefreshRegionBuckets {
                    region_epoch: _,
                    buckets,
                    bucket_ranges,
                    ..
                },
            )) = rx.try_recv()
            {
                assert_eq!(buckets.len(), bucket_ranges.unwrap().len());
                assert_eq!(buckets.len(), 5);
                for i in 0..5 {
                    if i == 0 || i == 2 {
                        assert!(!buckets[i].keys.is_empty());
                        assert!(buckets[i].size > 0);
                    } else {
                        assert!(buckets[i].keys.is_empty());
                        assert_eq!(buckets[i].size, 0);
                    }
                }
                break;
            }
        }
    }

    #[test]
    fn test_get_region_approximate_middle_cf() {
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
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }

        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let middle_key = get_region_approximate_middle(&engine, &region)
            .unwrap()
            .unwrap();

        let middle_key = Key::from_encoded_slice(keys::origin_key(&middle_key))
            .into_raw()
            .unwrap();
        assert_eq!(escape(&middle_key), "key_050");
    }
}
