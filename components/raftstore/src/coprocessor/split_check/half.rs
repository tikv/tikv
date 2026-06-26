// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, Range};
use kvproto::{
    metapb::Region,
    pdpb::{CheckPolicy, SplitReason},
};
use tikv_util::{box_try, config::ReadableSize};

use super::{
    super::{
        Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker, error::Result,
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
        if host.split_reason() == SplitReason::Size {
            return;
        }
        host.add_checker(Box::new(Checker::new(
            half_split_bucket_size(host.cfg.region_max_size().0),
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
    get_region_approximate_middle_in_range(db, region, None, None)
}

/// Get region approximate middle key from an explicit encoded key range.
///
/// The provided range is clamped to region boundaries. When the resulting
/// range is empty, returns `Ok(None)`.
pub fn get_region_approximate_middle_in_range(
    db: &impl KvEngine,
    region: &Region,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
) -> Result<Option<Vec<u8>>> {
    let region_start_key = keys::enc_start_key(region);
    let region_end_key = keys::enc_end_key(region);

    let start_key = match start_key {
        Some(start_key) if start_key > region_start_key.as_slice() => start_key.to_vec(),
        _ => region_start_key,
    };
    let end_key = match end_key {
        Some(end_key)
            if !end_key.is_empty()
                && (region_end_key.is_empty() || end_key < region_end_key.as_slice()) =>
        {
            end_key.to_vec()
        }
        _ => region_end_key,
    };

    if !end_key.is_empty() && start_key >= end_key {
        return Ok(None);
    }

    let range = Range::new(&start_key, &end_key);
    Ok(box_try!(
        db.get_range_approximate_split_keys(range, 1)
            .map(|mut v| v.pop())
    ))
}

#[cfg(test)]
mod tests {
    use std::{iter, sync::mpsc, time::Duration};

    use engine_test::ctor::{CfOptions, DbOptions};
    use engine_traits::{ALL_CFS, CF_DEFAULT, LARGE_CFS, MiscExt, SyncMutable};
    use kvproto::{
        metapb::{Peer, Region},
        pdpb::{CheckPolicy, SplitReason},
    };
    use tempfile::Builder;
    use tikv_util::{config::ReadableSize, escape, worker::Runnable};
    use txn_types::Key;

    use super::{
        super::size::tests::{must_generate_buckets, must_split_at},
        *,
    };
    use crate::{
        coprocessor::{
            BoxSplitCheckObserver, Config, CoprocessorHost, KeysCheckObserver, SizeCheckObserver,
            dispatcher::SchedTask,
        },
        store::{BucketRange, SplitCheckRunner, SplitCheckTask},
    };

    fn assert_no_ask_split(rx: &mpsc::Receiver<SchedTask>) {
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(SchedTask::AskSplit { .. }) => {
                    panic!("unexpected AskSplit task emitted");
                }
                Ok(_) => {
                    // UpdateApproximate* and bucket refresh are expected side effects.
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn assert_ask_split_in_range(
        rx: &mpsc::Receiver<SchedTask>,
        region: &Region,
        start_key: &[u8],
        end_key: &[u8],
    ) {
        loop {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Ok(SchedTask::AskSplit {
                    region_id,
                    split_keys,
                    source,
                    ..
                }) => {
                    assert_eq!(region_id, region.get_id());
                    assert_eq!(source, "split_checker_by_load");
                    assert_eq!(split_keys.len(), 1);
                    let split_key = &split_keys[0];
                    assert!(split_key.as_slice() > start_key);
                    if !end_key.is_empty() {
                        assert!(split_key.as_slice() < end_key);
                    }
                    return;
                }
                Ok(_) => {}
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    panic!("expected AskSplit task, but none received")
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("expected AskSplit task, but channel disconnected")
                }
            }
        }
    }

    /// SplitCheckerHost should pick Half/Size/Keys observers based on
    /// SplitReason
    #[test]
    fn test_new_split_checker_host_with_different_split_reasons() {
        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Default::default());

        let (tx, _rx) = mpsc::sync_channel(100);
        let mut cfg = Config::default();
        cfg.region_max_size = Some(ReadableSize(0));
        cfg.region_max_keys = Some(0);
        let host = CoprocessorHost::new(tx, cfg.clone());

        let engine = engine_test::kv::new_engine(
            Builder::new()
                .prefix("test-new-split-checker-host")
                .tempdir()
                .unwrap()
                .path()
                .to_str()
                .unwrap(),
            ALL_CFS,
        )
        .unwrap();

        let split_host =
            host.new_split_checker_host(&region, &engine, SplitReason::Size, CheckPolicy::Scan);
        assert_eq!(split_host.checkers_count(), 2);

        let split_host =
            host.new_split_checker_host(&region, &engine, SplitReason::Load, CheckPolicy::Scan);
        assert_eq!(split_host.checkers_count(), 3);

        let split_host =
            host.new_split_checker_host(&region, &engine, SplitReason::Admin, CheckPolicy::Scan);
        assert_eq!(split_host.checkers_count(), 3);
    }

    /// Load split should use HalfCheckObserver to find the midpoint split key
    #[test]
    fn test_load_split_finds_correct_split_key() {
        let path = Builder::new().prefix("test-load-split").tempdir().unwrap();
        let engine = engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_size: Some(ReadableSize(100)),
            ..Default::default()
        };
        let mut host = CoprocessorHost::new(tx.clone(), cfg);
        host.registry
            .register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        host.registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(tx.clone())),
        );
        host.registry.register_split_check_observer(
            300,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(tx.clone())),
        );

        let mut runnable = SplitCheckRunner::new(engine.clone(), tx, host, None);

        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }

        let start_key = Key::from_raw(b"0000").into_encoded();
        let end_key = Key::from_raw(b"0010").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region.clone(),
            Some(start_key),
            Some(end_key),
            SplitReason::Load,
            CheckPolicy::Scan,
            None,
        ));

        let expected_split_key = Key::from_raw(b"0006");
        must_split_at(&rx, &region, vec![expected_split_key.into_encoded()]);
    }

    #[test]
    fn test_split_check() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let engine = engine_test::kv::new_engine(path_str, ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_size: Some(ReadableSize(BUCKET_NUMBER_LIMIT as u64)),
            ..Default::default()
        };
        let mut runnable = SplitCheckRunner::new(
            engine.clone(),
            tx.clone(),
            CoprocessorHost::new(tx, cfg),
            None,
        );

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

    #[test]
    fn test_split_check_with_key_range() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let engine = engine_test::kv::new_engine(path_str, ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_max_size: Some(ReadableSize(BUCKET_NUMBER_LIMIT as u64)),
            ..Default::default()
        };
        let mut runnable = SplitCheckRunner::new(
            engine.clone(),
            tx.clone(),
            CoprocessorHost::new(tx, cfg),
            None,
        );

        for i in 0..11 {
            let k = format!("{:04}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }
        let start_key = Key::from_raw(b"0000").into_encoded();
        let end_key = Key::from_raw(b"0005").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region.clone(),
            Some(start_key),
            Some(end_key),
            SplitReason::Admin,
            CheckPolicy::Scan,
            None,
        ));
        let split_key = Key::from_raw(b"0003");
        must_split_at(&rx, &region, vec![split_key.into_encoded()]);
        let start_key = Key::from_raw(b"0005").into_encoded();
        let end_key = Key::from_raw(b"0010").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region.clone(),
            Some(start_key),
            Some(end_key),
            SplitReason::Admin,
            CheckPolicy::Scan,
            None,
        ));
        let split_key = Key::from_raw(b"0008");
        must_split_at(&rx, &region, vec![split_key.into_encoded()]);
        let start_key = Key::from_raw(b"0003").into_encoded();
        let end_key = Key::from_raw(b"0008").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region.clone(),
            Some(start_key),
            Some(end_key),
            SplitReason::Admin,
            CheckPolicy::Scan,
            None,
        ));
        let split_key = Key::from_raw(b"0006");
        must_split_at(&rx, &region, vec![split_key.into_encoded()]);
    }

    #[test]
    fn test_load_split_with_empty_key_range_no_split() {
        let path = Builder::new()
            .prefix("test-load-split-empty-range")
            .tempdir()
            .unwrap();
        let engine = engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            // Keep bucket size large enough so tiny ranges won't form >= 2 buckets.
            region_max_size: Some(ReadableSize::mb(256)),
            ..Default::default()
        };
        let mut host = CoprocessorHost::new(tx.clone(), cfg);
        host.registry
            .register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        host.registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(tx.clone())),
        );
        host.registry.register_split_check_observer(
            300,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(tx.clone())),
        );

        let mut runnable = SplitCheckRunner::new(engine, tx, host, None);

        let start_key = Key::from_raw(b"0000").into_encoded();
        let end_key = Key::from_raw(b"0001").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region,
            Some(start_key),
            Some(end_key),
            SplitReason::Load,
            CheckPolicy::Scan,
            None,
        ));

        assert_no_ask_split(&rx);
    }

    #[test]
    fn test_load_split_with_single_key_bucket_no_split() {
        let path = Builder::new()
            .prefix("test-load-split-single-key")
            .tempdir()
            .unwrap();
        let engine = engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            // Keep bucket size large enough so one key only forms one bucket.
            region_max_size: Some(ReadableSize::mb(256)),
            ..Default::default()
        };
        let mut host = CoprocessorHost::new(tx.clone(), cfg);
        host.registry
            .register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        host.registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(tx.clone())),
        );
        host.registry.register_split_check_observer(
            300,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(tx.clone())),
        );

        let mut runnable = SplitCheckRunner::new(engine.clone(), tx, host, None);

        let key = keys::data_key(Key::from_raw(b"0005").as_encoded());
        engine.put_cf(CF_DEFAULT, &key, &key).unwrap();
        engine.flush_cf(CF_DEFAULT, true).unwrap();

        let start_key = Key::from_raw(b"0005").into_encoded();
        let end_key = Key::from_raw(b"0006").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region,
            Some(start_key),
            Some(end_key),
            SplitReason::Load,
            CheckPolicy::Scan,
            None,
        ));

        assert_no_ask_split(&rx);
    }

    #[test]
    /// Regression test for boundary-after-strip behavior in load fallback.
    fn test_load_split_with_mvcc_boundary_key_no_split() {
        let path = Builder::new()
            .prefix("test-load-split-mvcc-boundary")
            .tempdir()
            .unwrap();
        let engine = engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);
        // Make range start equal to region start. If fallback picks mvcc key
        // and strips ts later, it would become a rejected boundary split key.
        region.set_start_key(Key::from_raw(b"0005").into_encoded());
        region.set_end_key(Key::from_raw(b"0010").into_encoded());

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            // Keep bucket size large enough so one key only forms one bucket.
            region_max_size: Some(ReadableSize::mb(256)),
            ..Default::default()
        };
        let mut host = CoprocessorHost::new(tx.clone(), cfg);
        host.registry
            .register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        host.registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(tx.clone())),
        );
        host.registry.register_split_check_observer(
            300,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(tx.clone())),
        );

        let mut runnable = SplitCheckRunner::new(engine.clone(), tx, host, None);

        // Only one user key exists in range, but with MVCC suffix.
        let key = keys::data_key(Key::from_raw(b"0005").append_ts(42.into()).as_encoded());
        engine.put_cf(CF_DEFAULT, &key, &key).unwrap();
        engine.flush_cf(CF_DEFAULT, true).unwrap();

        let start_key = Key::from_raw(b"0005").into_encoded();
        let end_key = Key::from_raw(b"0006").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region,
            Some(start_key),
            Some(end_key),
            SplitReason::Load,
            CheckPolicy::Scan,
            None,
        ));

        assert_no_ask_split(&rx);
    }

    #[test]
    fn test_load_split_with_single_bucket_uses_approximate_fallback() {
        let path = Builder::new()
            .prefix("test-load-split-approximate-fallback")
            .tempdir()
            .unwrap();
        let engine = engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            // Keep bucket size large enough so this range only forms one bucket.
            region_max_size: Some(ReadableSize::mb(256)),
            ..Default::default()
        };
        let mut host = CoprocessorHost::new(tx.clone(), cfg);
        host.registry
            .register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        host.registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(tx.clone())),
        );
        host.registry.register_split_check_observer(
            300,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(tx.clone())),
        );

        let mut runnable = SplitCheckRunner::new(engine.clone(), tx, host, None);

        for i in 0..21 {
            let k = format!("{:04}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &k).unwrap();
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }

        let start_key = Key::from_raw(b"0000").into_encoded();
        let end_key = Key::from_raw(b"0020").into_encoded();
        runnable.run(SplitCheckTask::split_check_key_range(
            region.clone(),
            Some(start_key.clone()),
            Some(end_key.clone()),
            SplitReason::Load,
            CheckPolicy::Scan,
            None,
        ));

        assert_ask_split_in_range(&rx, &region, &start_key, &end_key);
    }

    fn test_generate_region_bucket_impl(mvcc: bool) {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let engine = engine_test::kv::new_engine(path_str, ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_split_size: Some(ReadableSize(130_u64)),
            enable_region_bucket: Some(true),
            region_bucket_size: ReadableSize(20_u64), // so that each key below will form a bucket
            ..Default::default()
        };
        let cop_host = CoprocessorHost::new(tx.clone(), cfg);
        let mut runnable = SplitCheckRunner::new(engine.clone(), tx, cop_host.clone(), None);

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

        let host =
            cop_host.new_split_checker_host(&region, &engine, SplitReason::Size, CheckPolicy::Scan);
        assert_eq!(host.policy(), CheckPolicy::Scan);

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
        let host =
            cop_host.new_split_checker_host(&region, &engine, SplitReason::Size, CheckPolicy::Scan);
        assert_eq!(host.policy(), CheckPolicy::Scan);

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
        let engine = engine_test::kv::new_engine(path_str, ALL_CFS).unwrap();

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(5);

        let (tx, rx) = mpsc::sync_channel(100);
        let cfg = Config {
            region_split_size: Some(ReadableSize(130_u64)),
            enable_region_bucket: Some(true),
            region_bucket_size: ReadableSize(20_u64), // so that each key below will form a bucket
            ..Default::default()
        };
        let mut runnable = SplitCheckRunner::new(
            engine.clone(),
            tx.clone(),
            CoprocessorHost::new(tx, cfg),
            None,
        );

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
            if let Ok(SchedTask::RefreshRegionBuckets {
                buckets,
                bucket_ranges,
                ..
            }) = rx.try_recv()
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

        let db_opts = DbOptions::default();
        let mut cf_opts = CfOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let cfs_opts = LARGE_CFS.iter().map(|cf| (*cf, cf_opts.clone())).collect();
        let engine = engine_test::kv::new_engine_opt(path, db_opts, cfs_opts).unwrap();

        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat_n(b'v', 256));
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

    #[test]
    fn test_get_region_approximate_middle_in_range_cf() {
        let tmp = Builder::new()
            .prefix("test_raftstore_util_in_range")
            .tempdir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DbOptions::default();
        let mut cf_opts = CfOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let cfs_opts = LARGE_CFS.iter().map(|cf| (*cf, cf_opts.clone())).collect();
        let engine = engine_test::kv::new_engine_opt(path, db_opts, cfs_opts).unwrap();

        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat_n(b'v', 256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(CF_DEFAULT, &k, &big_value).unwrap();
            engine.flush_cf(CF_DEFAULT, true).unwrap();
        }

        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let start_key = keys::data_key(Key::from_raw(b"key_020").as_encoded());
        let end_key = keys::data_key(Key::from_raw(b"key_030").as_encoded());

        let middle_key = get_region_approximate_middle_in_range(
            &engine,
            &region,
            Some(&start_key),
            Some(&end_key),
        )
        .unwrap()
        .unwrap();

        let middle_key = Key::from_encoded_slice(keys::origin_key(&middle_key))
            .into_raw()
            .unwrap();
        assert!(middle_key.as_slice() >= &b"key_020"[..]);
        assert!(middle_key.as_slice() < &b"key_030"[..]);
    }
}
