// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::mem;
use std::path::Path;
use std::sync::{Arc, Mutex};

use engine_traits::LARGE_CFS;
use engine_traits::{KvEngine, Range, TableProperties, TablePropertiesCollection};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use error_code::ErrorCodeExt;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;

use crate::store::{CasualMessage, CasualRouter};

use super::super::error::Result;
use super::super::metrics::*;
use super::super::{Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker};
use super::Host;
use engine_rocks::RangeProperties;

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
            mem::replace(&mut self.split_keys, vec![])
        } else {
            vec![]
        }
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }

    fn approximate_split_keys(&mut self, region: &Region, engine: &E) -> Result<Vec<Vec<u8>>> {
        Ok(box_try!(get_approximate_split_keys(
            engine,
            region,
            self.batch_split_limit,
        )))
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
            &region,
            host.cfg.region_max_size.0 * host.cfg.batch_split_limit,
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
                    host.cfg.region_max_size.0,
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
        if region_size >= host.cfg.region_max_size.0 {
            info!(
                "approximate size over threshold, need to do split check";
                "region_id" => region.get_id(),
                "size" => region_size,
                "threshold" => host.cfg.region_max_size.0,
            );
            // when meet large region use approximate way to produce split keys
            if region_size >= host.cfg.region_max_size.0 * host.cfg.batch_split_limit {
                policy = CheckPolicy::Approximate
            }
            // Need to check size.
            host.add_checker(Box::new(Checker::new(
                host.cfg.region_max_size.0,
                host.cfg.region_split_size.0,
                host.cfg.batch_split_limit,
                policy,
            )));
        } else {
            // Does not need to check size.
            debug!(
                "approximate size less than threshold, does not need to do split check";
                "region_id" => region.get_id(),
                "size" => region_size,
                "threshold" => host.cfg.region_max_size.0,
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
    Ok(box_try!(get_range_approximate_size(
        db,
        range,
        large_threshold
    )))
}

fn get_range_approximate_size(
    db: &impl KvEngine,
    range: Range,
    large_threshold: u64,
) -> Result<u64> {
    let mut size = 0;
    for cfname in LARGE_CFS {
        size += get_range_approximate_size_cf(db, cfname, range, large_threshold)
            // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
            // backward compatibility.
            .or_else(|e| if cfname == &CF_LOCK { Ok(0) } else { Err(e) })?;
    }
    Ok(size)
}

fn get_range_approximate_size_cf(
    db: &impl KvEngine,
    cfname: &str,
    range: Range,
    large_threshold: u64,
) -> Result<u64> {
    let start_key = &range.start_key;
    let end_key = &range.end_key;
    let mut total_size = 0;
    let (_, mem_size) = box_try!(db.get_approximate_memtable_stats_cf(cfname, &range));
    total_size += mem_size;

    let collection = box_try!(db.get_range_properties_cf(cfname, &start_key, &end_key));
    for (_, v) in collection.iter() {
        let props = box_try!(RangeProperties::decode(&v.user_collected_properties()));
        total_size += props.get_approximate_size_in_range(&start_key, &end_key);
    }

    if large_threshold != 0 && total_size > large_threshold {
        let ssts = collection
            .iter()
            .map(|(k, v)| {
                let props = RangeProperties::decode(&v.user_collected_properties()).unwrap();
                let size = props.get_approximate_size_in_range(&start_key, &end_key);
                format!(
                    "{}:{}",
                    Path::new(&*k)
                        .file_name()
                        .map(|f| f.to_str().unwrap())
                        .unwrap_or(&*k),
                    size
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        info!(
            "range size is too large";
            "start" => log_wrappers::Value::key(&range.start_key),
            "end" => log_wrappers::Value::key(&range.end_key),
            "total_size" => total_size,
            "memtable" => mem_size,
            "ssts_size" => ssts,
            "cf" => cfname,
        )
    }
    Ok(total_size)
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
    Ok(box_try!(get_range_approximate_split_keys(
        db,
        range,
        batch_split_limit as usize
    )))
}

pub fn get_range_approximate_split_keys(
    db: &impl KvEngine,
    range: Range,
    key_count: usize,
) -> Result<Vec<Vec<u8>>> {
    let get_cf_size = |cf: &str| get_range_approximate_size_cf(db, cf, range, 0);
    let cfs = [
        (CF_DEFAULT, box_try!(get_cf_size(CF_DEFAULT))),
        (CF_WRITE, box_try!(get_cf_size(CF_WRITE))),
        // CF_LOCK doesn't have RangeProperties until v4.0, so we swallow the error for
        // backward compatibility.
        (CF_LOCK, get_cf_size(CF_LOCK).unwrap_or(0)),
    ];

    let total_size: u64 = cfs.iter().map(|(_, s)| s).sum();
    if total_size == 0 {
        return Err(box_err!("all CFs are empty"));
    }

    let (cf, _) = cfs.iter().max_by_key(|(_, s)| s).unwrap();

    get_range_approximate_split_keys_cf(db, cf, range, key_count)
}

fn get_range_approximate_split_keys_cf(
    db: &impl KvEngine,
    cfname: &str,
    range: Range,
    key_count: usize,
) -> Result<Vec<Vec<u8>>> {
    let start_key = &range.start_key;
    let end_key = &range.end_key;
    let collection = box_try!(db.get_range_properties_cf(cfname, &start_key, &end_key));

    let mut keys = vec![];
    for (_, v) in collection.iter() {
        let props = box_try!(RangeProperties::decode(&v.user_collected_properties()));
        keys.extend(
            props
                .take_excluded_range(start_key, end_key)
                .into_iter()
                .map(|(k, _)| k),
        );
    }

    if keys.is_empty() {
        return Ok(vec![]);
    }

    const SAMPLING_THRESHOLD: usize = 20000;
    const SAMPLE_RATIO: usize = 1000;
    // If there are too many keys, reduce its amount before sorting, or it may take too much
    // time to sort the keys.
    if keys.len() > SAMPLING_THRESHOLD {
        let len = keys.len();
        keys = keys.into_iter().step_by(len / SAMPLE_RATIO).collect();
    }
    keys.sort();

    // If the keys are too few, return them directly.
    if keys.len() <= key_count {
        return Ok(keys);
    }

    // Find `key_count` keys which divides the whole range into `parts` parts evenly.
    let mut res = Vec::with_capacity(key_count);
    let section_len = (keys.len() as f64) / ((key_count + 1) as f64);
    for i in 1..=key_count {
        res.push(keys[(section_len * (i as f64)) as usize].clone())
    }
    res.dedup();
    Ok(res)
}

#[cfg(test)]
pub mod tests {
    use super::Checker;
    use crate::coprocessor::properties::RangePropertiesCollectorFactory;
    use crate::coprocessor::{Config, CoprocessorHost, ObserverContext, SplitChecker};
    use crate::store::{CasualMessage, KeyEntry, SplitCheckRunner, SplitCheckTask};
    use engine::rocks;
    use engine::rocks::util::{new_engine_opt, CFOptions};
    use engine::rocks::{ColumnFamilyOptions, DBOptions, Writable};
    use engine_rocks::{Compat, RocksEngine};
    use engine_traits::{CfName, ALL_CFS, CF_DEFAULT, CF_WRITE, LARGE_CFS};
    use kvproto::metapb::Peer;
    use kvproto::metapb::Region;
    use kvproto::pdpb::CheckPolicy;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::{
        iter::{self, FromIterator},
        u64,
    };
    use tempfile::Builder;
    use tikv_util::collections::HashSet;
    use tikv_util::config::ReadableSize;
    use tikv_util::worker::Runnable;
    use txn_types::Key;

    use super::*;

    fn must_split_at_impl(
        rx: &mpsc::Receiver<(u64, CasualMessage<RocksEngine>)>,
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
                others => panic!("expect split check result, but got {:?}", others),
            }
        }
    }

    pub fn must_split_at(
        rx: &mpsc::Receiver<(u64, CasualMessage<RocksEngine>)>,
        exp_region: &Region,
        exp_split_keys: Vec<Vec<u8>>,
    ) {
        must_split_at_impl(rx, exp_region, exp_split_keys, false)
    }

    fn test_split_check_impl(cfs_with_range_prop: &[CfName], data_cf: CfName) {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let cfs_with_range_prop = HashSet::from_iter(cfs_with_range_prop.iter().cloned());
        let mut cf_opt = ColumnFamilyOptions::new();
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opt.add_table_properties_collector_factory("tikv.range-collector", f);

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                if cfs_with_range_prop.contains(cf) {
                    CFOptions::new(cf, cf_opt.clone())
                } else {
                    CFOptions::new(cf, ColumnFamilyOptions::new())
                }
            })
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
        cfg.region_max_size = ReadableSize(100);
        cfg.region_split_size = ReadableSize(60);
        cfg.batch_split_limit = 5;

        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        let cf_handle = engine.cf_handle(data_cf).unwrap();
        // so split key will be [z0006]
        for i in 0..7 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(&cf_handle, &s, &s).unwrap();
        }

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
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
            engine.put_cf(&cf_handle, &s, &s).unwrap();
        }

        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        engine.flush_cf(&cf_handle, true).unwrap();

        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(&rx, &region, vec![b"0006".to_vec()]);

        // so split keys will be [z0006, z0012]
        for i in 11..19 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(&cf_handle, &s, &s).unwrap();
        }
        engine.flush_cf(&cf_handle, true).unwrap();
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
        ));
        must_split_at(&rx, &region, vec![b"0006".to_vec(), b"0012".to_vec()]);

        // for test batch_split_limit
        // so split kets will be [z0006, z0012, z0018, z0024, z0030]
        for i in 19..41 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(&cf_handle, &s, &s).unwrap();
        }
        engine.flush_cf(&cf_handle, true).unwrap();
        runnable.run(SplitCheckTask::split_check(
            region.clone(),
            true,
            CheckPolicy::Scan,
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
        runnable.run(SplitCheckTask::split_check(region, true, CheckPolicy::Scan));
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
    fn test_cf_lock_without_range_prop() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opt = ColumnFamilyOptions::new();
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opt.add_table_properties_collector_factory("tikv.range-collector", f);

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                if cf != &CF_LOCK {
                    CFOptions::new(cf, cf_opt.clone())
                } else {
                    CFOptions::new(cf, ColumnFamilyOptions::new())
                }
            })
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
        cfg.region_max_size = ReadableSize(100);
        cfg.region_split_size = ReadableSize(60);
        cfg.batch_split_limit = 5;

        let mut runnable = SplitCheckRunner::new(
            Arc::clone(&engine),
            tx.clone(),
            CoprocessorHost::new(tx.clone(), cfg.clone()),
        );

        for cf in LARGE_CFS {
            let cf_handle = engine.cf_handle(cf).unwrap();
            for i in 0..7 {
                let s = keys::data_key(format!("{:04}", i).as_bytes());
                engine.put_cf(&cf_handle, &s, &s).unwrap();
            }
            engine.flush_cf(&cf_handle, true).unwrap();
        }

        for policy in &[CheckPolicy::Scan, CheckPolicy::Approximate] {
            runnable.run(SplitCheckTask::split_check(region.clone(), true, *policy));
            // Ignore the split keys. Only check whether it can split or not.
            must_split_at_impl(&rx, &region, vec![], true);
        }

        drop(engine);
        drop(runnable);

        // Reopen the engine and all cfs have range properties.
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let engine = Arc::new(new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap());

        let mut runnable =
            SplitCheckRunner::new(engine.clone(), tx.clone(), CoprocessorHost::new(tx, cfg));

        // Flush a sst of CF_LOCK with range properties.
        let cf_handle = engine.cf_handle(CF_LOCK).unwrap();
        for i in 7..15 {
            let s = keys::data_key(format!("{:04}", i).as_bytes());
            engine.put_cf(&cf_handle, &s, &s).unwrap();
        }
        engine.flush_cf(&cf_handle, true).unwrap();
        for policy in &[CheckPolicy::Scan, CheckPolicy::Approximate] {
            runnable.run(SplitCheckTask::split_check(region.clone(), true, *policy));
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
            if SplitChecker::<RocksEngine>::on_kv(&mut checker, &mut ctx, &data) {
                break;
            }
        }

        assert!(!SplitChecker::<RocksEngine>::split_keys(&mut checker).is_empty());
    }

    #[test]
    fn test_checker_with_max_twice_bigger_than_split_size() {
        let mut checker = Checker::new(20, 10, 1, CheckPolicy::Scan);
        let region = Region::default();
        let mut ctx = ObserverContext::new(&region);
        for _ in 0..2 {
            let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 5, CF_WRITE);
            if SplitChecker::<RocksEngine>::on_kv(&mut checker, &mut ctx, &data) {
                break;
            }
        }

        assert!(!SplitChecker::<RocksEngine>::split_keys(&mut checker).is_empty());
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

        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);

        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = Arc::new(rocks::util::new_engine_opt(path, db_opts, cfs_opts).unwrap());

        let region = make_region(1, vec![], vec![]);
        assert_eq!(
            get_approximate_split_keys(engine.c(), &region, 1).is_err(),
            true
        );

        let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));
        for i in 0..100 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            engine.flush_cf(cf_handle, true).unwrap();
        }
        assert_eq!(
            get_approximate_split_keys(engine.c(), &region, 1).is_err(),
            true
        );
    }

    fn test_get_approximate_split_keys_impl(data_cf: CfName) {
        let tmp = Builder::new()
            .prefix("test_raftstore_util")
            .tempdir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();

        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let engine = Arc::new(rocks::util::new_engine_opt(path, db_opts, cfs_opts).unwrap());

        let cf_handle = engine.cf_handle(data_cf).unwrap();
        let mut big_value = Vec::with_capacity(256);
        big_value.extend(iter::repeat(b'v').take(256));

        for i in 0..4 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let region = make_region(1, vec![], vec![]);
        let split_keys = get_approximate_split_keys(engine.c(), &region, 0)
            .unwrap()
            .into_iter()
            .map(|k| {
                Key::from_encoded_slice(keys::origin_key(&k))
                    .into_raw()
                    .unwrap()
            })
            .collect::<Vec<Vec<u8>>>();

        assert_eq!(split_keys.is_empty(), true);
        for i in 4..5 {
            let k = format!("key_{:03}", i).into_bytes();
            let k = keys::data_key(Key::from_raw(&k).as_encoded());
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let split_keys = get_approximate_split_keys(engine.c(), &region, 1)
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
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let split_keys = get_approximate_split_keys(engine.c(), &region, 2)
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
            engine.put_cf(cf_handle, &k, &big_value).unwrap();
            // Flush for every key so that we can know the exact middle key.
            engine.flush_cf(cf_handle, true).unwrap();
        }
        let split_keys = get_approximate_split_keys(engine.c(), &region, 5)
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
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = Arc::new(rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        let cf_size = 2 + 1024 + 2 + 2048 + 2 + 4096;
        for &(key, vlen) in &cases {
            for cfname in LARGE_CFS {
                let k1 = keys::data_key(key.as_bytes());
                let v1 = vec![0; vlen as usize];
                assert_eq!(k1.len(), 2);
                let cf = db.cf_handle(cfname).unwrap();
                db.put_cf(cf, &k1, &v1).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
        }

        let region = make_region(1, vec![], vec![]);
        let size = get_region_approximate_size(db.c(), &region, 0).unwrap();
        assert_eq!(size, cf_size * LARGE_CFS.len() as u64);
    }

    #[test]
    fn test_region_maybe_inaccurate_approximate_size() {
        let path = Builder::new()
            .prefix("_test_raftstore_region_maybe_inaccurate_approximate_size")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = Arc::new(rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let mut cf_size = 0;
        for i in 0..100 {
            let k1 = keys::data_key(format!("k1{}", i).as_bytes());
            let k2 = keys::data_key(format!("k9{}", i).as_bytes());
            let v = vec![0; 4096];
            cf_size += k1.len() + k2.len() + v.len() * 2;
            let cf = db.cf_handle("default").unwrap();
            db.put_cf(cf, &k1, &v).unwrap();
            db.put_cf(cf, &k2, &v).unwrap();
            db.flush_cf(cf, true).unwrap();
        }

        let region = make_region(1, vec![], vec![]);
        let size = get_region_approximate_size(db.c(), &region, 0).unwrap();
        assert_eq!(size, cf_size as u64);

        let region = make_region(1, b"k2".to_vec(), b"k8".to_vec());
        let size = get_region_approximate_size(db.c(), &region, 0).unwrap();
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
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let f = Box::new(RangePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = Arc::new(rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

        let mut cf_size = 0;
        let cf = db.cf_handle("default").unwrap();
        for i in 0..10 {
            let v = vec![0; 4096];
            for j in 10000 * i..10000 * (i + 1) {
                let k1 = keys::data_key(format!("k1{:0100}", j).as_bytes());
                let k2 = keys::data_key(format!("k9{:0100}", j).as_bytes());
                cf_size += k1.len() + k2.len() + v.len() * 2;
                db.put_cf(cf, &k1, &v).unwrap();
                db.put_cf(cf, &k2, &v).unwrap();
            }
            db.flush_cf(cf, true).unwrap();
        }

        let region = make_region(1, vec![], vec![]);
        b.iter(|| {
            let size = get_region_approximate_size(db.c(), &region, 0).unwrap();
            assert_eq!(size, cf_size as u64);
        })
    }
}
