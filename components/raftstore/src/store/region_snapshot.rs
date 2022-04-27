// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use engine_traits::{
    IterOptions, KvEngine, Peekable, ReadOptions, Result as EngineResult, Snapshot,
};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftApplyState;
use pd_client::BucketMeta;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::store::{util, PeerStorage, TxnExt};
use crate::{Error, Result};
use engine_traits::util::check_key_in_range;
use engine_traits::RaftEngine;
use engine_traits::CF_RAFT;
use engine_traits::{Error as EngineError, Iterable, Iterator};
use fail::fail_point;
use keys::DATA_PREFIX_KEY;
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::{box_err, error};
use tikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
#[derive(Debug)]
pub struct RegionSnapshot<S: Snapshot> {
    snap: Arc<S>,
    region: Arc<Region>,
    apply_index: Arc<AtomicU64>,
    pub term: Option<NonZeroU64>,
    pub txn_extra_op: TxnExtraOp,
    // `None` means the snapshot does not provide peer related transaction extensions.
    pub txn_ext: Option<Arc<TxnExt>>,
    pub bucket_meta: Option<Arc<BucketMeta>>,
}

impl<S> RegionSnapshot<S>
where
    S: Snapshot,
{
    #[allow(clippy::new_ret_no_self)] // temporary until this returns RegionSnapshot<E>
    pub fn new<EK>(ps: &PeerStorage<EK, impl RaftEngine>) -> RegionSnapshot<EK::Snapshot>
    where
        EK: KvEngine,
    {
        RegionSnapshot::from_snapshot(Arc::new(ps.raw_snapshot()), Arc::new(ps.region().clone()))
    }

    pub fn from_raw<EK>(db: EK, region: Region) -> RegionSnapshot<EK::Snapshot>
    where
        EK: KvEngine,
    {
        RegionSnapshot::from_snapshot(Arc::new(db.snapshot()), Arc::new(region))
    }

    pub fn from_snapshot(snap: Arc<S>, region: Arc<Region>) -> RegionSnapshot<S> {
        RegionSnapshot {
            snap,
            region,
            // Use 0 to indicate that the apply index is missing and we need to KvGet it,
            // since apply index must be >= RAFT_INIT_LOG_INDEX.
            apply_index: Arc::new(AtomicU64::new(0)),
            term: None,
            txn_extra_op: TxnExtraOp::Noop,
            txn_ext: None,
            bucket_meta: None,
        }
    }

    #[inline]
    pub fn get_region(&self) -> &Region {
        &self.region
    }

    #[inline]
    pub fn get_snapshot(&self) -> &S {
        self.snap.as_ref()
    }

    #[inline]
    pub fn get_apply_index(&self) -> Result<u64> {
        let apply_index = self.apply_index.load(Ordering::SeqCst);
        if apply_index == 0 {
            self.get_apply_index_from_storage()
        } else {
            Ok(apply_index)
        }
    }

    fn get_apply_index_from_storage(&self) -> Result<u64> {
        let apply_state: Option<RaftApplyState> = self
            .snap
            .get_msg_cf(CF_RAFT, &keys::apply_state_key(self.region.get_id()))?;
        match apply_state {
            Some(s) => {
                let apply_index = s.get_applied_index();
                self.apply_index.store(apply_index, Ordering::SeqCst);
                Ok(apply_index)
            }
            None => Err(box_err!("Unable to get applied index")),
        }
    }

    pub fn iter(&self, iter_opt: IterOptions) -> RegionIterator<S> {
        RegionIterator::new(&self.snap, Arc::clone(&self.region), iter_opt)
    }

    pub fn iter_cf(&self, cf: &str, iter_opt: IterOptions) -> Result<RegionIterator<S>> {
        Ok(RegionIterator::new_cf(
            &self.snap,
            Arc::clone(&self.region),
            iter_opt,
            cf,
        ))
    }

    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_PREFIX_KEY.len(), 0);
        let end = KeyBuilder::from_slice(end_key, DATA_PREFIX_KEY.len(), 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);
        self.scan_impl(self.iter(iter_opt), start_key, f)
    }

    // like `scan`, only on a specific column family.
    pub fn scan_cf<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_PREFIX_KEY.len(), 0);
        let end = KeyBuilder::from_slice(end_key, DATA_PREFIX_KEY.len(), 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);
        self.scan_impl(self.iter_cf(cf, iter_opt)?, start_key, f)
    }

    fn scan_impl<F>(&self, mut it: RegionIterator<S>, start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut it_valid = it.seek(start_key)?;
        while it_valid {
            it_valid = f(it.key(), it.value())? && it.next()?;
        }
        Ok(())
    }

    #[inline]
    pub fn get_start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    #[inline]
    pub fn get_end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }
}

impl<S> Clone for RegionSnapshot<S>
where
    S: Snapshot,
{
    fn clone(&self) -> Self {
        RegionSnapshot {
            snap: self.snap.clone(),
            region: Arc::clone(&self.region),
            apply_index: Arc::clone(&self.apply_index),
            term: self.term,
            txn_extra_op: self.txn_extra_op,
            txn_ext: self.txn_ext.clone(),
            bucket_meta: self.bucket_meta.clone(),
        }
    }
}

impl<S> Peekable for RegionSnapshot<S>
where
    S: Snapshot,
{
    type DBVector = <S as Peekable>::DBVector;

    fn get_value_opt(
        &self,
        opts: &ReadOptions,
        key: &[u8],
    ) -> EngineResult<Option<Self::DBVector>> {
        check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = keys::data_key(key);
        self.snap
            .get_value_opt(opts, &data_key)
            .map_err(|e| self.handle_get_value_error(e, "", key))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> EngineResult<Option<Self::DBVector>> {
        check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = keys::data_key(key);
        self.snap
            .get_value_cf_opt(opts, cf, &data_key)
            .map_err(|e| self.handle_get_value_error(e, cf, key))
    }
}

impl<S> RegionSnapshot<S>
where
    S: Snapshot,
{
    #[inline(never)]
    fn handle_get_value_error(&self, e: EngineError, cf: &str, key: &[u8]) -> EngineError {
        CRITICAL_ERROR.with_label_values(&["rocksdb get"]).inc();
        if panic_when_unexpected_key_or_data() {
            set_panic_mark();
            panic!(
                "failed to get value of key {} in region {}: {:?}",
                log_wrappers::Value::key(key),
                self.region.get_id(),
                e,
            );
        } else {
            error!(
                "failed to get value of key in cf";
                "key" => log_wrappers::Value::key(key),
                "region" => self.region.get_id(),
                "cf" => cf,
                "error" => ?e,
            );
            e
        }
    }
}

/// `RegionIterator` wrap a rocksdb iterator and only allow it to
/// iterate in the region. It behaves as if underlying
/// db only contains one region.
pub struct RegionIterator<S: Snapshot> {
    iter: <S as Iterable>::Iterator,
    region: Arc<Region>,
}

fn update_lower_bound(iter_opt: &mut IterOptions, region: &Region) {
    let region_start_key = keys::enc_start_key(region);
    if iter_opt.lower_bound().is_some() && !iter_opt.lower_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_lower_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_start_key.as_slice() > *iter_opt.lower_bound().as_ref().unwrap() {
            iter_opt.set_vec_lower_bound(region_start_key);
        }
    } else {
        iter_opt.set_vec_lower_bound(region_start_key);
    }
}

fn update_upper_bound(iter_opt: &mut IterOptions, region: &Region) {
    let region_end_key = keys::enc_end_key(region);
    if iter_opt.upper_bound().is_some() && !iter_opt.upper_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_upper_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_end_key.as_slice() < *iter_opt.upper_bound().as_ref().unwrap() {
            iter_opt.set_vec_upper_bound(region_end_key, 0);
        }
    } else {
        iter_opt.set_vec_upper_bound(region_end_key, 0);
    }
}

// we use engine::rocks's style iterator, doesn't need to impl std iterator.
impl<S> RegionIterator<S>
where
    S: Snapshot,
{
    pub fn new(snap: &S, region: Arc<Region>, mut iter_opt: IterOptions) -> RegionIterator<S> {
        update_lower_bound(&mut iter_opt, &region);
        update_upper_bound(&mut iter_opt, &region);
        let iter = snap
            .iterator_opt(iter_opt)
            .expect("creating snapshot iterator"); // FIXME error handling
        RegionIterator { iter, region }
    }

    pub fn new_cf(
        snap: &S,
        region: Arc<Region>,
        mut iter_opt: IterOptions,
        cf: &str,
    ) -> RegionIterator<S> {
        update_lower_bound(&mut iter_opt, &region);
        update_upper_bound(&mut iter_opt, &region);
        let iter = snap
            .iterator_cf_opt(cf, iter_opt)
            .expect("creating snapshot iterator"); // FIXME error handling
        RegionIterator { iter, region }
    }

    pub fn seek_to_first(&mut self) -> Result<bool> {
        self.iter.seek_to_first().map_err(Error::from)
    }

    pub fn seek_to_last(&mut self) -> Result<bool> {
        self.iter.seek_to_last().map_err(Error::from)
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        fail_point!("region_snapshot_seek", |_| {
            Err(box_err!("region seek error"))
        });
        self.should_seekable(key)?;
        let key = keys::data_key(key);
        self.iter.seek(key.as_slice().into()).map_err(Error::from)
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.should_seekable(key)?;
        let key = keys::data_key(key);
        self.iter
            .seek_for_prev(key.as_slice().into())
            .map_err(Error::from)
    }

    pub fn prev(&mut self) -> Result<bool> {
        self.iter.prev().map_err(Error::from)
    }

    pub fn next(&mut self) -> Result<bool> {
        self.iter.next().map_err(Error::from)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        keys::origin_key(self.iter.key())
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        self.iter.value()
    }

    #[inline]
    pub fn valid(&self) -> Result<bool> {
        self.iter.valid().map_err(Error::from)
    }

    #[inline]
    pub fn should_seekable(&self, key: &[u8]) -> Result<()> {
        if let Err(e) = util::check_key_in_region_inclusive(key, &self.region) {
            return handle_check_key_in_region_error(e);
        }
        Ok(())
    }
}

#[inline(never)]
fn handle_check_key_in_region_error(e: crate::Error) -> Result<()> {
    // Split out the error case to reduce hot-path code size.
    CRITICAL_ERROR
        .with_label_values(&["key not in region"])
        .inc();
    if panic_when_unexpected_key_or_data() {
        set_panic_mark();
        panic!("key exceed bound: {:?}", e);
    } else {
        Err(e)
    }
}

#[cfg(test)]
mod tests {
    use crate::store::PeerStorage;
    use crate::Result;

    use engine_test::kv::KvTestSnapshot;
    use engine_test::new_temp_engine;
    use engine_traits::{Engines, KvEngine, Peekable, RaftEngine, SyncMutable};
    use keys::data_key;
    use kvproto::metapb::{Peer, Region};
    use tempfile::Builder;
    use tikv_util::worker;

    use super::*;

    type DataSet = Vec<(Vec<u8>, Vec<u8>)>;

    fn new_peer_storage<EK, ER>(engines: Engines<EK, ER>, r: &Region) -> PeerStorage<EK, ER>
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let (region_sched, _) = worker::dummy_scheduler();
        let (raftlog_fetch_sched, _) = worker::dummy_scheduler();
        PeerStorage::new(
            engines,
            r,
            region_sched,
            raftlog_fetch_sched,
            0,
            "".to_owned(),
        )
        .unwrap()
    }

    fn load_default_dataset<EK, ER>(engines: Engines<EK, ER>) -> (PeerStorage<EK, ER>, DataSet)
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let mut r = Region::default();
        r.mut_peers().push(Peer::default());
        r.set_id(10);
        r.set_start_key(b"a2".to_vec());
        r.set_end_key(b"a7".to_vec());

        let base_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
            (b"a9".to_vec(), b"v9".to_vec()),
        ];

        for &(ref k, ref v) in &base_data {
            engines.kv.put(&data_key(k), v).unwrap();
        }
        let store = new_peer_storage(engines, &r);
        (store, base_data)
    }

    fn load_multiple_levels_dataset<EK, ER>(
        engines: Engines<EK, ER>,
    ) -> (PeerStorage<EK, ER>, DataSet)
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let mut r = Region::default();
        r.mut_peers().push(Peer::default());
        r.set_id(10);
        r.set_start_key(b"a04".to_vec());
        r.set_end_key(b"a15".to_vec());

        let levels = vec![
            (b"a01".to_vec(), 1),
            (b"a02".to_vec(), 5),
            (b"a03".to_vec(), 3),
            (b"a04".to_vec(), 4),
            (b"a05".to_vec(), 1),
            (b"a06".to_vec(), 2),
            (b"a07".to_vec(), 2),
            (b"a08".to_vec(), 5),
            (b"a09".to_vec(), 6),
            (b"a10".to_vec(), 0),
            (b"a11".to_vec(), 1),
            (b"a12".to_vec(), 4),
            (b"a13".to_vec(), 2),
            (b"a14".to_vec(), 5),
            (b"a15".to_vec(), 3),
            (b"a16".to_vec(), 2),
            (b"a17".to_vec(), 1),
            (b"a18".to_vec(), 0),
        ];

        let mut data = vec![];
        {
            let db = &engines.kv;
            for &(ref k, level) in &levels {
                db.put(&data_key(k), k).unwrap();
                db.flush(true).unwrap();
                data.push((k.to_vec(), k.to_vec()));
                db.compact_files_in_range(Some(&data_key(k)), Some(&data_key(k)), Some(level))
                    .unwrap();
            }
        }

        let store = new_peer_storage(engines, &r);
        (store, data)
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let mut r = Region::default();
        r.set_id(10);
        r.set_start_key(b"key0".to_vec());
        r.set_end_key(b"key4".to_vec());
        let store = new_peer_storage(engines.clone(), &r);

        let key3 = b"key3";
        engines.kv.put_msg(&data_key(key3), &r).expect("");

        let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);
        let v3 = snap.get_msg(key3).expect("");
        assert_eq!(v3, Some(r));

        let v0 = snap.get_value(b"key0").expect("");
        assert!(v0.is_none());

        let v4 = snap.get_value(b"key5");
        assert!(v4.is_err());
    }

    #[allow(clippy::type_complexity)]
    #[test]
    fn test_seek_and_seek_prev() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, _) = load_default_dataset(engines);
        let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);

        let check_seek_result = |snap: &RegionSnapshot<KvTestSnapshot>,
                                 lower_bound: Option<&[u8]>,
                                 upper_bound: Option<&[u8]>,
                                 seek_table: &Vec<(
            &[u8],
            bool,
            Option<(&[u8], &[u8])>,
            Option<(&[u8], &[u8])>,
        )>| {
            let iter_opt = IterOptions::new(
                lower_bound.map(|v| KeyBuilder::from_slice(v, keys::DATA_PREFIX_KEY.len(), 0)),
                upper_bound.map(|v| KeyBuilder::from_slice(v, keys::DATA_PREFIX_KEY.len(), 0)),
                true,
            );
            let mut iter = snap.iter(iter_opt);
            for (seek_key, in_range, seek_exp, prev_exp) in seek_table.clone() {
                let check_res = |iter: &RegionIterator<KvTestSnapshot>,
                                 res: Result<bool>,
                                 exp: Option<(&[u8], &[u8])>| {
                    if !in_range {
                        assert!(
                            res.is_err(),
                            "exp failed at {}",
                            log_wrappers::Value::key(seek_key)
                        );
                        return;
                    }
                    if exp.is_none() {
                        assert!(
                            !res.unwrap(),
                            "exp none at {}",
                            log_wrappers::Value::key(seek_key)
                        );
                        return;
                    }

                    assert!(
                        res.unwrap(),
                        "should succeed at {}",
                        log_wrappers::Value::key(seek_key)
                    );
                    let (exp_key, exp_val) = exp.unwrap();
                    assert_eq!(iter.key(), exp_key);
                    assert_eq!(iter.value(), exp_val);
                };
                let seek_res = iter.seek(seek_key);
                check_res(&iter, seek_res, seek_exp);
                let prev_res = iter.seek_for_prev(seek_key);
                check_res(&iter, prev_res, prev_exp);
            }
        };

        let mut seek_table: Vec<(&[u8], bool, Option<(&[u8], &[u8])>, Option<(&[u8], &[u8])>)> = vec![
            (b"a1", false, None, None),
            (b"a2", true, Some((b"a3", b"v3")), None),
            (b"a3", true, Some((b"a3", b"v3")), Some((b"a3", b"v3"))),
            (b"a4", true, Some((b"a5", b"v5")), Some((b"a3", b"v3"))),
            (b"a6", true, None, Some((b"a5", b"v5"))),
            (b"a7", true, None, Some((b"a5", b"v5"))),
            (b"a9", false, None, None),
        ];
        check_seek_result(&snap, None, None, &seek_table);
        check_seek_result(&snap, None, Some(b"a9"), &seek_table);
        check_seek_result(&snap, Some(b"a1"), None, &seek_table);
        check_seek_result(&snap, Some(b""), Some(b""), &seek_table);
        check_seek_result(&snap, Some(b"a1"), Some(b"a9"), &seek_table);
        check_seek_result(&snap, Some(b"a2"), Some(b"a9"), &seek_table);
        check_seek_result(&snap, Some(b"a2"), Some(b"a7"), &seek_table);
        check_seek_result(&snap, Some(b"a1"), Some(b"a7"), &seek_table);

        seek_table = vec![
            (b"a1", false, None, None),
            (b"a2", true, None, None),
            (b"a3", true, None, None),
            (b"a4", true, None, None),
            (b"a6", true, None, None),
            (b"a7", true, None, None),
            (b"a9", false, None, None),
        ];
        check_seek_result(&snap, None, Some(b"a1"), &seek_table);
        check_seek_result(&snap, Some(b"a8"), None, &seek_table);
        check_seek_result(&snap, Some(b"a7"), Some(b"a2"), &seek_table);

        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, _) = load_multiple_levels_dataset(engines);
        let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);

        seek_table = vec![
            (b"a01", false, None, None),
            (b"a03", false, None, None),
            (b"a05", true, Some((b"a05", b"a05")), Some((b"a05", b"a05"))),
            (b"a10", true, Some((b"a10", b"a10")), Some((b"a10", b"a10"))),
            (b"a14", true, Some((b"a14", b"a14")), Some((b"a14", b"a14"))),
            (b"a15", true, None, Some((b"a14", b"a14"))),
            (b"a18", false, None, None),
            (b"a19", false, None, None),
        ];
        check_seek_result(&snap, None, None, &seek_table);
        check_seek_result(&snap, None, Some(b"a20"), &seek_table);
        check_seek_result(&snap, Some(b"a00"), None, &seek_table);
        check_seek_result(&snap, Some(b""), Some(b""), &seek_table);
        check_seek_result(&snap, Some(b"a00"), Some(b"a20"), &seek_table);
        check_seek_result(&snap, Some(b"a01"), Some(b"a20"), &seek_table);
        check_seek_result(&snap, Some(b"a01"), Some(b"a15"), &seek_table);
        check_seek_result(&snap, Some(b"a00"), Some(b"a15"), &seek_table);
    }

    #[test]
    fn test_iterate() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, base_data) = load_default_dataset(engines.clone());

        let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);
        let mut data = vec![];
        snap.scan(b"a2", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(data, &base_data[1..3]);

        data.clear();
        snap.scan(b"a2", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(false)
        })
        .unwrap();

        assert_eq!(data.len(), 1);

        let mut iter = snap.iter(IterOptions::default());
        assert!(iter.seek_to_first().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next().unwrap() {
                break;
            }
        }
        assert_eq!(res, base_data[1..3].to_vec());

        // test last region
        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let store = new_peer_storage(engines.clone(), &region);
        let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);
        data.clear();
        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 5);
        assert_eq!(data, base_data);

        let mut iter = snap.iter(IterOptions::default());
        assert!(iter.seek(b"a1").unwrap());

        assert!(iter.seek_to_first().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next().unwrap() {
                break;
            }
        }
        assert_eq!(res, base_data);

        // test iterator with upper bound
        let store = new_peer_storage(engines, &region);
        let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);
        let mut iter = snap.iter(IterOptions::new(
            None,
            Some(KeyBuilder::from_slice(b"a5", DATA_PREFIX_KEY.len(), 0)),
            true,
        ));
        assert!(iter.seek_to_first().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next().unwrap() {
                break;
            }
        }
        assert_eq!(res, base_data[0..2].to_vec());
    }

    #[test]
    fn test_reverse_iterate_with_lower_bound() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, test_data) = load_default_dataset(engines);

        let snap = RegionSnapshot::<KvTestSnapshot>::new(&store);
        let mut iter_opt = IterOptions::default();
        iter_opt.set_lower_bound(b"a3", 1);
        let mut iter = snap.iter(iter_opt);
        assert!(iter.seek_to_last().unwrap());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.prev().unwrap() {
                break;
            }
        }
        res.sort();
        assert_eq!(res, test_data[1..3].to_vec());
    }
}
