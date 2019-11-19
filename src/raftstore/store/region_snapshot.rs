// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::{TablePropertiesCollection, DB};
use engine::{
    self, IterOption,
};
use engine_rocks::{RocksEngineIterator, RocksDBVector, RocksSnapshot, RocksSyncSnapshot};
use engine_traits::{SeekKey, Peekable, Result as EngineResult, ReadOptions, Snapshot as SnapshotTrait};
use kvproto::metapb::Region;
use std::sync::Arc;

use crate::raftstore::store::keys::DATA_PREFIX_KEY;
use crate::raftstore::store::{keys, util, PeerStorage};
use crate::raftstore::Result;
use engine_traits::util::check_key_in_range;
use engine_traits::{Iterable, Iterator, Error as EngineError};
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
#[derive(Debug)]
pub struct RegionSnapshot {
    snap: RocksSyncSnapshot,
    region: Arc<Region>,
}

impl RegionSnapshot {
    pub fn new(ps: &PeerStorage) -> RegionSnapshot {
        RegionSnapshot::from_snapshot(ps.raw_snapshot().into_sync(), ps.region().clone())
    }

    pub fn from_raw(db: Arc<DB>, region: Region) -> RegionSnapshot {
        RegionSnapshot::from_snapshot(RocksSnapshot::new(db).into_sync(), region)
    }

    pub fn from_snapshot(snap: RocksSyncSnapshot, region: Region) -> RegionSnapshot {
        RegionSnapshot {
            snap,
            region: Arc::new(region),
        }
    }

    pub fn get_region(&self) -> &Region {
        &self.region
    }

    pub fn iter(&self, iter_opt: IterOption) -> RegionIterator {
        RegionIterator::new(&self.snap, Arc::clone(&self.region), iter_opt)
    }

    pub fn iter_cf(&self, cf: &str, iter_opt: IterOption) -> Result<RegionIterator> {
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
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
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
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
        self.scan_impl(self.iter_cf(cf, iter_opt)?, start_key, f)
    }

    fn scan_impl<F>(&self, mut it: RegionIterator, start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        if !it.seek(start_key)? {
            return Ok(());
        }
        while it.valid() {
            let r = f(it.key(), it.value())?;

            if !r || !it.next() {
                break;
            }
        }

        it.status()
    }

    pub fn get_properties_cf(&self, cf: &str) -> Result<TablePropertiesCollection> {
        let start = keys::enc_start_key(&self.region);
        let end = keys::enc_end_key(&self.region);
        let prop = engine::util::get_range_properties_cf(&self.snap.get_db().as_inner(), cf, &start, &end)?;
        Ok(prop)
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }
}

impl Clone for RegionSnapshot {
    fn clone(&self) -> Self {
        RegionSnapshot {
            snap: self.snap.clone(),
            region: Arc::clone(&self.region),
        }
    }
}

impl Peekable for RegionSnapshot {
    type DBVector = RocksDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> EngineResult<Option<Self::DBVector>> {
        check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = keys::data_key(key);
        self.snap.get_value_opt(opts, &data_key).map_err(|e| {
            CRITICAL_ERROR.with_label_values(&["rocksdb get"]).inc();
            if panic_when_unexpected_key_or_data() {
                set_panic_mark();
                panic!(
                    "failed to get value of key {} in region {}: {:?}",
                    hex::encode_upper(&key),
                    self.region.get_id(),
                    e,
                );
            } else {
                error!(
                    "failed to get value of key";
                    "key" => hex::encode_upper(&key),
                    "region" => self.region.get_id(),
                    "error" => ?e,
                );
                e
            }
        })
    }

    fn get_value_cf_opt(&self, opts: &ReadOptions, cf: &str, key: &[u8]) -> EngineResult<Option<Self::DBVector>> {
        check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )
        .map_err(|e| EngineError::Other(box_err!(e)))?;
        let data_key = keys::data_key(key);
        self.snap.get_value_cf_opt(opts, cf, &data_key).map_err(|e| {
            CRITICAL_ERROR.with_label_values(&["rocksdb get"]).inc();
            if panic_when_unexpected_key_or_data() {
                set_panic_mark();
                panic!(
                    "failed to get value of key {} in region {}: {:?}",
                    hex::encode_upper(&key),
                    self.region.get_id(),
                    e,
                );
            } else {
                error!(
                    "failed to get value of key in cf";
                    "key" => hex::encode_upper(&key),
                    "region" => self.region.get_id(),
                    "cf" => cf,
                    "error" => ?e,
                );
                e
            }
        })
    }
}

/// `RegionIterator` wrap a rocksdb iterator and only allow it to
/// iterate in the region. It behaves as if underlying
/// db only contains one region.
pub struct RegionIterator {
    iter: RocksEngineIterator,
    valid: bool,
    region: Arc<Region>,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

fn update_lower_bound(iter_opt: &mut IterOption, region: &Region) {
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

fn update_upper_bound(iter_opt: &mut IterOption, region: &Region) {
    let region_end_key = keys::enc_end_key(region);
    if iter_opt.upper_bound().is_some() && !iter_opt.upper_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_upper_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_end_key.as_slice() < *iter_opt.upper_bound().as_ref().unwrap() {
            iter_opt.set_vec_upper_bound(region_end_key);
        }
    } else {
        iter_opt.set_vec_upper_bound(region_end_key);
    }
}

// we use engine::rocks's style iterator, doesn't need to impl std iterator.
impl RegionIterator {
    pub fn new(snap: &RocksSyncSnapshot, region: Arc<Region>, mut iter_opt: IterOption) -> RegionIterator {
        update_lower_bound(&mut iter_opt, &region);
        update_upper_bound(&mut iter_opt, &region);
        let start_key = iter_opt.lower_bound().unwrap().to_vec();
        let end_key = iter_opt.upper_bound().unwrap().to_vec();
        let iter = snap
            .iterator_opt(iter_opt)
            .expect("creating snapshot iterator"); // FIXME error handling
        RegionIterator {
            iter,
            valid: false,
            start_key,
            end_key,
            region,
        }
    }

    pub fn new_cf(
        snap: &RocksSyncSnapshot,
        region: Arc<Region>,
        mut iter_opt: IterOption,
        cf: &str,
    ) -> RegionIterator {
        update_lower_bound(&mut iter_opt, &region);
        update_upper_bound(&mut iter_opt, &region);
        let start_key = iter_opt.lower_bound().unwrap().to_vec();
        let end_key = iter_opt.upper_bound().unwrap().to_vec();
        let iter = snap
            .iterator_cf_opt(cf, iter_opt)
            .expect("creating snapshot iterator"); // FIXME error handling
        RegionIterator {
            iter,
            valid: false,
            start_key,
            end_key,
            region,
        }
    }

    pub fn seek_to_first(&mut self) -> bool {
        self.valid = self.iter.seek(self.start_key.as_slice().into());

        self.update_valid(true)
    }

    #[inline]
    fn update_valid(&mut self, forward: bool) -> bool {
        if self.valid {
            let key = self.iter.key();
            self.valid = if forward {
                key < self.end_key.as_slice()
            } else {
                key >= self.start_key.as_slice()
            };
        }
        self.valid
    }

    pub fn seek_to_last(&mut self) -> bool {
        if !self.iter.seek(self.end_key.as_slice().into()) && !self.iter.seek(SeekKey::End) {
            self.valid = false;
            return self.valid;
        }

        while self.iter.key() >= self.end_key.as_slice() && self.iter.prev() {}

        self.valid = self.iter.valid();
        self.update_valid(false)
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        fail_point!("region_snapshot_seek", |_| {
            return Err(box_err!("region seek error"));
        });

        self.should_seekable(key)?;
        let key = keys::data_key(key);
        if key == self.end_key {
            self.valid = false;
        } else {
            self.valid = self.iter.seek(key.as_slice().into());
        }

        Ok(self.update_valid(true))
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.should_seekable(key)?;
        let key = keys::data_key(key);
        self.valid = self.iter.seek_for_prev(key.as_slice().into());
        if self.valid && self.iter.key() == self.end_key.as_slice() {
            self.valid = self.iter.prev();
        }
        Ok(self.update_valid(false))
    }

    pub fn prev(&mut self) -> bool {
        if !self.valid {
            return false;
        }
        self.valid = self.iter.prev();

        self.update_valid(false)
    }

    pub fn next(&mut self) -> bool {
        if !self.valid {
            return false;
        }
        self.valid = self.iter.next();

        self.update_valid(true)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        assert!(self.valid);
        keys::origin_key(self.iter.key())
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        assert!(self.valid);
        self.iter.value()
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.valid
    }

    #[inline]
    pub fn status(&self) -> Result<()> {
        self.iter.status().map_err(From::from)
    }

    #[inline]
    pub fn should_seekable(&self, key: &[u8]) -> Result<()> {
        if let Err(e) = util::check_key_in_region_inclusive(key, &self.region) {
            CRITICAL_ERROR
                .with_label_values(&["key not in region"])
                .inc();
            if panic_when_unexpected_key_or_data() {
                set_panic_mark();
                panic!("key exceed bound: {:?}", e);
            } else {
                return Err(e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use kvproto::metapb::{Peer, Region};
    use tempfile::{Builder, TempDir};

    use crate::config::TiKvConfig;
    use crate::raftstore::store::keys::*;
    use crate::raftstore::store::snap::snap_io::{apply_sst_cf_file, build_sst_cf_file};
    use crate::raftstore::store::PeerStorage;
    use crate::raftstore::Result;
    use crate::storage::mvcc::ScannerBuilder;
    use crate::storage::mvcc::{Write, WriteType};
    use crate::storage::txn::Scanner;
    use crate::storage::{CFStatistics, Cursor, Key, ScanMode};
    use engine::rocks;
    use engine::rocks::util::compact_files_in_range;
    use engine::rocks::{IngestExternalFileOptions, Snapshot, Writable};
    use engine::util::{delete_all_files_in_range, delete_all_in_range};
    use engine::Engines;
    use engine::*;
    use engine::{ALL_CFS, CF_DEFAULT};
    use engine_rocks::RocksIOLimiter;
    use engine_rocks::RocksSstWriterBuilder;
    use engine_traits::{SstWriter, SstWriterBuilder, Peekable};
    use tikv_util::config::{ReadableDuration, ReadableSize};
    use tikv_util::worker;

    use super::*;

    type DataSet = Vec<(Vec<u8>, Vec<u8>)>;

    fn new_temp_engine(path: &TempDir) -> Engines {
        let raft_path = path.path().join(Path::new("raft"));
        let shared_block_cache = false;
        Engines::new(
            Arc::new(
                rocks::util::new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None)
                    .unwrap(),
            ),
            Arc::new(
                rocks::util::new_engine(raft_path.to_str().unwrap(), None, &[CF_DEFAULT], None)
                    .unwrap(),
            ),
            shared_block_cache,
        )
    }

    fn new_peer_storage(engines: Engines, r: &Region) -> PeerStorage {
        let (sched, _) = worker::dummy_scheduler();
        PeerStorage::new(engines, r, sched, 0, "".to_owned()).unwrap()
    }

    fn load_default_dataset(engines: Engines) -> (PeerStorage, DataSet) {
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

    fn load_multiple_levels_dataset(engines: Engines) -> (PeerStorage, DataSet) {
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
                compact_files_in_range(&db, Some(&data_key(k)), Some(&data_key(k)), Some(level))
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

        let snap = RegionSnapshot::new(&store);
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
        let (store, _) = load_default_dataset(engines.clone());
        let snap = RegionSnapshot::new(&store);

        let check_seek_result = |snap: &RegionSnapshot,
                                 lower_bound: Option<&[u8]>,
                                 upper_bound: Option<&[u8]>,
                                 seek_table: &Vec<(
            &[u8],
            bool,
            Option<(&[u8], &[u8])>,
            Option<(&[u8], &[u8])>,
        )>| {
            let iter_opt = IterOption::new(
                lower_bound.map(|v| KeyBuilder::from_slice(v, keys::DATA_PREFIX_KEY.len(), 0)),
                upper_bound.map(|v| KeyBuilder::from_slice(v, keys::DATA_PREFIX_KEY.len(), 0)),
                true,
            );
            let mut iter = snap.iter(iter_opt);
            for (seek_key, in_range, seek_exp, prev_exp) in seek_table.clone() {
                let check_res =
                    |iter: &RegionIterator, res: Result<bool>, exp: Option<(&[u8], &[u8])>| {
                        if !in_range {
                            assert!(
                                res.is_err(),
                                "exp failed at {}",
                                hex::encode_upper(seek_key)
                            );
                            return;
                        }
                        if exp.is_none() {
                            assert!(!res.unwrap(), "exp none at {}", hex::encode_upper(seek_key));
                            return;
                        }

                        assert!(
                            res.unwrap(),
                            "should succeed at {}",
                            hex::encode_upper(seek_key)
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
        let (store, _) = load_multiple_levels_dataset(engines.clone());
        let snap = RegionSnapshot::new(&store);

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

        let snap = RegionSnapshot::new(&store);
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

        let mut iter = snap.iter(IterOption::default());
        assert!(iter.seek_to_first());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next() {
                break;
            }
        }
        assert_eq!(res, base_data[1..3].to_vec());

        // test last region
        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let store = new_peer_storage(engines.clone(), &region);
        let snap = RegionSnapshot::new(&store);
        data.clear();
        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 5);
        assert_eq!(data, base_data);

        let mut iter = snap.iter(IterOption::default());
        assert!(iter.seek(b"a1").unwrap());

        assert!(iter.seek_to_first());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next() {
                break;
            }
        }
        assert_eq!(res, base_data);

        // test iterator with upper bound
        let store = new_peer_storage(engines, &region);
        let snap = RegionSnapshot::new(&store);
        let mut iter = snap.iter(IterOption::new(
            None,
            Some(KeyBuilder::from_slice(b"a5", DATA_PREFIX_KEY.len(), 0)),
            true,
        ));
        assert!(iter.seek_to_first());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.next() {
                break;
            }
        }
        assert_eq!(res, base_data[0..2].to_vec());
    }

    #[test]
    fn test_reverse_iterate() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, test_data) = load_default_dataset(engines.clone());

        let snap = RegionSnapshot::new(&store);
        let mut statistics = CFStatistics::default();
        let it = snap.iter(IterOption::default());
        let mut iter = Cursor::new(it, ScanMode::Mixed);
        assert!(!iter
            .reverse_seek(&Key::from_encoded_slice(b"a2"), &mut statistics)
            .unwrap());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a7"), &mut statistics)
            .unwrap());
        let mut pair = (
            iter.key(&mut statistics).to_vec(),
            iter.value(&mut statistics).to_vec(),
        );
        assert_eq!(pair, (b"a5".to_vec(), b"v5".to_vec()));
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a5"), &mut statistics)
            .unwrap());
        pair = (
            iter.key(&mut statistics).to_vec(),
            iter.value(&mut statistics).to_vec(),
        );
        assert_eq!(pair, (b"a3".to_vec(), b"v3".to_vec()));
        assert!(!iter
            .reverse_seek(&Key::from_encoded_slice(b"a3"), &mut statistics)
            .unwrap());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a1"), &mut statistics)
            .is_err());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a8"), &mut statistics)
            .is_err());

        assert!(iter.seek_to_last(&mut statistics));
        let mut res = vec![];
        loop {
            res.push((
                iter.key(&mut statistics).to_vec(),
                iter.value(&mut statistics).to_vec(),
            ));
            if !iter.prev(&mut statistics) {
                break;
            }
        }
        let mut expect = test_data[1..3].to_vec();
        expect.reverse();
        assert_eq!(res, expect);

        // test last region
        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        let store = new_peer_storage(engines, &region);
        let snap = RegionSnapshot::new(&store);
        let it = snap.iter(IterOption::default());
        let mut iter = Cursor::new(it, ScanMode::Mixed);
        assert!(!iter
            .reverse_seek(&Key::from_encoded_slice(b"a1"), &mut statistics)
            .unwrap());
        assert!(iter
            .reverse_seek(&Key::from_encoded_slice(b"a2"), &mut statistics)
            .unwrap());
        let pair = (
            iter.key(&mut statistics).to_vec(),
            iter.value(&mut statistics).to_vec(),
        );
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        for kv_pairs in test_data.windows(2) {
            let seek_key = Key::from_encoded(kv_pairs[1].0.clone());
            assert!(
                iter.reverse_seek(&seek_key, &mut statistics).unwrap(),
                "{}",
                seek_key
            );
            let pair = (
                iter.key(&mut statistics).to_vec(),
                iter.value(&mut statistics).to_vec(),
            );
            assert_eq!(pair, kv_pairs[0]);
        }

        assert!(iter.seek_to_last(&mut statistics));
        let mut res = vec![];
        loop {
            res.push((
                iter.key(&mut statistics).to_vec(),
                iter.value(&mut statistics).to_vec(),
            ));
            if !iter.prev(&mut statistics) {
                break;
            }
        }
        let mut expect = test_data.clone();
        expect.reverse();
        assert_eq!(res, expect);
    }

    #[test]
    fn test_reverse_iterate_with_lower_bound() {
        let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
        let engines = new_temp_engine(&path);
        let (store, test_data) = load_default_dataset(engines);

        let snap = RegionSnapshot::new(&store);
        let mut iter_opt = IterOption::default();
        iter_opt.set_lower_bound(b"a3", 1);
        let mut iter = snap.iter(iter_opt);
        assert!(iter.seek_to_last());
        let mut res = vec![];
        loop {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
            if !iter.prev() {
                break;
            }
        }
        res.sort();
        assert_eq!(res, test_data[1..3].to_vec());
    }

    #[test]
    fn test_delete_files_in_range_for_titan() {
        let path = Builder::new()
            .prefix("test-titan-delete-files-in-range")
            .tempdir()
            .unwrap();

        // Set configs and create engines
        let mut cfg = TiKvConfig::default();
        let cache = cfg.storage.block_cache.build_shared_cache();
        cfg.rocksdb.titan.enabled = true;
        cfg.rocksdb.titan.disable_gc = true;
        cfg.rocksdb.titan.purge_obsolete_files_period = ReadableDuration::secs(1);
        cfg.rocksdb.defaultcf.disable_auto_compactions = true;
        // Disable dynamic_level_bytes, otherwise SST files would be ingested to L0.
        cfg.rocksdb.defaultcf.dynamic_level_bytes = false;
        cfg.rocksdb.defaultcf.titan.min_gc_batch_size = ReadableSize(0);
        cfg.rocksdb.defaultcf.titan.discardable_ratio = 0.4;
        cfg.rocksdb.defaultcf.titan.sample_ratio = 1.0;
        cfg.rocksdb.defaultcf.titan.min_blob_size = ReadableSize(0);
        let kv_db_opts = cfg.rocksdb.build_opt();
        let kv_cfs_opts = cfg.rocksdb.build_cf_opts(&cache);

        let raft_path = path.path().join(Path::new("titan"));
        let shared_block_cache = false;
        let engines = Engines::new(
            Arc::new(
                rocks::util::new_engine(
                    path.path().to_str().unwrap(),
                    Some(kv_db_opts),
                    ALL_CFS,
                    Some(kv_cfs_opts),
                )
                .unwrap(),
            ),
            Arc::new(
                rocks::util::new_engine(raft_path.to_str().unwrap(), None, &[CF_DEFAULT], None)
                    .unwrap(),
            ),
            shared_block_cache,
        );

        // Write some mvcc keys and values into db
        // default_cf : a_7, b_7
        // write_cf : a_8, b_8
        let start_ts = 7.into();
        let commit_ts = 8.into();
        let write = Write::new(WriteType::Put, start_ts, None);
        let db = &engines.kv;
        let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
        let write_cf = db.cf_handle(CF_WRITE).unwrap();
        db.put_cf(
            &default_cf,
            &data_key(Key::from_raw(b"a").append_ts(start_ts).as_encoded()),
            b"a_value",
        )
        .unwrap();
        db.put_cf(
            &write_cf,
            &data_key(Key::from_raw(b"a").append_ts(commit_ts).as_encoded()),
            &write.as_ref().to_bytes(),
        )
        .unwrap();
        db.put_cf(
            &default_cf,
            &data_key(Key::from_raw(b"b").append_ts(start_ts).as_encoded()),
            b"b_value",
        )
        .unwrap();
        db.put_cf(
            &write_cf,
            &data_key(Key::from_raw(b"b").append_ts(commit_ts).as_encoded()),
            &write.as_ref().to_bytes(),
        )
        .unwrap();

        // Flush and compact the kvs into L6.
        db.flush(true).unwrap();
        compact_files_in_range(&db, None, None, None).unwrap();
        let value = db.get_property_int(&"rocksdb.num-files-at-level0").unwrap();
        assert_eq!(value, 0);
        let value = db.get_property_int(&"rocksdb.num-files-at-level6").unwrap();
        assert_eq!(value, 1);

        // Delete one mvcc kvs we have written above.
        // Here we make the kvs on the L5 by ingesting SST.
        let sst_file_path = Path::new(db.path()).join("for_ingest.sst");
        let mut writer = RocksSstWriterBuilder::new()
            .build(&sst_file_path.to_str().unwrap())
            .unwrap();
        writer
            .delete(&data_key(
                Key::from_raw(b"a").append_ts(start_ts).as_encoded(),
            ))
            .unwrap();
        writer.finish().unwrap();
        let mut opts = IngestExternalFileOptions::new();
        opts.move_files(true);
        db.ingest_external_file_cf(&default_cf, &opts, &[sst_file_path.to_str().unwrap()])
            .unwrap();

        // Now the LSM structure of default cf is:
        // L5: [delete(a_7)]
        // L6: [put(a_7, blob1), put(b_7, blob1)]
        // the ranges of two SST files are overlapped.
        //
        // There is one blob file in Titan
        // blob1: (a_7, a_value), (b_7, b_value)
        let value = db.get_property_int(&"rocksdb.num-files-at-level0").unwrap();
        assert_eq!(value, 0);
        let value = db.get_property_int(&"rocksdb.num-files-at-level5").unwrap();
        assert_eq!(value, 1);
        let value = db.get_property_int(&"rocksdb.num-files-at-level6").unwrap();
        assert_eq!(value, 1);

        // Used to trigger titan gc
        let db = &engines.kv;
        db.put(b"1", b"1").unwrap();
        db.flush(true).unwrap();
        db.put(b"2", b"2").unwrap();
        db.flush(true).unwrap();
        compact_files_in_range(db, Some(b"0"), Some(b"3"), Some(1)).unwrap();

        // Now the LSM structure of default cf is:
        // memtable: [put(b_7, blob4)] (because of Titan GC)
        // L0: [put(1, blob2), put(2, blob3)]
        // L5: [delete(a_7)]
        // L6: [put(a_7, blob1), put(b_7, blob1)]
        // the ranges of two SST files are overlapped.
        //
        // There is four blob files in Titan
        // blob1: (a_7, a_value), (b_7, b_value)
        // blob2: (1, 1)
        // blob3: (2, 2)
        // blob4: (b_7, b_value)
        let value = db.get_property_int(&"rocksdb.num-files-at-level0").unwrap();
        assert_eq!(value, 0);
        let value = db.get_property_int(&"rocksdb.num-files-at-level1").unwrap();
        assert_eq!(value, 1);
        let value = db.get_property_int(&"rocksdb.num-files-at-level5").unwrap();
        assert_eq!(value, 1);
        let value = db.get_property_int(&"rocksdb.num-files-at-level6").unwrap();
        assert_eq!(value, 1);

        // Wait Titan to purge obsolete files
        thread::sleep(Duration::from_secs(2));
        // Now the LSM structure of default cf is:
        // memtable: [put(b_7, blob4)] (because of Titan GC)
        // L0: [put(1, blob2), put(2, blob3)]
        // L5: [delete(a_7)]
        // L6: [put(a_7, blob1), put(b_7, blob1)]
        // the ranges of two SST files are overlapped.
        //
        // There is three blob files in Titan
        // blob2: (1, 1)
        // blob3: (2, 2)
        // blob4: (b_7, b_value)

        // `delete_files_in_range` may expose some old keys.
        // For Titan it may encounter `missing blob file` in `delete_all_in_range`,
        // so we set key_only for Titan.
        delete_all_files_in_range(
            &engines.kv,
            &data_key(Key::from_raw(b"a").as_encoded()),
            &data_key(Key::from_raw(b"b").as_encoded()),
        )
        .unwrap();
        delete_all_in_range(
            &engines.kv,
            &data_key(Key::from_raw(b"a").as_encoded()),
            &data_key(Key::from_raw(b"b").as_encoded()),
            false,
        )
        .unwrap();

        // Now the LSM structure of default cf is:
        // memtable: [put(b_7, blob4)] (because of Titan GC)
        // L0: [put(1, blob2), put(2, blob3)]
        // L6: [put(a_7, blob1), put(b_7, blob1)]
        // the ranges of two SST files are overlapped.
        //
        // There is three blob files in Titan
        // blob2: (1, 1)
        // blob3: (2, 2)
        // blob4: (b_7, b_value)
        let value = db.get_property_int(&"rocksdb.num-files-at-level0").unwrap();
        assert_eq!(value, 0);
        let value = db.get_property_int(&"rocksdb.num-files-at-level1").unwrap();
        assert_eq!(value, 1);
        let value = db.get_property_int(&"rocksdb.num-files-at-level5").unwrap();
        assert_eq!(value, 0);
        let value = db.get_property_int(&"rocksdb.num-files-at-level6").unwrap();
        assert_eq!(value, 1);

        // Generate a snapshot
        let default_sst_file_path = path.path().join("default.sst");
        let write_sst_file_path = path.path().join("write.sst");
        build_sst_cf_file::<RocksIOLimiter>(
            &default_sst_file_path.to_str().unwrap(),
            &Snapshot::new(Arc::clone(&engines.kv)),
            CF_DEFAULT,
            b"",
            b"{",
            None,
        )
        .unwrap();
        build_sst_cf_file::<RocksIOLimiter>(
            &write_sst_file_path.to_str().unwrap(),
            &Snapshot::new(Arc::clone(&engines.kv)),
            CF_WRITE,
            b"",
            b"{",
            None,
        )
        .unwrap();

        // Apply the snapshot to other DB.
        let dir1 = Builder::new()
            .prefix("test-snap-cf-db-apply")
            .tempdir()
            .unwrap();
        let engines1 = new_temp_engine(&dir1);
        apply_sst_cf_file(
            &default_sst_file_path.to_str().unwrap(),
            &engines1.kv,
            CF_DEFAULT,
        )
        .unwrap();
        apply_sst_cf_file(
            &write_sst_file_path.to_str().unwrap(),
            &engines1.kv,
            CF_WRITE,
        )
        .unwrap();

        // Do scan on other DB.
        let mut r = Region::default();
        r.mut_peers().push(Peer::default());
        r.set_start_key(b"a".to_vec());
        r.set_end_key(b"z".to_vec());
        let snapshot = RegionSnapshot::from_raw(Arc::clone(&engines1.kv), r);
        let mut scanner = ScannerBuilder::new(snapshot, 10.into(), false)
            .range(Some(Key::from_raw(b"a")), None)
            .build()
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"b_value".to_vec())),
        );
    }
}
