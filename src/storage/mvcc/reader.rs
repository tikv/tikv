// Copyright 2016 PingCAP, Inc.
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

use storage::engine::{Snapshot, Cursor, ScanMode, Statistics};
use storage::{Key, Value, CF_LOCK, CF_WRITE};
use super::{Error, Result};
use super::lock::Lock;
use super::write::{Write, WriteType};
use raftstore::store::engine::IterOption;
use std::u64;
use kvproto::kvrpcpb::IsolationLevel;
use util::properties::GetPropertiesOptions;

const GC_MAX_ROW_VERSIONS_THRESHOLD: u64 = 100;

pub struct MvccReader<'a> {
    snapshot: &'a Snapshot,
    statistics: &'a mut Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor<'a>>,
    lock_cursor: Option<Cursor<'a>>,
    write_cursor: Option<Cursor<'a>>,

    scan_mode: Option<ScanMode>,
    key_only: bool,

    fill_cache: bool,
    upper_bound: Option<Vec<u8>>,
    isolation_level: IsolationLevel,
}

impl<'a> MvccReader<'a> {
    pub fn new(snapshot: &'a Snapshot,
               statistics: &'a mut Statistics,
               scan_mode: Option<ScanMode>,
               fill_cache: bool,
               upper_bound: Option<Vec<u8>>,
               isolation_level: IsolationLevel)
               -> MvccReader<'a> {
        MvccReader {
            snapshot: snapshot,
            statistics: statistics,
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode: scan_mode,
            isolation_level: isolation_level,
            key_only: false,
            fill_cache: fill_cache,
            upper_bound: upper_bound,
        }
    }

    pub fn reset(&mut self, upper_bound: Option<Vec<u8>>) {
        self.upper_bound = upper_bound;
        self.data_cursor = None;
        self.lock_cursor = None;
        self.write_cursor = None;
    }

    pub fn set_key_only(&mut self, key_only: bool) {
        self.key_only = key_only;
    }

    pub fn load_data(&mut self, key: &Key, ts: u64) -> Result<Value> {
        if self.key_only {
            return Ok(vec![]);
        }
        if self.scan_mode.is_some() && self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, self.fill_cache);
            self.data_cursor = Some(try!(self.snapshot.iter(iter_opt, self.get_scan_mode(true))));
        }

        let k = key.append_ts(ts);
        let res = if let Some(ref mut cursor) = self.data_cursor {
            match try!(cursor.get(&k, &mut self.statistics.data)) {
                None => panic!("key {} not found, ts {}", key, ts),
                Some(v) => v.to_vec(),
            }
        } else {
            self.statistics.data.get += 1;
            match try!(self.snapshot.get(&k)) {
                None => panic!("key {} not found, ts: {}", key, ts),
                Some(v) => v,
            }
        };

        self.statistics.data.processed += 1;

        Ok(res)
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        if self.scan_mode.is_some() && self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(None, true);
            let iter = try!(self.snapshot.iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true)));
            self.lock_cursor = Some(iter);
        }

        let res = if let Some(ref mut cursor) = self.lock_cursor {
            match try!(cursor.get(key, &mut self.statistics.lock)) {
                Some(v) => Some(try!(Lock::parse(v))),
                None => None,
            }
        } else {
            self.statistics.lock.get += 1;
            match try!(self.snapshot.get_cf(CF_LOCK, key)) {
                Some(v) => Some(try!(Lock::parse(&v))),
                None => None,
            }
        };

        if res.is_some() {
            self.statistics.lock.processed += 1;
        }

        Ok(res)
    }

    fn get_scan_mode(&self, allow_backward: bool) -> ScanMode {
        match self.scan_mode {
            Some(ScanMode::Forward) => ScanMode::Forward,
            Some(ScanMode::Backward) if allow_backward => ScanMode::Backward,
            _ => ScanMode::Mixed,
        }
    }

    pub fn seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, Write)>> {
        self.seek_write_impl(key, ts, false)
    }

    pub fn reverse_seek_write(&mut self, key: &Key, ts: u64) -> Result<Option<(u64, Write)>> {
        self.seek_write_impl(key, ts, true)
    }

    fn seek_write_impl(&mut self,
                       key: &Key,
                       ts: u64,
                       reverse: bool)
                       -> Result<Option<(u64, Write)>> {
        if self.scan_mode.is_some() {
            if self.write_cursor.is_none() {
                let iter_opt = IterOption::new(None, self.fill_cache);
                let iter = try!(self.snapshot
                    .iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(false)));
                self.write_cursor = Some(iter);
            }
        } else {
            // use prefix bloom filter
            let iter_opt = IterOption::default().use_prefix_seek().set_prefix_same_as_start(true);
            let iter = try!(self.snapshot.iter_cf(CF_WRITE, iter_opt, ScanMode::Mixed));
            self.write_cursor = Some(iter);
        }

        let mut cursor = self.write_cursor.as_mut().unwrap();
        let ok = if reverse {
            try!(cursor.near_seek_for_prev(&key.append_ts(ts), &mut self.statistics.write))
        } else {
            try!(cursor.near_seek(&key.append_ts(ts), &mut self.statistics.write))
        };
        if !ok {
            return Ok(None);
        }
        let write_key = Key::from_encoded(cursor.key().to_vec());
        let commit_ts = try!(write_key.decode_ts());
        let k = try!(write_key.truncate_ts());
        if &k != key {
            return Ok(None);
        }
        let write = try!(Write::parse(cursor.value()));
        self.statistics.write.processed += 1;
        Ok(Some((commit_ts, write)))
    }

    fn check_lock(&mut self, key: &Key, mut ts: u64) -> Result<Option<u64>> {
        if let Some(lock) = try!(self.load_lock(key)) {
            if lock.ts <= ts {
                if ts == u64::MAX && try!(key.raw()) == lock.primary {
                    // when ts==u64::MAX(which means to get latest committed version for
                    // primary key),and current key is the primary key, returns the latest
                    // commit version's value
                    ts = lock.ts - 1;
                } else {
                    // There is a pending lock. Client should wait or clean it.
                    return Err(Error::KeyIsLocked {
                        key: try!(key.raw()),
                        primary: lock.primary,
                        ts: lock.ts,
                        ttl: lock.ttl,
                    });
                }
            }
        }
        Ok(Some(ts))
    }

    pub fn get(&mut self, key: &Key, mut ts: u64) -> Result<Option<Value>> {
        // Check for locks that signal concurrent writes.
        match self.isolation_level {
            IsolationLevel::SI => {
                if let Some(new_ts) = try!(self.check_lock(key, ts)) {
                    ts = new_ts;
                }
            }
            IsolationLevel::RC => {}
        }
        loop {
            match try!(self.seek_write(key, ts)) {
                Some((commit_ts, mut write)) => {
                    match write.write_type {
                        WriteType::Put => {
                            self.statistics.write.processed += 1;
                            if write.short_value.is_some() {
                                if self.key_only {
                                    return Ok(Some(vec![]));
                                }
                                return Ok(write.short_value.take());
                            }
                            return self.load_data(key, write.start_ts).map(Some);
                        }
                        WriteType::Delete => {
                            self.statistics.write.processed += 1;
                            return Ok(None);
                        }
                        WriteType::Lock | WriteType::Rollback => ts = commit_ts - 1,
                    }
                }
                None => return Ok(None),
            }
        }
    }

    pub fn get_txn_commit_info(&mut self,
                               key: &Key,
                               start_ts: u64)
                               -> Result<Option<(u64, WriteType)>> {
        let mut seek_ts = start_ts;
        while let Some((commit_ts, write)) = try!(self.reverse_seek_write(key, seek_ts)) {
            if write.start_ts == start_ts {
                self.statistics.write.processed += 1;
                return Ok(Some((commit_ts, write.write_type)));
            }
            seek_ts = commit_ts + 1;
        }
        Ok(None)
    }

    fn create_data_cursor(&mut self) -> Result<()> {
        if self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, self.fill_cache);
            let iter = try!(self.snapshot.iter(iter_opt, self.get_scan_mode(true)));
            self.data_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_none() {
            let iter_opt = IterOption::new(self.upper_bound.as_ref().cloned(), self.fill_cache);
            let iter = try!(self.snapshot.iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(false)));
            self.write_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(self.upper_bound.as_ref().cloned(), true);
            let iter = try!(self.snapshot.iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true)));
            self.lock_cursor = Some(iter);
        }
        Ok(())
    }

    // Return the first committed key which start_ts equals to ts
    pub fn seek_ts(&mut self, ts: u64) -> Result<Option<Key>> {
        assert!(self.scan_mode.is_some());
        try!(self.create_write_cursor());

        let mut cursor = self.write_cursor.as_mut().unwrap();
        let mut ok = cursor.seek_to_first(&mut self.statistics.write);

        while ok {
            if try!(Write::parse(cursor.value())).start_ts == ts {
                return Ok(Some(try!(Key::from_encoded(cursor.key().to_vec()).truncate_ts())));
            }
            ok = cursor.next(&mut self.statistics.write);
        }
        Ok(None)
    }

    pub fn seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        assert!(self.scan_mode.is_some());
        try!(self.create_write_cursor());
        try!(self.create_lock_cursor());

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let mut w_cur = self.write_cursor.as_mut().unwrap();
                let mut l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if try!(w_cur.near_seek(&key, &mut self.statistics.write)) {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if try!(l_cur.near_seek(&key, &mut self.statistics.lock)) {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => try!(Key::from_encoded(k.to_vec()).truncate_ts()),
                    (Some(wk), Some(lk)) => {
                        if wk < lk {
                            try!(Key::from_encoded(wk.to_vec()).truncate_ts())
                        } else {
                            Key::from_encoded(lk.to_vec())
                        }
                    }
                }
            };
            if let Some(v) = try!(self.get(&key, ts)) {
                return Ok(Some((key, v)));
            }
            key = key.append_ts(0);
        }
    }

    pub fn reverse_seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        assert!(self.scan_mode.is_some());
        try!(self.create_write_cursor());
        try!(self.create_lock_cursor());

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let mut w_cur = self.write_cursor.as_mut().unwrap();
                let mut l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if try!(w_cur.near_reverse_seek(&key, &mut self.statistics.write)) {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if try!(l_cur.near_reverse_seek(&key, &mut self.statistics.lock)) {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => try!(Key::from_encoded(k.to_vec()).truncate_ts()),
                    (Some(wk), Some(lk)) => {
                        if wk < lk {
                            Key::from_encoded(lk.to_vec())
                        } else {
                            try!(Key::from_encoded(wk.to_vec()).truncate_ts())
                        }
                    }
                }
            };
            if let Some(v) = try!(self.get(&key, ts)) {
                return Ok(Some((key, v)));
            }
        }
    }

    #[allow(type_complexity)]
    pub fn scan_lock<F>(&mut self,
                        start: Option<Key>,
                        filter: F,
                        limit: Option<usize>)
                        -> Result<(Vec<(Key, Lock)>, Option<Key>)>
        where F: Fn(&Lock) -> bool
    {
        try!(self.create_lock_cursor());
        let mut cursor = self.lock_cursor.as_mut().unwrap();
        let ok = match start {
            Some(ref x) => try!(cursor.seek(x, &mut self.statistics.lock)),
            None => cursor.seek_to_first(&mut self.statistics.lock),
        };
        if !ok {
            return Ok((vec![], None));
        }
        let mut locks = vec![];
        while cursor.valid() {
            let key = Key::from_encoded(cursor.key().to_vec());
            let lock = try!(Lock::parse(cursor.value()));
            if filter(&lock) {
                locks.push((key.clone(), lock));
                if let Some(limit) = limit {
                    if locks.len() >= limit {
                        return Ok((locks, Some(key)));
                    }
                }
            }
            cursor.next(&mut self.statistics.lock);
        }
        self.statistics.lock.processed += locks.len();
        Ok((locks, None))
    }

    pub fn scan_keys(&mut self,
                     mut start: Option<Key>,
                     limit: usize)
                     -> Result<(Vec<Key>, Option<Key>)> {
        let iter_opt = IterOption::new(None, self.fill_cache);
        let scan_mode = self.get_scan_mode(false);
        let mut cursor = try!(self.snapshot.iter_cf(CF_WRITE, iter_opt, scan_mode));
        let mut keys = vec![];
        loop {
            let ok = match start {
                Some(ref x) => try!(cursor.near_seek(x, &mut self.statistics.write)),
                None => cursor.seek_to_first(&mut self.statistics.write),
            };
            if !ok {
                return Ok((keys, None));
            }
            if keys.len() >= limit {
                self.statistics.write.processed += keys.len();
                return Ok((keys, start));
            }
            let key = try!(Key::from_encoded(cursor.key().to_vec()).truncate_ts());
            start = Some(key.append_ts(0));
            keys.push(key);
        }
    }

    // Get all Value of the given key in CF_DEFAULT
    pub fn scan_values_in_default(&mut self, key: &Key) -> Result<Vec<(u64, Value)>> {
        try!(self.create_data_cursor());
        let mut cursor = self.data_cursor.as_mut().unwrap();
        let mut ok = cursor.seek_to_first(&mut self.statistics.data);
        if !ok {
            return Err(Error::Other(From::from("data_cursor cannot seek_to_first".to_string())));
        }
        ok = try!(cursor.seek(key, &mut self.statistics.data));
        if !ok {
            return Err(Error::Other(From::from(format!("data_cursor cannot seek to {:?}", key)
                .to_string())));
        }
        let mut v = vec![];
        while ok {
            let cur_key = Key::from_encoded(cursor.key().to_vec());
            let cur_key_without_ts = try!(cur_key.truncate_ts());
            if cur_key_without_ts.encoded().as_slice() == key.encoded().as_slice() {
                v.push((try!(cur_key.decode_ts()), cursor.value().to_vec()));
            }
            if cur_key_without_ts.encoded().as_slice() != key.encoded().as_slice() {
                break;
            }
            ok = cursor.next(&mut self.statistics.data);
        }
        Ok(v)
    }

    // Returns true if it needs gc.
    // This is for optimization purpose, does not mean to be accurate.
    pub fn need_gc(&self, safe_point: u64, ratio_threshold: f64) -> bool {
        // Always GC.
        if ratio_threshold < 1.0 {
            return true;
        }

        let mut opts = GetPropertiesOptions::default();
        opts.max_ts = Some(safe_point);
        let props = match self.snapshot.get_properties_cf(CF_WRITE, &opts) {
            Ok(v) => v,
            Err(_) => return true,
        };

        // No data older than safe_point to GC.
        if props.min_ts > safe_point {
            return false;
        }

        // Note: Since the properties are file-based, it can be false positive.
        // For example, multiple files can have a different version of the same row.

        // A lot of MVCC versions to GC.
        if props.num_versions as f64 > props.num_rows as f64 * ratio_threshold {
            return true;
        }
        // A lot of non-effective MVCC versions to GC.
        if props.num_versions as f64 > props.num_puts as f64 * ratio_threshold {
            return true;
        }

        // A lot of MVCC versions of a single row to GC.
        props.max_row_versions > GC_MAX_ROW_VERSIONS_THRESHOLD
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::u64;
    use kvproto::metapb::{Peer, Region};
    use kvproto::kvrpcpb::IsolationLevel;
    use rocksdb::{self, DB, Writable, WriteBatch};
    use std::sync::Arc;
    use storage::{Options, Mutation, Statistics, ALL_CFS, CF_DEFAULT, CF_RAFT, CF_LOCK, CF_WRITE,
                  make_key};
    use storage::engine::Modify;
    use storage::mvcc::{MvccTxn, MvccReader};
    use tempdir::TempDir;
    use raftstore::coprocessor::RegionSnapshot;
    use raftstore::errors::Result;
    use raftstore::store::keys;
    use util::rocksdb::{self as rocksdb_util, CFOptions};
    use util::properties::{UserProperties, UserPropertiesCollectorFactory};

    struct RegionEngine {
        db: Arc<DB>,
        region: Region,
    }

    impl RegionEngine {
        pub fn new(db: Arc<DB>, region: Region) -> RegionEngine {
            RegionEngine {
                db: db.clone(),
                region: region,
            }
        }

        pub fn put(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Put((make_key(pk), vec![]));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn lock(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Lock(make_key(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        pub fn delete(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let m = Mutation::Delete(make_key(pk));
            self.prewrite(m, pk, start_ts);
            self.commit(pk, start_ts, commit_ts);
        }

        fn prewrite(&mut self, m: Mutation, pk: &[u8], start_ts: u64) {
            let mut stat = Statistics::default();
            let snap = RegionSnapshot::from_raw(self.db.clone(), self.region.clone());
            let mut txn = MvccTxn::new(&snap, &mut stat, start_ts, None, IsolationLevel::SI);
            txn.prewrite(m, pk, &Options::default()).unwrap();
            self.write(txn.modifies());
        }

        fn commit(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let k = make_key(pk);
            let mut stat = Statistics::default();
            let snap = RegionSnapshot::from_raw(self.db.clone(), self.region.clone());
            let mut txn = MvccTxn::new(&snap, &mut stat, start_ts, None, IsolationLevel::SI);
            txn.commit(&k, commit_ts).unwrap();
            self.write(txn.modifies());
        }

        fn gc(&mut self, pk: &[u8], safe_point: u64) {
            let k = make_key(pk);
            let mut stat = Statistics::default();
            let snap = RegionSnapshot::from_raw(self.db.clone(), self.region.clone());
            let mut txn = MvccTxn::new(&snap, &mut stat, safe_point, None, IsolationLevel::SI);
            txn.gc(&k, safe_point).unwrap();
            self.write(txn.modifies());
        }

        fn write(&mut self, modifies: Vec<Modify>) {
            let db = &self.db;
            let wb = WriteBatch::new();
            for rev in modifies {
                match rev {
                    Modify::Put(cf, k, v) => {
                        let k = keys::data_key(k.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.put_cf(handle, &k, &v).unwrap();
                    }
                    Modify::Delete(cf, k) => {
                        let k = keys::data_key(k.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.delete_cf(handle, &k).unwrap();
                    }
                }
            }
            db.write(wb).unwrap();
        }

        fn flush(&mut self) {
            for cf in ALL_CFS {
                let cf = rocksdb_util::get_cf_handle(&self.db, cf).unwrap();
                self.db.flush_cf(cf, true).unwrap();
            }
        }

        fn compact(&mut self) {
            for cf in ALL_CFS {
                let cf = rocksdb_util::get_cf_handle(&self.db, cf).unwrap();
                self.db.compact_range_cf(cf, None, None);
            }
        }
    }

    fn open_db(path: &str, with_properties: bool) -> Arc<DB> {
        let db_opts = rocksdb::Options::new();
        let mut cf_opts = rocksdb::Options::new();
        if with_properties {
            let f = Box::new(UserPropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.test-collector", f);
        }
        let cfs_opts = vec![CFOptions::new(CF_DEFAULT, rocksdb::Options::new()),
                            CFOptions::new(CF_RAFT, rocksdb::Options::new()),
                            CFOptions::new(CF_LOCK, rocksdb::Options::new()),
                            CFOptions::new(CF_WRITE, cf_opts)];
        Arc::new(rocksdb_util::new_engine_opt(path, db_opts, cfs_opts).unwrap())
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
        let mut peer = Peer::new();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = Region::new();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    fn get_properties(db: Arc<DB>, region: Region, safe_point: u64) -> Result<UserProperties> {
        let mut opts = GetPropertiesOptions::default();
        opts.max_ts = Some(safe_point);
        let snap = RegionSnapshot::from_raw(db.clone(), region.clone());
        snap.get_properties_cf(CF_WRITE, &opts)
    }

    fn check_need_gc(db: Arc<DB>, region: Region, safe_point: u64, need_gc: bool) {
        let snap = RegionSnapshot::from_raw(db.clone(), region.clone());
        let mut stat = Statistics::default();
        let reader = MvccReader::new(&snap, &mut stat, None, false, None, IsolationLevel::SI);
        assert_eq!(reader.need_gc(safe_point, 1.0), need_gc);
    }

    #[test]
    fn test_need_gc() {
        let path = TempDir::new("_test_storage_mvcc_reader").expect("");
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![10]);
        test_without_properties(path, &region);
        test_with_properties(path, &region);
    }

    fn test_without_properties(path: &str, region: &Region) {
        let db = open_db(path, false);
        let mut engine = RegionEngine::new(db.clone(), region.clone());

        // Put 2 keys.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        assert!(get_properties(db.clone(), region.clone(), 10).is_err());
        check_need_gc(db.clone(), region.clone(), 10, true);
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(get_properties(db.clone(), region.clone(), 10).is_err());
        check_need_gc(db.clone(), region.clone(), 10, true);
    }

    #[allow(cyclomatic_complexity)]
    fn test_with_properties(path: &str, region: &Region) {
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(db.clone(), region.clone());

        // Put 2 keys.
        engine.put(&[2], 3, 3);
        engine.put(&[3], 4, 4);
        engine.flush();
        // After this flush, we have a SST file w/ properties, plus the SST
        // file w/o properties from previous flush. We always need GC as
        // long as we can't get properties from any SST files.
        assert!(get_properties(db.clone(), region.clone(), 10).is_err());
        check_need_gc(db.clone(), region.clone(), 10, true);
        engine.compact();
        // After this compact, the two SST files are compacted into a new
        // SST file with properties. Now all SST files have properties and
        // all keys have only one version, so we don't need gc.
        let props = get_properties(db.clone(), region.clone(), 10).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);
        check_need_gc(db.clone(), region.clone(), 10, false);

        // Put 2 more keys and delete them.
        engine.put(&[5], 5, 5);
        engine.put(&[6], 6, 6);
        engine.delete(&[5], 7, 7);
        engine.delete(&[6], 8, 8);
        engine.flush();
        // After this flush, keys 5,6 in the new SST file have more than one
        // versions, so we need gc.
        let props = get_properties(db.clone(), region.clone(), 10).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 8);
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        check_need_gc(db.clone(), region.clone(), 10, true);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props = get_properties(db.clone(), region.clone(), 0).unwrap();
        assert_eq!(props.min_ts, u64::MAX);
        assert_eq!(props.max_ts, 0);
        assert_eq!(props.num_rows, 0);
        assert_eq!(props.num_puts, 0);
        assert_eq!(props.num_versions, 0);
        assert_eq!(props.max_row_versions, 0);
        check_need_gc(db.clone(), region.clone(), 0, false);

        // We gc the two deleted keys manually.
        engine.gc(&[5], 10);
        engine.gc(&[6], 10);
        engine.compact();
        // After this compact, all versions of keys 5,6 are deleted,
        // no keys have more than one versions, so we don't need gc.
        let props = get_properties(db.clone(), region.clone(), 10).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);
        check_need_gc(db.clone(), region.clone(), 10, false);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props = get_properties(db.clone(), region.clone(), 10).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 9);
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
        check_need_gc(db.clone(), region.clone(), 10, true);
    }
}
