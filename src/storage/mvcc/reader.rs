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

use super::lock::{Lock, LockType};
use super::write::{Write, WriteType};
use super::{Error, Result};
use kvproto::kvrpcpb::IsolationLevel;
use raftstore::store::engine::IterOption;
use std::u64;
use storage::engine::{Cursor, ScanMode, Snapshot, Statistics};
use storage::{Key, Value, CF_LOCK, CF_WRITE};
use util::properties::MvccProperties;

const GC_MAX_ROW_VERSIONS_THRESHOLD: u64 = 100;

// When there are many versions for the user key, after several tries,
// we will use seek to locate the right position. But this will turn around
// the write cf's iterator's direction inside RocksDB, and the next user key
// need to turn back the direction to backward. As we have tested, turn around
// iterator's direction from forward to backward is as expensive as seek in
// RocksDB, so don't set REVERSE_SEEK_BOUND too small.
const REVERSE_SEEK_BOUND: u64 = 32;

pub struct MvccReader {
    snapshot: Box<Snapshot>,
    statistics: Statistics,
    // cursors are used for speeding up scans.
    data_cursor: Option<Cursor>,
    lock_cursor: Option<Cursor>,
    write_cursor: Option<Cursor>,

    scan_mode: Option<ScanMode>,
    key_only: bool,

    fill_cache: bool,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    isolation_level: IsolationLevel,
}

impl MvccReader {
    pub fn new(
        snapshot: Box<Snapshot>,
        scan_mode: Option<ScanMode>,
        fill_cache: bool,
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
        isolation_level: IsolationLevel,
    ) -> MvccReader {
        MvccReader {
            snapshot,
            statistics: Statistics::default(),
            data_cursor: None,
            lock_cursor: None,
            write_cursor: None,
            scan_mode,
            isolation_level,
            key_only: false,
            fill_cache,
            lower_bound,
            upper_bound,
        }
    }

    pub fn get_statistics(&self) -> &Statistics {
        &self.statistics
    }

    pub fn collect_statistics_into(&mut self, stats: &mut Statistics) {
        stats.add(&self.statistics);
        self.statistics = Statistics::default();
    }

    pub fn set_key_only(&mut self, key_only: bool) {
        self.key_only = key_only;
    }

    pub fn load_data(&mut self, key: &Key, ts: u64) -> Result<Value> {
        if self.key_only {
            return Ok(vec![]);
        }
        if self.scan_mode.is_some() && self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, self.fill_cache);
            self.data_cursor = Some(self.snapshot.iter(iter_opt, self.get_scan_mode(true))?);
        }

        let k = key.append_ts(ts);
        let res = if let Some(ref mut cursor) = self.data_cursor {
            match cursor.get(&k, &mut self.statistics.data)? {
                None => panic!("key {} not found, ts {}", key, ts),
                Some(v) => v.to_vec(),
            }
        } else {
            self.statistics.data.get += 1;
            match self.snapshot.get(&k)? {
                None => panic!("key {} not found, ts: {}", key, ts),
                Some(v) => v,
            }
        };

        self.statistics.data.processed += 1;
        self.statistics.data.flow_stats.read_bytes += k.raw().unwrap_or_default().len() + res.len();
        self.statistics.data.flow_stats.read_keys += 1;
        Ok(res)
    }

    pub fn load_lock(&mut self, key: &Key) -> Result<Option<Lock>> {
        if self.scan_mode.is_some() && self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, true);
            let iter = self
                .snapshot
                .iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }

        let res = if let Some(ref mut cursor) = self.lock_cursor {
            match cursor.get(key, &mut self.statistics.lock)? {
                Some(v) => Some(Lock::parse(v)?),
                None => None,
            }
        } else {
            self.statistics.lock.get += 1;
            match self.snapshot.get_cf(CF_LOCK, key)? {
                Some(v) => Some(Lock::parse(&v)?),
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

    fn seek_write_impl(
        &mut self,
        key: &Key,
        ts: u64,
        reverse: bool,
    ) -> Result<Option<(u64, Write)>> {
        if self.scan_mode.is_some() {
            if self.write_cursor.is_none() {
                let iter_opt = IterOption::new(None, None, self.fill_cache);
                let iter = self
                    .snapshot
                    .iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(false))?;
                self.write_cursor = Some(iter);
            }
        } else {
            // use prefix bloom filter
            let iter_opt = IterOption::default()
                .use_prefix_seek()
                .set_prefix_same_as_start(true);
            let iter = self.snapshot.iter_cf(CF_WRITE, iter_opt, ScanMode::Mixed)?;
            self.write_cursor = Some(iter);
        }

        let cursor = self.write_cursor.as_mut().unwrap();
        let ok = if reverse {
            cursor.near_seek_for_prev(&key.append_ts(ts), &mut self.statistics.write)?
        } else {
            cursor.near_seek(&key.append_ts(ts), &mut self.statistics.write)?
        };
        if !ok {
            return Ok(None);
        }
        let write_key = Key::from_encoded(cursor.key().to_vec());
        let commit_ts = write_key.decode_ts()?;
        let k = write_key.truncate_ts()?;
        if &k != key {
            return Ok(None);
        }
        let write = Write::parse(cursor.value())?;
        self.statistics.write.processed += 1;
        self.statistics.write.flow_stats.read_bytes += cursor.key().len() + cursor.value().len();
        self.statistics.write.flow_stats.read_keys += 1;
        Ok(Some((commit_ts, write)))
    }

    fn check_lock(&mut self, key: &Key, ts: u64) -> Result<u64> {
        if let Some(lock) = self.load_lock(key)? {
            return self.check_lock_impl(key, ts, lock);
        }
        Ok(ts)
    }

    fn check_lock_impl(&self, key: &Key, ts: u64, lock: Lock) -> Result<u64> {
        if lock.ts > ts || lock.lock_type == LockType::Lock {
            // ignore lock when lock.ts > ts or lock's type is Lock
            return Ok(ts);
        }

        if ts == u64::MAX && key.raw()? == lock.primary {
            // when ts==u64::MAX(which means to get latest committed version for
            // primary key),and current key is the primary key, returns the latest
            // commit version's value
            return Ok(lock.ts - 1);
        }

        // There is a pending lock. Client should wait or clean it.
        Err(Error::KeyIsLocked {
            key: key.raw()?,
            primary: lock.primary,
            ts: lock.ts,
            ttl: lock.ttl,
        })
    }

    pub fn get(&mut self, key: &Key, mut ts: u64) -> Result<Option<Value>> {
        // Check for locks that signal concurrent writes.
        match self.isolation_level {
            IsolationLevel::SI => ts = self.check_lock(key, ts)?,
            IsolationLevel::RC => {}
        }
        loop {
            match self.seek_write(key, ts)? {
                Some((commit_ts, mut write)) => match write.write_type {
                    WriteType::Put => {
                        if write.short_value.is_some() {
                            if self.key_only {
                                return Ok(Some(vec![]));
                            }
                            return Ok(write.short_value.take());
                        }
                        return self.load_data(key, write.start_ts).map(Some);
                    }
                    WriteType::Delete => {
                        return Ok(None);
                    }
                    WriteType::Lock | WriteType::Rollback => ts = commit_ts - 1,
                },
                None => return Ok(None),
            }
        }
    }

    pub fn get_txn_commit_info(
        &mut self,
        key: &Key,
        start_ts: u64,
    ) -> Result<Option<(u64, WriteType)>> {
        let mut seek_ts = start_ts;
        while let Some((commit_ts, write)) = self.reverse_seek_write(key, seek_ts)? {
            if write.start_ts == start_ts {
                return Ok(Some((commit_ts, write.write_type)));
            }
            seek_ts = commit_ts + 1;
        }
        Ok(None)
    }

    fn create_data_cursor(&mut self) -> Result<()> {
        self.scan_mode = Some(ScanMode::Forward);
        if self.data_cursor.is_none() {
            let iter_opt = IterOption::new(None, None, self.fill_cache);
            let iter = self.snapshot.iter(iter_opt, self.get_scan_mode(true))?;
            self.data_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_write_cursor(&mut self) -> Result<()> {
        if self.write_cursor.is_none() {
            let iter_opt = IterOption::new(
                self.lower_bound.as_ref().cloned(),
                self.upper_bound.as_ref().cloned(),
                self.fill_cache,
            );
            let iter = self
                .snapshot
                .iter_cf(CF_WRITE, iter_opt, self.get_scan_mode(true))?;
            self.write_cursor = Some(iter);
        }
        Ok(())
    }

    fn create_lock_cursor(&mut self) -> Result<()> {
        if self.lock_cursor.is_none() {
            let iter_opt = IterOption::new(
                self.lower_bound.as_ref().cloned(),
                self.upper_bound.as_ref().cloned(),
                true,
            );
            let iter = self
                .snapshot
                .iter_cf(CF_LOCK, iter_opt, self.get_scan_mode(true))?;
            self.lock_cursor = Some(iter);
        }
        Ok(())
    }

    // Return the first committed key which start_ts equals to ts
    pub fn seek_ts(&mut self, ts: u64) -> Result<Option<Key>> {
        assert!(self.scan_mode.is_some());
        self.create_write_cursor()?;

        let cursor = self.write_cursor.as_mut().unwrap();
        let mut ok = cursor.seek_to_first(&mut self.statistics.write);

        while ok {
            if Write::parse(cursor.value())?.start_ts == ts {
                return Ok(Some(
                    Key::from_encoded(cursor.key().to_vec()).truncate_ts()?,
                ));
            }
            ok = cursor.next(&mut self.statistics.write);
        }
        Ok(None)
    }

    pub fn seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        assert!(*self.scan_mode.as_ref().unwrap() == ScanMode::Forward);
        self.create_write_cursor()?;
        self.create_lock_cursor()?;

        let (mut write_valid, mut lock_valid) = (true, true);

        loop {
            key = {
                let w_cur = self.write_cursor.as_mut().unwrap();
                let l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if w_cur.near_seek(&key, &mut self.statistics.write)? {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if l_cur.near_seek(&key, &mut self.statistics.lock)? {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => Key::from_encoded(k.to_vec()).truncate_ts()?,
                    (Some(wk), Some(lk)) => if wk < lk {
                        Key::from_encoded(wk.to_vec()).truncate_ts()?
                    } else {
                        Key::from_encoded(lk.to_vec())
                    },
                }
            };
            if let Some(v) = self.get(&key, ts)? {
                return Ok(Some((key, v)));
            }
            key = key.append_ts(0);
        }
    }

    pub fn reverse_seek(&mut self, mut key: Key, ts: u64) -> Result<Option<(Key, Value)>> {
        assert!(*self.scan_mode.as_ref().unwrap() == ScanMode::Backward);
        self.create_write_cursor()?;
        self.create_lock_cursor()?;

        let (mut write_valid, mut lock_valid) = (true, true);
        loop {
            key = {
                let w_cur = self.write_cursor.as_mut().unwrap();
                let l_cur = self.lock_cursor.as_mut().unwrap();
                let (mut w_key, mut l_key) = (None, None);
                if write_valid {
                    if w_cur.near_reverse_seek(&key, &mut self.statistics.write)? {
                        w_key = Some(w_cur.key());
                    } else {
                        w_key = None;
                        write_valid = false;
                    }
                }
                if lock_valid {
                    if l_cur.near_reverse_seek(&key, &mut self.statistics.lock)? {
                        l_key = Some(l_cur.key());
                    } else {
                        l_key = None;
                        lock_valid = false;
                    }
                }
                match (w_key, l_key) {
                    (None, None) => return Ok(None),
                    (None, Some(k)) => Key::from_encoded(k.to_vec()),
                    (Some(k), None) => Key::from_encoded(k.to_vec()).truncate_ts()?,
                    (Some(wk), Some(lk)) => if wk < lk {
                        Key::from_encoded(lk.to_vec())
                    } else {
                        Key::from_encoded(wk.to_vec()).truncate_ts()?
                    },
                }
            };

            if let Some(v) = self.reverse_get_impl(&key, ts)? {
                return Ok(Some((key, v)));
            }

            // reverse_get_impl may call write_cursor's prev.
            if !self.write_cursor.as_ref().unwrap().valid() {
                write_valid = false;
            }
        }
    }

    fn reverse_get_impl(&mut self, user_key: &Key, ts: u64) -> Result<Option<Value>> {
        assert!(self.lock_cursor.is_some());
        assert!(self.write_cursor.is_some());

        // Check lock.
        match self.isolation_level {
            IsolationLevel::SI => {
                let l_cur = self.lock_cursor.as_ref().unwrap();
                if l_cur.valid() && l_cur.key() == user_key.encoded().as_slice() {
                    self.statistics.lock.processed += 1;
                    self.check_lock_impl(user_key, ts, Lock::parse(l_cur.value())?)?;
                }
            }
            IsolationLevel::RC => {}
        }

        // Get value for this user key.
        // At first, we use several prev to try to get the latest version.
        let mut lastest_version = (None /* start_ts */, None /* short value */);
        let mut last_handled_key: Option<Vec<u8>> = None;
        for _ in 0..REVERSE_SEEK_BOUND {
            if !self.write_cursor.as_mut().unwrap().valid() {
                return self.get_value(user_key, lastest_version.0, lastest_version.1);
            }

            let mut write = {
                let (commit_ts, key) = {
                    let w_cur = self.write_cursor.as_ref().unwrap();
                    last_handled_key = Some(w_cur.key().to_vec());
                    let w_key = Key::from_encoded(w_cur.key().to_vec());
                    (w_key.decode_ts()?, w_key.truncate_ts()?)
                };

                // reach neighbour user key or can't see this version.
                if ts < commit_ts || &key != user_key {
                    assert!(&key <= user_key);
                    return self.get_value(user_key, lastest_version.0, lastest_version.1);
                }
                self.statistics.write.processed += 1;
                Write::parse(self.write_cursor.as_ref().unwrap().value())?
            };

            match write.write_type {
                WriteType::Put => {
                    if write.short_value.is_some() {
                        if self.key_only {
                            lastest_version = (None, Some(vec![]));
                        } else {
                            lastest_version = (None, write.short_value.take());
                        }
                    } else {
                        lastest_version = (Some(write.start_ts), None);
                    }
                }
                WriteType::Delete => lastest_version = (None, None),
                WriteType::Lock | WriteType::Rollback => {}
            }
            self.write_cursor
                .as_mut()
                .unwrap()
                .prev(&mut self.statistics.write);
        }

        // After several prev, we still not get the latest version for the specified ts,
        // use seek to locate the latest version.
        let key = user_key.append_ts(ts);
        let valid = self
            .write_cursor
            .as_mut()
            .unwrap()
            .internal_seek(&key, &mut self.statistics.write)?;
        assert!(valid);
        loop {
            let mut write = {
                // If we reach the last handled key, it means we have checked all versions
                // for this user key.
                if self.write_cursor.as_ref().unwrap().key()
                    >= last_handled_key.as_ref().unwrap().as_slice()
                {
                    return self.get_value(user_key, lastest_version.0, lastest_version.1);
                }

                let w_cur = self.write_cursor.as_ref().unwrap();
                let w_key = Key::from_encoded(w_cur.key().to_vec());
                let commit_ts = w_key.decode_ts()?;
                assert!(commit_ts <= ts);
                let key = w_key.truncate_ts()?;
                assert_eq!(&key, user_key);
                self.statistics.write.processed += 1;
                Write::parse(w_cur.value())?
            };

            match write.write_type {
                WriteType::Put => {
                    if write.short_value.is_some() {
                        if self.key_only {
                            return Ok(Some(vec![]));
                        } else {
                            return Ok(write.short_value.take());
                        }
                    } else {
                        return Ok(Some(self.load_data(user_key, write.start_ts)?));
                    }
                }
                WriteType::Delete => return Ok(None),
                WriteType::Lock | WriteType::Rollback => {
                    let w_cur = self.write_cursor.as_mut().unwrap();
                    assert!(w_cur.next(&mut self.statistics.write));
                }
            }
        }
    }

    fn get_value(
        &mut self,
        user_key: &Key,
        start_ts: Option<u64>,
        short_value: Option<Vec<u8>>,
    ) -> Result<Option<Value>> {
        if let Some(ts) = start_ts {
            Ok(Some(self.load_data(user_key, ts)?))
        } else {
            Ok(short_value)
        }
    }

    #[allow(type_complexity)]
    pub fn scan_lock<F>(
        &mut self,
        start: Option<Key>,
        filter: F,
        limit: usize,
    ) -> Result<(Vec<(Key, Lock)>, Option<Key>)>
    where
        F: Fn(&Lock) -> bool,
    {
        self.create_lock_cursor()?;
        let cursor = self.lock_cursor.as_mut().unwrap();
        let ok = match start {
            Some(ref x) => cursor.seek(x, &mut self.statistics.lock)?,
            None => cursor.seek_to_first(&mut self.statistics.lock),
        };
        if !ok {
            return Ok((vec![], None));
        }
        let mut locks = vec![];
        while cursor.valid() {
            let key = Key::from_encoded(cursor.key().to_vec());
            let lock = Lock::parse(cursor.value())?;
            if filter(&lock) {
                locks.push((key.clone(), lock));
                if limit > 0 && locks.len() >= limit {
                    return Ok((locks, Some(key)));
                }
            }
            cursor.next(&mut self.statistics.lock);
        }
        self.statistics.lock.processed += locks.len();
        Ok((locks, None))
    }

    pub fn scan_keys(
        &mut self,
        mut start: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let iter_opt = IterOption::new(None, None, self.fill_cache);
        let scan_mode = self.get_scan_mode(false);
        let mut cursor = self.snapshot.iter_cf(CF_WRITE, iter_opt, scan_mode)?;
        let mut keys = vec![];
        loop {
            let ok = match start {
                Some(ref x) => cursor.near_seek(x, &mut self.statistics.write)?,
                None => cursor.seek_to_first(&mut self.statistics.write),
            };
            if !ok {
                return Ok((keys, None));
            }
            if keys.len() >= limit {
                self.statistics.write.processed += keys.len();
                return Ok((keys, start));
            }
            let key = Key::from_encoded(cursor.key().to_vec()).truncate_ts()?;
            start = Some(key.append_ts(0));
            keys.push(key);
        }
    }

    // Get all Value of the given key in CF_DEFAULT
    pub fn scan_values_in_default(&mut self, key: &Key) -> Result<Vec<(u64, Value)>> {
        self.create_data_cursor()?;
        let cursor = self.data_cursor.as_mut().unwrap();
        let mut ok = cursor.seek(key, &mut self.statistics.data)?;
        if !ok {
            return Ok(vec![]);
        }
        let mut v = vec![];
        while ok {
            let cur_key = Key::from_encoded(cursor.key().to_vec());
            let cur_key_without_ts = cur_key.truncate_ts()?;
            if cur_key_without_ts.encoded().as_slice() == key.encoded().as_slice() {
                v.push((cur_key.decode_ts()?, cursor.value().to_vec()));
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

        let props = match self.get_mvcc_properties(safe_point) {
            Some(v) => v,
            None => return true,
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

    fn get_mvcc_properties(&self, safe_point: u64) -> Option<MvccProperties> {
        let collection = match self.snapshot.get_properties_cf(CF_WRITE) {
            Ok(v) => v,
            Err(_) => return None,
        };
        if collection.is_empty() {
            return None;
        }
        // Aggregate MVCC properties.
        let mut props = MvccProperties::new();
        for (_, v) in &*collection {
            let mvcc = match MvccProperties::decode(v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => return None,
            };
            // Filter out properties after safe_point.
            if mvcc.min_ts > safe_point {
                continue;
            }
            props.add(&mvcc);
        }
        Some(props)
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::IsolationLevel;
    use kvproto::metapb::{Peer, Region};
    use raftstore::store::keys;
    use raftstore::store::RegionSnapshot;
    use rocksdb::{self, Writable, WriteBatch, DB};
    use std::sync::Arc;
    use std::u64;
    use storage::engine::{Modify, ScanMode};
    use storage::mvcc::{MvccReader, MvccTxn};
    use storage::{make_key, Mutation, Options, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use tempdir::TempDir;
    use util::properties::{MvccProperties, MvccPropertiesCollectorFactory};
    use util::rocksdb::{self as rocksdb_util, CFOptions};

    use super::REVERSE_SEEK_BOUND;

    struct RegionEngine {
        db: Arc<DB>,
        region: Region,
    }

    impl RegionEngine {
        pub fn new(db: Arc<DB>, region: Region) -> RegionEngine {
            RegionEngine {
                db: Arc::clone(&db),
                region,
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
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(Box::new(snap), start_ts, None, IsolationLevel::SI, true);
            txn.prewrite(m, pk, &Options::default()).unwrap();
            self.write(txn.into_modifies());
        }

        fn commit(&mut self, pk: &[u8], start_ts: u64, commit_ts: u64) {
            let k = make_key(pk);
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(Box::new(snap), start_ts, None, IsolationLevel::SI, true);
            txn.commit(&k, commit_ts).unwrap();
            self.write(txn.into_modifies());
        }

        fn rollback(&mut self, pk: &[u8], start_ts: u64) {
            let k = make_key(pk);
            let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
            let mut txn = MvccTxn::new(Box::new(snap), start_ts, None, IsolationLevel::SI, true);
            txn.rollback(&k).unwrap();
            self.write(txn.into_modifies());
        }

        fn gc(&mut self, pk: &[u8], safe_point: u64) {
            let k = make_key(pk);
            loop {
                let snap = RegionSnapshot::from_raw(Arc::clone(&self.db), self.region.clone());
                let mut txn =
                    MvccTxn::new(Box::new(snap), safe_point, None, IsolationLevel::SI, true);
                txn.gc(&k, safe_point).unwrap();
                let modifies = txn.into_modifies();
                if modifies.is_empty() {
                    return;
                }
                self.write(modifies);
            }
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
                    Modify::DeleteRange(cf, k1, k2) => {
                        let k1 = keys::data_key(k1.encoded());
                        let k2 = keys::data_key(k2.encoded());
                        let handle = rocksdb_util::get_cf_handle(db, cf).unwrap();
                        wb.delete_range_cf(handle, &k1, &k2).unwrap();
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
        let db_opts = rocksdb::DBOptions::new();
        let mut cf_opts = rocksdb::ColumnFamilyOptions::new();
        cf_opts.set_write_buffer_size(32 * 1024 * 1024);
        if with_properties {
            let f = Box::new(MvccPropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.test-collector", f);
        }
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_RAFT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_LOCK, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_WRITE, cf_opts),
        ];
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

    fn check_need_gc(
        db: Arc<DB>,
        region: Region,
        safe_point: u64,
        need_gc: bool,
    ) -> Option<MvccProperties> {
        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let reader = MvccReader::new(Box::new(snap), None, false, None, None, IsolationLevel::SI);
        assert_eq!(reader.need_gc(safe_point, 1.0), need_gc);
        reader.get_mvcc_properties(safe_point)
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
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Put 2 keys.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
    }

    #[allow(cyclomatic_complexity)]
    fn test_with_properties(path: &str, region: &Region) {
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Put 2 keys.
        engine.put(&[2], 3, 3);
        engine.put(&[3], 4, 4);
        engine.flush();
        // After this flush, we have a SST file w/ properties, plus the SST
        // file w/o properties from previous flush. We always need GC as
        // long as we can't get properties from any SST files.
        assert!(check_need_gc(Arc::clone(&db), region.clone(), 10, true).is_none());
        engine.compact();
        // After this compact, the two SST files are compacted into a new
        // SST file with properties. Now all SST files have properties and
        // all keys have only one version, so we don't need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // Put 2 more keys and delete them.
        engine.put(&[5], 5, 5);
        engine.put(&[6], 6, 6);
        engine.delete(&[5], 7, 7);
        engine.delete(&[6], 8, 8);
        engine.flush();
        // After this flush, keys 5,6 in the new SST file have more than one
        // versions, so we need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 8);
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 0, false).unwrap();
        assert_eq!(props.min_ts, u64::MAX);
        assert_eq!(props.max_ts, 0);
        assert_eq!(props.num_rows, 0);
        assert_eq!(props.num_puts, 0);
        assert_eq!(props.num_versions, 0);
        assert_eq!(props.max_row_versions, 0);

        // We gc the two deleted keys manually.
        engine.gc(&[5], 10);
        engine.gc(&[6], 10);
        engine.compact();
        // After this compact, all versions of keys 5,6 are deleted,
        // no keys have more than one versions, so we don't need gc.
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 4);
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props = check_need_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1);
        assert_eq!(props.max_ts, 9);
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }

    #[test]
    fn test_mvcc_reader_reverse_seek_many_tombstones() {
        let path =
            TempDir::new("_test_storage_mvcc_reader_reverse_seek_many_tombstones").expect("");
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Generate RocksDB tombstones in write cf.
        let start_ts = 1;
        let safe_point = 2;
        for i in 0..256 {
            for y in 0..256 {
                let pk = &[i as u8, y as u8];
                let m = Mutation::Put((make_key(pk), vec![]));
                engine.prewrite(m, pk, start_ts);
                engine.rollback(pk, start_ts);
                // Generate 65534 RocksDB tombstones between [0,0] and [255,255].
                if !((i == 0 && y == 0) || (i == 255 && y == 255)) {
                    engine.gc(pk, safe_point);
                }
            }
        }

        // Generate 256 locks in lock cf.
        let start_ts = 3;
        for i in 0..256 {
            let pk = &[i as u8];
            let m = Mutation::Put((make_key(pk), vec![]));
            engine.prewrite(m, pk, start_ts);
        }

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(
            Box::new(snap),
            Some(ScanMode::Backward),
            false,
            None,
            None,
            IsolationLevel::SI,
        );
        let row = &[255 as u8];
        let k = make_key(row);

        // Call reverse seek
        let ts = 2;
        assert_eq!(reader.reverse_seek(k, ts).unwrap(), None);
        let statistics = reader.get_statistics();
        assert_eq!(statistics.lock.prev, 256);
        assert_eq!(statistics.write.prev, 1);
    }

    #[test]
    fn test_mvcc_reader_reverse_seek_basic() {
        let path = TempDir::new("_test_storage_mvcc_reader_reverse_seek_basic").expect("");
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![], vec![]);
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(Arc::clone(&db), region.clone());

        // Generate REVERSE_SEEK_BOUND / 2 Put for key [10].
        let k = &[10 as u8];
        for ts in 0..REVERSE_SEEK_BOUND / 2 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            engine.commit(k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND + 1 Put for key [9].
        let k = &[9 as u8];
        for ts in 0..REVERSE_SEEK_BOUND + 1 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            engine.commit(k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND / 2 Put and REVERSE_SEEK_BOUND / 2 + 1 Rollback for key [8].
        let k = &[8 as u8];
        for ts in 0..REVERSE_SEEK_BOUND + 1 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            if ts < REVERSE_SEEK_BOUND / 2 {
                engine.commit(k, ts, ts);
            } else {
                engine.rollback(k, ts);
            }
        }

        // Generate REVERSE_SEEK_BOUND / 2 Put 1 delete and REVERSE_SEEK_BOUND/2 Rollback for key [7].
        let k = &[7 as u8];
        for ts in 0..REVERSE_SEEK_BOUND / 2 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            engine.commit(k, ts, ts);
        }
        {
            let ts = REVERSE_SEEK_BOUND / 2;
            let m = Mutation::Delete(make_key(k));
            engine.prewrite(m, k, ts);
            engine.commit(k, ts, ts);
        }
        for ts in REVERSE_SEEK_BOUND / 2 + 1..REVERSE_SEEK_BOUND + 1 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            engine.rollback(k, ts);
        }

        // Generate 1 PUT for key [6].
        let k = &[6 as u8];
        for ts in 0..1 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            engine.commit(k, ts, ts);
        }

        // Generate REVERSE_SEEK_BOUND + 1 Rollback for key [5].
        let k = &[5 as u8];
        for ts in 0..REVERSE_SEEK_BOUND + 1 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            engine.rollback(k, ts);
        }

        // Generate 1 PUT with ts = REVERSE_SEEK_BOUND and 1 PUT
        // with ts = REVERSE_SEEK_BOUND + 1 for key [4].
        let k = &[4 as u8];
        for ts in REVERSE_SEEK_BOUND..REVERSE_SEEK_BOUND + 2 {
            let m = Mutation::Put((make_key(k), vec![ts as u8]));
            engine.prewrite(m, k, ts);
            engine.commit(k, ts, ts);
        }

        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region.clone());
        let mut reader = MvccReader::new(
            Box::new(snap),
            Some(ScanMode::Backward),
            false,
            None,
            None,
            IsolationLevel::SI,
        );

        let ts = REVERSE_SEEK_BOUND;
        // Use REVERSE_SEEK_BOUND / 2 prev to get key [10].
        assert_eq!(
            reader.reverse_seek(make_key(&[11 as u8]), ts).unwrap(),
            Some((
                make_key(&[10 as u8]),
                vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8]
            ))
        );
        let mut total_prev = REVERSE_SEEK_BOUND as usize / 2;
        let mut total_seek = 0;
        let mut total_next = 0;
        assert_eq!(reader.get_statistics().write.prev, total_prev);
        assert_eq!(reader.get_statistics().write.seek, total_seek);
        assert_eq!(reader.get_statistics().write.next, total_next);
        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
        assert_eq!(reader.get_statistics().write.get, 0);

        // Use REVERSE_SEEK_BOUND prev and 1 seek to get key [9].
        // So the total prev += REVERSE_SEEK_BOUND, total seek = 1.
        assert_eq!(
            reader.reverse_seek(make_key(&[10 as u8]), ts).unwrap(),
            Some((make_key(&[9 as u8]), vec![REVERSE_SEEK_BOUND as u8]))
        );
        total_prev += REVERSE_SEEK_BOUND as usize;
        total_seek += 1;
        assert_eq!(reader.get_statistics().write.prev, total_prev);
        assert_eq!(reader.get_statistics().write.seek, total_seek);
        assert_eq!(reader.get_statistics().write.next, total_next);
        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
        assert_eq!(reader.get_statistics().write.get, 0);

        // Use REVERSE_SEEK_BOUND + 1 prev (1 in near_reverse_seek and REVERSE_SEEK_BOUND
        // in reverse_get_impl), 1 seek and 1 next to get key [8].
        // So the total prev += REVERSE_SEEK_BOUND + 1, total next += 1, total seek += 1.
        assert_eq!(
            reader.reverse_seek(make_key(&[9 as u8]), ts).unwrap(),
            Some((
                make_key(&[8 as u8]),
                vec![(REVERSE_SEEK_BOUND / 2 - 1) as u8]
            ))
        );
        total_prev += REVERSE_SEEK_BOUND as usize + 1;
        total_seek += 1;
        total_next += 1;
        assert_eq!(reader.get_statistics().write.prev, total_prev);
        assert_eq!(reader.get_statistics().write.seek, total_seek);
        assert_eq!(reader.get_statistics().write.next, total_next);
        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
        assert_eq!(reader.get_statistics().write.get, 0);

        // key [7] will cause REVERSE_SEEK_BOUND + 2 prev (2 in near_reverse_seek and
        // REVERSE_SEEK_BOUND in reverse_get_impl), 1 seek and 1 next and get DEL.
        // key [6] will cause 3 prev (2 in near_reverse_seek and 1 in reverse_get_impl).
        // So the total prev += REVERSE_SEEK_BOUND + 6, total next += 1, total seek += 1.
        assert_eq!(
            reader.reverse_seek(make_key(&[8 as u8]), ts).unwrap(),
            Some((make_key(&[6 as u8]), vec![0 as u8]))
        );
        total_prev += REVERSE_SEEK_BOUND as usize + 5;
        total_seek += 1;
        total_next += 1;
        assert_eq!(reader.get_statistics().write.prev, total_prev);
        assert_eq!(reader.get_statistics().write.seek, total_seek);
        assert_eq!(reader.get_statistics().write.next, total_next);
        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
        assert_eq!(reader.get_statistics().write.get, 0);

        // key [5] will cause REVERSE_SEEK_BOUND prev (REVERSE_SEEK_BOUND in reverse_get_impl)
        // and 1 seek but get none.
        // And then will call near_reverse_seek(key[5]) to fetch the previous key, this will cause
        // 2 prev in near_reverse_seek.
        // key [4] will cause 1 prev.
        // So the total prev += REVERSE_SEEK_BOUND + 3, total next += 1, total seek += 1.
        assert_eq!(
            reader.reverse_seek(make_key(&[6 as u8]), ts).unwrap(),
            Some((make_key(&[4 as u8]), vec![REVERSE_SEEK_BOUND as u8]))
        );
        total_prev += REVERSE_SEEK_BOUND as usize + 3;
        total_seek += 1;
        total_next += 1;
        assert_eq!(reader.get_statistics().write.prev, total_prev);
        assert_eq!(reader.get_statistics().write.seek, total_seek);
        assert_eq!(reader.get_statistics().write.next, total_next);
        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
        assert_eq!(reader.get_statistics().write.get, 0);

        // Use a prev and reach the very beginning.
        assert_eq!(reader.reverse_seek(make_key(&[4 as u8]), ts).unwrap(), None);
        total_prev += 1;
        assert_eq!(reader.get_statistics().write.prev, total_prev);
        assert_eq!(reader.get_statistics().write.seek, total_seek);
        assert_eq!(reader.get_statistics().write.next, total_next);
        assert_eq!(reader.get_statistics().write.seek_for_prev, 1);
        assert_eq!(reader.get_statistics().write.get, 0);
    }
}
