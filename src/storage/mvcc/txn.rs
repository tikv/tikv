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

use std::fmt;
use storage::{Key, Value, Mutation, CF_DEFAULT, CF_LOCK, CF_WRITE};
use storage::engine::{Snapshot, Modify, ScanMode};
use super::reader::MvccReader;
use super::lock::{LockType, Lock};
use super::write::{WriteType, Write};
use super::{Error, Result};
use super::metrics::*;

pub struct MvccTxn<'a> {
    reader: MvccReader<'a>,
    start_ts: u64,
    writes: Vec<Modify>,
}

impl<'a> fmt::Debug for MvccTxn<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "txn @{}", self.start_ts)
    }
}

impl<'a> MvccTxn<'a> {
    pub fn new(snapshot: &'a Snapshot, start_ts: u64, mode: Option<ScanMode>) -> MvccTxn<'a> {
        MvccTxn {
            reader: MvccReader::new(snapshot, mode),
            start_ts: start_ts,
            writes: vec![],
        }
    }

    pub fn modifies(self) -> Vec<Modify> {
        self.writes
    }

    fn lock_key(&mut self, key: Key, lock_type: LockType, primary: Vec<u8>, ttl: u64) {
        let lock = Lock::new(lock_type, primary, self.start_ts, ttl);
        self.writes.push(Modify::Put(CF_LOCK, key, lock.to_bytes()));
    }

    fn unlock_key(&mut self, key: Key) {
        self.writes.push(Modify::Delete(CF_LOCK, key));
    }

    pub fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.reader.get(key, self.start_ts)
    }

    pub fn prewrite(&mut self, mutation: Mutation, primary: &[u8], lock_ttl: u64) -> Result<()> {
        let key = mutation.key();
        if let Some((commit, _)) = try!(self.reader.seek_write(&key, u64::max_value())) {
            // Abort on writes after our start timestamp ...
            if commit >= self.start_ts {
                return Err(Error::WriteConflict);
            }
        }
        // ... or locks at any timestamp.
        if let Some(lock) = try!(self.reader.load_lock(&key)) {
            if lock.ts != self.start_ts {
                return Err(Error::KeyIsLocked {
                    key: try!(key.raw()),
                    primary: lock.primary,
                    ts: lock.ts,
                    ttl: lock.ttl,
                });
            }
        }
        self.lock_key(key.clone(),
                      LockType::from_mutation(&mutation),
                      primary.to_vec(),
                      lock_ttl);

        if let Mutation::Put((_, ref value)) = mutation {
            let value_key = key.append_ts(self.start_ts);
            self.writes.push(Modify::Put(CF_DEFAULT, value_key, value.clone()));
        }
        Ok(())
    }

    pub fn commit(&mut self, key: &Key, commit_ts: u64) -> Result<()> {
        let lock_type = match try!(self.reader.load_lock(key)) {
            Some(ref lock) if lock.ts == self.start_ts => lock.lock_type,
            _ => {
                return match try!(self.reader.get_txn_commit_ts(key, self.start_ts)) {
                    // Committed by concurrent transaction.
                    Some(_) => Ok(()),
                    // Rollbacked by concurrent transaction.
                    None => {
                        info!("txn conflict (lock not found), key:{}, start_ts:{}, commit_ts:{}",
                              key,
                              self.start_ts,
                              commit_ts);
                        Err(Error::TxnLockNotFound)
                    }
                };
            }
        };
        let write = Write::new(WriteType::from_lock_type(lock_type), self.start_ts);
        self.writes.push(Modify::Put(CF_WRITE, key.append_ts(commit_ts), write.to_bytes()));
        self.unlock_key(key.clone());
        Ok(())
    }

    pub fn rollback(&mut self, key: &Key) -> Result<()> {
        match try!(self.reader.load_lock(key)) {
            Some(ref lock) if lock.ts == self.start_ts => {
                let data_key = key.append_ts(lock.ts);
                self.writes.push(Modify::Delete(CF_DEFAULT, data_key));
            }
            _ => {
                return match try!(self.reader.get_txn_commit_ts(key, self.start_ts)) {
                    // Already committed by concurrent transaction.
                    Some(ts) => {
                        info!("txn conflict (committed), key:{}, start_ts:{}, commit_ts:{}",
                              key,
                              self.start_ts,
                              ts);
                        Err(Error::Committed { commit_ts: ts })
                    }
                    // Rollbacked by concurrent transaction.
                    None => Ok(()),
                };
            }
        }
        self.writes.push(Modify::Put(CF_WRITE,
                                     key.append_ts(self.start_ts),
                                     Write::new(WriteType::Rollback, self.start_ts).to_bytes()));
        self.unlock_key(key.clone());
        Ok(())
    }

    pub fn gc(&mut self, key: &Key, safe_point: u64) -> Result<()> {
        let mut remove_older = false;
        let mut ts: u64 = u64::max_value();
        let mut versions = 0;
        let mut delete_versions = 0;
        while let Some((commit, write)) = try!(self.reader.seek_write(key, ts)) {
            if !remove_older {
                if commit <= safe_point {
                    // Set `remove_older` after we find the latest value.
                    match write.write_type {
                        WriteType::Put | WriteType::Delete => {
                            remove_older = true;
                        }
                        WriteType::Rollback | WriteType::Lock => {}
                    }

                    // Latest write before `safe_point` can be deleted if its type is Delete,
                    // Rollback or Lock.
                    match write.write_type {
                        WriteType::Delete | WriteType::Rollback | WriteType::Lock => {
                            self.writes.push(Modify::Delete(CF_WRITE, key.append_ts(commit)));
                            delete_versions += 1;
                        }
                        WriteType::Put => {}
                    }
                }
            } else {
                self.writes.push(Modify::Delete(CF_WRITE, key.append_ts(commit)));
                if write.write_type == WriteType::Put {
                    self.writes.push(Modify::Delete(CF_DEFAULT, key.append_ts(write.start_ts)));
                }
                delete_versions += 1;
            }
            ts = commit - 1;
            versions += 1;
        }
        MVCC_VERSIONS_HISTOGRAM.observe(versions as f64);
        if delete_versions > 0 {
            GC_DELETE_VERSIONS_HISTOGRAM.observe(delete_versions as f64);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::Context;
    use super::MvccTxn;
    use super::super::MvccReader;
    use super::super::write::{Write, WriteType};
    use storage::{make_key, Mutation, ALL_CFS, CF_WRITE, ScanMode};
    use storage::engine::{self, Engine, TEMP_DIR};

    #[test]
    fn test_mvcc_txn_read() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_get_none(engine.as_ref(), b"x", 1);

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_get_none(engine.as_ref(), b"x", 3);
        must_get_err(engine.as_ref(), b"x", 7);

        must_commit(engine.as_ref(), b"x", 5, 10);
        must_get_none(engine.as_ref(), b"x", 3);
        must_get_none(engine.as_ref(), b"x", 7);
        must_get(engine.as_ref(), b"x", 13, b"x5");

        must_prewrite_delete(engine.as_ref(), b"x", b"x", 15);
        must_commit(engine.as_ref(), b"x", 15, 20);
        must_get_none(engine.as_ref(), b"x", 3);
        must_get_none(engine.as_ref(), b"x", 7);
        must_get(engine.as_ref(), b"x", 13, b"x5");
        must_get(engine.as_ref(), b"x", 17, b"x5");
        must_get_none(engine.as_ref(), b"x", 23);
    }

    #[test]
    fn test_mvcc_txn_prewrite() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        // Key is locked.
        must_locked(engine.as_ref(), b"x", 5);
        // Retry prewrite.
        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        // Conflict.
        must_prewrite_lock_err(engine.as_ref(), b"x", b"x", 6);

        must_commit(engine.as_ref(), b"x", 5, 10);
        must_written(engine.as_ref(), b"x", 5, 10, WriteType::Put);
        // Write conflict.
        must_prewrite_lock_err(engine.as_ref(), b"x", b"x", 6);
        must_unlocked(engine.as_ref(), b"x");
        // Not conflict.
        must_prewrite_lock(engine.as_ref(), b"x", b"x", 12);
        must_locked(engine.as_ref(), b"x", 12);
        must_rollback(engine.as_ref(), b"x", 12);
        must_unlocked(engine.as_ref(), b"x");
        must_written(engine.as_ref(), b"x", 12, 12, WriteType::Rollback);
        // Cannot retry Prewrite after rollback.
        must_prewrite_lock_err(engine.as_ref(), b"x", b"x", 12);
        // Can prewrite after rollback.
        must_prewrite_delete(engine.as_ref(), b"x", b"x", 13);
        must_rollback(engine.as_ref(), b"x", 13);
        must_unlocked(engine.as_ref(), b"x");
    }

    #[test]
    fn test_mvcc_txn_commit_ok() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        must_prewrite_put(engine.as_ref(), b"x", b"x10", b"x", 10);
        must_prewrite_lock(engine.as_ref(), b"y", b"x", 10);
        must_prewrite_delete(engine.as_ref(), b"z", b"x", 10);
        must_locked(engine.as_ref(), b"x", 10);
        must_locked(engine.as_ref(), b"y", 10);
        must_locked(engine.as_ref(), b"z", 10);
        must_commit(engine.as_ref(), b"x", 10, 15);
        must_commit(engine.as_ref(), b"y", 10, 15);
        must_commit(engine.as_ref(), b"z", 10, 15);
        must_written(engine.as_ref(), b"x", 10, 15, WriteType::Put);
        must_written(engine.as_ref(), b"y", 10, 15, WriteType::Lock);
        must_written(engine.as_ref(), b"z", 10, 15, WriteType::Delete);
        // commit should be idempotent
        must_commit(engine.as_ref(), b"x", 10, 15);
        must_commit(engine.as_ref(), b"y", 10, 15);
        must_commit(engine.as_ref(), b"z", 10, 15);
    }

    #[test]
    fn test_mvcc_txn_commit_err() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        // Not prewrite yet
        must_commit_err(engine.as_ref(), b"x", 1, 2);
        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        // start_ts not match
        must_commit_err(engine.as_ref(), b"x", 4, 5);
        must_rollback(engine.as_ref(), b"x", 5);
        // commit after rollback
        must_commit_err(engine.as_ref(), b"x", 5, 6);
    }

    #[test]
    fn test_mvcc_txn_rollback() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_rollback(engine.as_ref(), b"x", 5);
        // rollback should be idempotent
        must_rollback(engine.as_ref(), b"x", 5);
        // lock should be released after rollback
        must_unlocked(engine.as_ref(), b"x");
        must_prewrite_lock(engine.as_ref(), b"x", b"x", 10);
        must_rollback(engine.as_ref(), b"x", 10);
        // data should be dropped after rollback
        must_get_none(engine.as_ref(), b"x", 20);
    }

    #[test]
    fn test_mvcc_txn_rollback_err() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit(engine.as_ref(), b"x", 5, 10);
        must_rollback_err(engine.as_ref(), b"x", 5);
        must_rollback_err(engine.as_ref(), b"x", 5);
    }

    #[test]
    fn test_gc() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit(engine.as_ref(), b"x", 5, 10);
        must_prewrite_put(engine.as_ref(), b"x", b"x15", b"x", 15);
        must_commit(engine.as_ref(), b"x", 15, 20);
        must_prewrite_delete(engine.as_ref(), b"x", b"x", 25);
        must_commit(engine.as_ref(), b"x", 25, 30);
        must_prewrite_put(engine.as_ref(), b"x", b"x35", b"x", 35);
        must_commit(engine.as_ref(), b"x", 35, 40);
        must_prewrite_lock(engine.as_ref(), b"x", b"x", 45);
        must_commit(engine.as_ref(), b"x", 45, 50);
        must_prewrite_put(engine.as_ref(), b"x", b"x55", b"x", 55);
        must_rollback(engine.as_ref(), b"x", 55);

        // Transactions:
        // startTS commitTS Command
        // --
        // 55      -        PUT "x55" (Rollback)
        // 45      50       LOCK
        // 35      40       PUT "x35"
        // 25      30       DELETE
        // 15      20       PUT "x15"
        //  5      10       PUT "x5"

        // CF data layout:
        // ts CFDefault   CFWrite
        // --
        // 55             Rollback(PUT,50)
        // 50             Commit(LOCK,45)
        // 45
        // 40             Commit(PUT,35)
        // 35   x35
        // 30             Commit(Delete,25)
        // 25
        // 20             Commit(PUT,15)
        // 15   x15
        // 10             Commit(PUT,5)
        // 5    x5

        must_gc(engine.as_ref(), b"x", 12);
        must_get(engine.as_ref(), b"x", 12, b"x5");

        must_gc(engine.as_ref(), b"x", 22);
        must_get(engine.as_ref(), b"x", 22, b"x15");
        must_get_none(engine.as_ref(), b"x", 12);

        must_gc(engine.as_ref(), b"x", 32);
        must_get_none(engine.as_ref(), b"x", 22);
        must_get_none(engine.as_ref(), b"x", 35);

        must_gc(engine.as_ref(), b"x", 60);
        must_get(engine.as_ref(), b"x", 62, b"x35");
    }

    #[test]
    fn test_write() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_seek_write_none(engine.as_ref(), b"x", 5);

        must_commit(engine.as_ref(), b"x", 5, 10);
        must_seek_write(engine.as_ref(),
                        b"x",
                        u64::max_value(),
                        5,
                        10,
                        WriteType::Put);
        must_reverse_seek_write(engine.as_ref(), b"x", 5, 5, 10, WriteType::Put);
        must_seek_write_none(engine.as_ref(), b"a", u64::max_value());
        must_reverse_seek_write_none(engine.as_ref(), b"y", 5);
        must_get_commit_ts(engine.as_ref(), b"x", 5, 10);

        must_prewrite_delete(engine.as_ref(), b"x", b"x", 15);
        must_rollback(engine.as_ref(), b"x", 15);
        must_seek_write(engine.as_ref(),
                        b"x",
                        u64::max_value(),
                        15,
                        15,
                        WriteType::Rollback);
        must_reverse_seek_write_none(engine.as_ref(), b"x", 15);
        must_get_commit_ts(engine.as_ref(), b"x", 5, 10);
        must_get_commit_ts_none(engine.as_ref(), b"x", 15);

        must_prewrite_lock(engine.as_ref(), b"x", b"x", 25);
        must_commit(engine.as_ref(), b"x", 25, 30);
        must_seek_write(engine.as_ref(),
                        b"x",
                        u64::max_value(),
                        25,
                        30,
                        WriteType::Lock);
        must_reverse_seek_write(engine.as_ref(), b"x", 25, 25, 30, WriteType::Lock);
        must_get_commit_ts(engine.as_ref(), b"x", 25, 30);
    }

    #[test]
    fn test_scan_keys() {
        let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
        must_prewrite_put(engine.as_ref(), b"a", b"a", b"a", 1);
        must_commit(engine.as_ref(), b"a", 1, 10);
        must_prewrite_lock(engine.as_ref(), b"c", b"c", 1);
        must_commit(engine.as_ref(), b"c", 1, 5);
        must_prewrite_delete(engine.as_ref(), b"e", b"e", 1);
        must_commit(engine.as_ref(), b"e", 1, 20);
        must_prewrite_put(engine.as_ref(), b"b", b"b", b"b", 1);
        must_prewrite_lock(engine.as_ref(), b"d", b"d", 10);
        must_prewrite_delete(engine.as_ref(), b"f", b"f", 5);

        // Keys are ["a", "c", "e"].
        must_scan_keys(engine.as_ref(), None, 100, vec![b"a", b"c", b"e"], None);
        must_scan_keys(engine.as_ref(), None, 3, vec![b"a", b"c", b"e"], None);
        must_scan_keys(engine.as_ref(), None, 2, vec![b"a", b"c"], Some(b"c"));
        must_scan_keys(engine.as_ref(), Some(b"c"), 1, vec![b"c"], Some(b"c"));
    }

    fn must_get(engine: &Engine, key: &[u8], ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), ts, None);
        assert_eq!(txn.get(&make_key(key)).unwrap().unwrap(), expect);
    }

    fn must_get_none(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), ts, None);
        assert!(txn.get(&make_key(key)).unwrap().is_none());
    }

    fn must_get_err(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), ts, None);
        assert!(txn.get(&make_key(key)).is_err());
    }

    fn must_prewrite_put(engine: &Engine, key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), ts, None);
        txn.prewrite(Mutation::Put((make_key(key), value.to_vec())), pk, 0).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_prewrite_delete(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), ts, None);
        txn.prewrite(Mutation::Delete(make_key(key)), pk, 0).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_prewrite_lock(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), ts, None);
        txn.prewrite(Mutation::Lock(make_key(key)), pk, 0).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_prewrite_lock_err(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), ts, None);
        assert!(txn.prewrite(Mutation::Lock(make_key(key)), pk, 0).is_err());
    }

    fn must_commit(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), start_ts, None);
        txn.commit(&make_key(key), commit_ts).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_commit_err(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), start_ts, None);
        assert!(txn.commit(&make_key(key), commit_ts).is_err());
    }

    fn must_rollback(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), start_ts, None);
        txn.rollback(&make_key(key)).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_rollback_err(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), start_ts, None);
        assert!(txn.rollback(&make_key(key)).is_err());
    }

    fn must_gc(engine: &Engine, key: &[u8], safe_point: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), 0, None);
        txn.gc(&make_key(key), safe_point).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_locked(engine: &Engine, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        let lock = reader.load_lock(&make_key(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts);
    }

    fn must_unlocked(engine: &Engine, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        assert!(reader.load_lock(&make_key(key)).unwrap().is_none());
    }

    fn must_written(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64, tp: WriteType) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let k = make_key(key).append_ts(commit_ts);
        let v = snapshot.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = Write::parse(&v).unwrap();
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, tp);
    }

    fn must_seek_write_none(engine: &Engine, key: &[u8], ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        assert!(reader.seek_write(&make_key(key), ts).unwrap().is_none());
    }

    fn must_seek_write(engine: &Engine,
                       key: &[u8],
                       ts: u64,
                       start_ts: u64,
                       commit_ts: u64,
                       write_type: WriteType) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        let (t, write) = reader.seek_write(&make_key(key), ts).unwrap().unwrap();
        assert_eq!(t, commit_ts);
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, write_type);
    }

    fn must_reverse_seek_write_none(engine: &Engine, key: &[u8], ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        assert!(reader.reverse_seek_write(&make_key(key), ts).unwrap().is_none());
    }

    fn must_reverse_seek_write(engine: &Engine,
                               key: &[u8],
                               ts: u64,
                               start_ts: u64,
                               commit_ts: u64,
                               write_type: WriteType) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        let (t, write) = reader.reverse_seek_write(&make_key(key), ts).unwrap().unwrap();
        assert_eq!(t, commit_ts);
        assert_eq!(write.start_ts, start_ts);
        assert_eq!(write.write_type, write_type);
    }

    fn must_get_commit_ts(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        assert_eq!(reader.get_txn_commit_ts(&make_key(key), start_ts).unwrap().unwrap(),
                   commit_ts);
    }

    fn must_get_commit_ts_none(engine: &Engine, key: &[u8], start_ts: u64) {
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), None);
        assert!(reader.get_txn_commit_ts(&make_key(key), start_ts).unwrap().is_none());
    }

    fn must_scan_keys(engine: &Engine,
                      start: Option<&[u8]>,
                      limit: usize,
                      keys: Vec<&[u8]>,
                      next_start: Option<&[u8]>) {
        let expect = (keys.into_iter().map(make_key).collect(),
                      next_start.map(|x| make_key(x).append_ts(0)));
        let snapshot = engine.snapshot(&Context::new()).unwrap();
        let mut reader = MvccReader::new(snapshot.as_ref(), Some(ScanMode::Mixed));
        assert_eq!(reader.scan_keys(start.map(make_key), limit).unwrap(),
                   expect);
    }
}
