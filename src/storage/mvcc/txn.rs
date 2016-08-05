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
use storage::engine::{Snapshot, Modify};
use util::codec::number::NumberEncoder;
use super::reader::MvccReader;
use super::lock::{LockType, Lock};
use super::{Error, Result};

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
    pub fn new(snapshot: &'a Snapshot, start_ts: u64) -> MvccTxn<'a> {
        MvccTxn {
            reader: MvccReader::new(snapshot),
            start_ts: start_ts,
            writes: vec![],
        }
    }

    pub fn modifies(&mut self) -> Vec<Modify> {
        self.writes.drain(..).collect()
    }

    fn lock_key(&mut self, key: Key, lock_type: LockType, primary: Vec<u8>) {
        let lock = Lock::new(lock_type, primary, self.start_ts);
        self.writes.push(Modify::Put(CF_LOCK, key, lock.to_bytes()));
    }

    fn unlock_key(&mut self, key: Key) {
        self.writes.push(Modify::Delete(CF_LOCK, key));
    }

    pub fn get(&mut self, key: &Key) -> Result<Option<Value>> {
        self.reader.get(key, self.start_ts)
    }

    pub fn prewrite(&mut self, mutation: Mutation, primary: &[u8]) -> Result<()> {
        let key = mutation.key();
        if let Some((_, commit)) = try!(self.reader.seek_write(&key, u64::max_value())) {
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
                });
            }
        }
        self.lock_key(key.clone(),
                      LockType::from_mutation(&mutation),
                      primary.to_vec());

        if let Mutation::Put((_, ref value)) = mutation {
            let value_key = key.append_ts(self.start_ts);
            self.writes.push(Modify::Put(CF_DEFAULT, value_key, value.clone()));
        }
        Ok(())
    }

    pub fn commit(&mut self, key: &Key, commit_ts: u64) -> Result<()> {
        let lock_type = match try!(self.reader.load_lock(key)) {
            Some(ref lock) if lock.ts == self.start_ts => lock.typ.clone(),
            _ => {
                return match try!(self.reader.get_txn_commit_ts(key, self.start_ts)) {
                    // Committed by concurrent transaction.
                    Some(_) => Ok(()),
                    // Rollbacked by concurrent transaction.
                    None => Err(Error::TxnLockNotFound),
                };
            }
        };
        match lock_type {
            LockType::Put | LockType::Delete => {
                let mut value = vec![];
                value.encode_var_u64(self.start_ts).unwrap();
                self.writes.push(Modify::Put(CF_WRITE, key.append_ts(commit_ts), value));
            }
            _ => {}
        }
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
                    Some(ts) => Err(Error::AlreadyCommitted { commit_ts: ts }),
                    // Rollbacked by concurrent transaction.
                    None => Ok(()),
                };
            }
        }
        self.unlock_key(key.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::Context;
    use super::MvccTxn;
    use storage::{make_key, Mutation, DEFAULT_CFS};
    use storage::engine::{self, Engine, Dsn, TEMP_DIR};
    use storage::mvcc::TEST_TS_BASE;

    #[test]
    fn test_mvcc_txn_read() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR), DEFAULT_CFS).unwrap();

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
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR), DEFAULT_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        // Key is locked.
        must_prewrite_lock_err(engine.as_ref(), b"x", b"x", 6);
        must_commit(engine.as_ref(), b"x", 5, 10);
        // Write conflict
        must_prewrite_lock_err(engine.as_ref(), b"x", b"x", 6);
        // Not conflict
        must_prewrite_lock(engine.as_ref(), b"x", b"x", 12);
        must_rollback(engine.as_ref(), b"x", 12);
        // Can prewrite after rollback
        must_prewrite_lock(engine.as_ref(), b"x", b"x", 13);
        must_rollback(engine.as_ref(), b"x", 13);
    }

    #[test]
    fn test_mvcc_txn_commit_ok() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR), DEFAULT_CFS).unwrap();
        must_prewrite_put(engine.as_ref(), b"x", b"x10", b"x", 10);
        must_commit(engine.as_ref(), b"x", 10, 15);
        // commit should be idempotent
        must_commit(engine.as_ref(), b"x", 10, 15);
    }

    #[test]
    fn test_mvcc_txn_commit_err() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR), DEFAULT_CFS).unwrap();

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
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR), DEFAULT_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_rollback(engine.as_ref(), b"x", 5);
        // rollback should be idempotent
        must_rollback(engine.as_ref(), b"x", 5);
        // lock should be released after rollback
        must_prewrite_lock(engine.as_ref(), b"x", b"x", 10);
        must_rollback(engine.as_ref(), b"x", 10);
        // data should be dropped after rollback
        must_get_none(engine.as_ref(), b"x", 20);
    }

    #[test]
    fn test_mvcc_txn_rollback_err() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR), DEFAULT_CFS).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit(engine.as_ref(), b"x", 5, 10);
        must_rollback_err(engine.as_ref(), b"x", 5);
    }

    fn to_fake_ts(ts: u64) -> u64 {
        TEST_TS_BASE + ts
    }

    fn must_get(engine: &Engine, key: &[u8], ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(ts));
        assert_eq!(txn.get(&make_key(key)).unwrap().unwrap(), expect);
    }

    fn must_get_none(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(ts));
        assert!(txn.get(&make_key(key)).unwrap().is_none());
    }

    fn must_get_err(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(ts));
        assert!(txn.get(&make_key(key)).is_err());
    }

    fn must_prewrite_put(engine: &Engine, key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(ts));
        txn.prewrite(Mutation::Put((make_key(key), value.to_vec())), pk).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_prewrite_delete(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(ts));
        txn.prewrite(Mutation::Delete(make_key(key)), pk).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_prewrite_lock(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(ts));
        txn.prewrite(Mutation::Lock(make_key(key)), pk).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_prewrite_lock_err(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(ts));
        assert!(txn.prewrite(Mutation::Lock(make_key(key)), pk).is_err());
    }

    fn must_commit(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(start_ts));
        txn.commit(&make_key(key), to_fake_ts(commit_ts)).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_commit_err(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(start_ts));
        assert!(txn.commit(&make_key(key), to_fake_ts(commit_ts)).is_err());
    }

    fn must_rollback(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(start_ts));
        txn.rollback(&make_key(key)).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
    }

    fn must_rollback_err(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot.as_ref(), to_fake_ts(start_ts));
        assert!(txn.rollback(&make_key(key)).is_err());
    }
}
