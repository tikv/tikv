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
use std::collections::BTreeMap;
use storage::Key;
use storage::engine::{Engine, Snapshot, Modify, Cursor};
use kvproto::kvpb::{Row, Column, Op, Mutation};
use kvproto::mvccpb::{MetaLock, MetaItem, MetaColumn};
use kvproto::kvrpcpb::Context;
use super::meta::{Meta, FIRST_META_INDEX};
use super::{Error, Result};

trait MvccReader {
    fn read(&mut self, key: &Key) -> Result<Option<Vec<u8>>>;

    fn load_meta(&mut self, row: &Key, index: u64) -> Result<Meta> {
        let meta = match try!(self.read(&row.append_ts(index))) {
            Some(x) => try!(Meta::parse(&x)),
            None => Meta::new(),
        };
        Ok(meta)
    }

    fn get_row(&mut self, row: Vec<u8>, cols: Vec<Vec<u8>>, ts: u64) -> Result<Row> {
        let row_key = Key::from_raw(&row);
        self.get_row_impl(&row_key, row, cols, None, ts)
    }

    fn get_row_impl(&mut self,
                    row_key: &Key,
                    row_key_raw: Vec<u8>,
                    cols: Vec<Vec<u8>>,
                    first_meta: Option<Meta>,
                    ts: u64)
                    -> Result<Row> {
        // Load the first meta if not passed in.
        let mut meta = match first_meta {
            Some(x) => x,
            None => try!(self.load_meta(row_key, FIRST_META_INDEX)),
        };
        // Check for locks that signal concurrent writes.
        if let Some(lock) = meta.get_lock() {
            if lock.get_start_ts() <= ts {
                // There is a pending lock. Client should wait or clean it.
                return Err(Error::KeyIsLocked {
                    key: row_key_raw,
                    primary: lock.get_primary_key().to_vec(),
                    ts: lock.get_start_ts(),
                });
            }
        }
        let mut row = Row::new();
        row.set_row_key(row_key_raw.clone());
        let mut pending_cols = BTreeMap::new();
        for col in cols {
            pending_cols.insert(col.to_vec(), true);
        }
        loop {
            // Find the latest write below our start timestamp for each column.
            for item in meta.iter_items().filter(|x| x.get_commit_ts() <= ts) {
                if pending_cols.len() == 0 {
                    break;
                }
                for col in item.get_columns() {
                    if let Some(_) = pending_cols.remove(col.get_name()) {
                        if col.get_op() == Op::Put {
                            let value_key =
                                row_key.append_ts_column(item.get_start_ts(), col.get_name());
                            if let Some(x) = try!(self.read(&value_key)) {
                                let mut column = Column::new();
                                column.set_name(col.get_name().to_vec());
                                column.set_value(x);
                                row.mut_columns().push(column);
                            }
                        }
                    }
                }
            }
            if pending_cols.len() == 0 {
                break;
            }
            match meta.next_index() {
                Some(index) => {
                    meta = try!(self.load_meta(row_key, index));
                }
                None => break,
            }
        }
        Ok(row)
    }
}

pub struct MvccTxn<'a> {
    engine: &'a Engine,
    snapshot: MvccSnapshot<'a>,
    ctx: &'a Context,
    start_ts: u64,
    writes: Vec<Modify>,
}

impl<'a> fmt::Debug for MvccTxn<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "txn @{} - {:?}", self.start_ts, self.engine)
    }
}

impl<'a> MvccTxn<'a> {
    pub fn new(engine: &'a Engine,
               snapshot: &'a Snapshot,
               ctx: &'a Context,
               start_ts: u64)
               -> MvccTxn<'a> {
        MvccTxn {
            engine: engine,
            snapshot: MvccSnapshot::new(snapshot, start_ts),
            ctx: ctx,
            start_ts: start_ts,
            writes: vec![],
        }
    }

    pub fn submit(&mut self) -> Result<()> {
        let batch = self.writes.drain(..).collect();
        try!(self.engine.write(self.ctx, batch));
        Ok(())
    }

    fn write_meta(&mut self, row_key: &Key, meta: &mut Meta) {
        if let Some((split_meta, index)) = meta.split() {
            let modify = Modify::Put((row_key.append_ts(index), split_meta.to_bytes()));
            self.writes.push(modify);
        }
        let modify = Modify::Put((row_key.append_ts(FIRST_META_INDEX), meta.to_bytes()));
        self.writes.push(modify);
    }

    pub fn get(&mut self, row: Vec<u8>, cols: Vec<Vec<u8>>) -> Result<Row> {
        let ts = self.start_ts;
        self.get_row(row, cols, ts)
    }

    pub fn prewrite(&mut self, mut mutation: Mutation, primary: &[u8]) -> Result<()> {
        let row_key = Key::from_raw(mutation.get_row_key());
        let mut meta = try!(self.snapshot.load_meta(&row_key, FIRST_META_INDEX));
        // Abort on writes after our start timestamp ...
        if let Some(latest) = meta.iter_items().nth(0) {
            if latest.get_commit_ts() >= self.start_ts {
                return Err(Error::WriteConflict);
            }
        }
        // ... or locks at any timestamp.
        if let Some(lock) = meta.get_lock() {
            if lock.get_start_ts() != self.start_ts {
                return Err(Error::KeyIsLocked {
                    key: mutation.get_row_key().to_vec(),
                    primary: lock.get_primary_key().to_vec(),
                    ts: lock.get_start_ts(),
                });
            }
            // If we have processed a Mutation with the same ts before, simply believe they are the
            // same.
            // TODO: Be serious, check if they are equal or try to merge them.
            return Ok(());
        }

        let mut lock = MetaLock::new();
        for (op, mut col) in mutation.take_ops()
            .into_iter()
            .zip(mutation.take_columns().into_iter()) {
            if op == Op::Put {
                let value_key = row_key.append_ts_column(self.start_ts, col.get_name());
                self.writes.push(Modify::Put((value_key, col.take_value())));
            }
            let mut column = MetaColumn::new();
            column.set_op(op);
            column.set_name(col.take_name());
            lock.mut_columns().push(column);
        }
        lock.set_primary_key(primary.to_vec());
        lock.set_start_ts(self.start_ts);
        meta.set_lock(lock);
        self.write_meta(&row_key, &mut meta);
        Ok(())
    }

    pub fn commit(&mut self, row: &[u8], commit_ts: u64) -> Result<()> {
        let row_key = Key::from_raw(row);
        let mut meta = try!(self.snapshot.load_meta(&row_key, FIRST_META_INDEX));
        try!(self.commit_impl(commit_ts, &mut meta));
        self.write_meta(&row_key, &mut meta);
        Ok(())
    }

    fn commit_impl(&mut self, commit_ts: u64, meta: &mut Meta) -> Result<()> {
        match meta.get_lock() {
            Some(lock) if lock.get_start_ts() == self.start_ts => {}
            _ => {
                return match meta.get_item_by_start_ts(self.start_ts) {
                    // Committed by concurrent transaction.
                    Some(_) => Ok(()),
                    // Rollbacked by concurrent transaction.
                    None => Err(Error::TxnLockNotFound),
                };
            }
        }
        let mut lock = meta.clear_lock();
        let mut item = MetaItem::new();
        item.set_start_ts(self.start_ts);
        item.set_commit_ts(commit_ts);
        for col in lock.take_columns().into_iter() {
            if col.get_op() == Op::Put || col.get_op() == Op::Del {
                item.mut_columns().push(col);
            }
        }
        if item.get_columns().len() > 0 {
            meta.push_item(item);
        }
        Ok(())
    }

    pub fn commit_then_get(&mut self,
                           row: Vec<u8>,
                           cols: Vec<Vec<u8>>,
                           commit_ts: u64,
                           get_ts: u64)
                           -> Result<Row> {
        let row_key = Key::from_raw(&row);
        let mut meta = try!(self.load_meta(&row_key, FIRST_META_INDEX));
        try!(self.commit_impl(commit_ts, &mut meta));
        let res = try!(self.get_row_impl(&row_key, row, cols, Some(meta.clone()), get_ts));
        self.write_meta(&row_key, &mut meta);
        Ok(res)
    }

    pub fn rollback(&mut self, row: &[u8]) -> Result<()> {
        let row_key = Key::from_raw(row);
        let mut meta = try!(self.load_meta(&row_key, FIRST_META_INDEX));
        try!(self.rollback_impl(&row_key, &mut meta));
        self.write_meta(&row_key, &mut meta);
        Ok(())
    }

    fn rollback_impl(&mut self, key: &Key, meta: &mut Meta) -> Result<()> {
        match meta.get_lock() {
            Some(lock) if lock.get_start_ts() == self.start_ts => {
                for col in lock.get_columns().to_vec() {
                    if col.get_op() == Op::Put {
                        let value_key = key.append_ts_column(self.start_ts, col.get_name());
                        self.writes.push(Modify::Delete(value_key));
                    }
                }
            }
            _ => {
                return match meta.get_item_by_start_ts(self.start_ts) {
                    // Already committed by concurrent transaction.
                    Some(lock) => Err(Error::AlreadyCommitted { commit_ts: lock.get_commit_ts() }),
                    // Rollbacked by concurrent transaction.
                    None => Ok(()),
                };
            }
        }
        meta.clear_lock();
        Ok(())
    }

    pub fn rollback_then_get(&mut self, row: Vec<u8>, cols: Vec<Vec<u8>>) -> Result<Row> {
        let row_key = Key::from_raw(&row);
        let mut meta = try!(self.load_meta(&row_key, FIRST_META_INDEX));
        try!(self.rollback_impl(&row_key, &mut meta));
        let ts = self.start_ts;
        let res = try!(self.get_row_impl(&row_key, row, cols, Some(meta.clone()), ts));
        self.write_meta(&row_key, &mut meta);
        Ok(res)
    }
}

impl<'a> MvccReader for MvccTxn<'a> {
    fn read(&mut self, key: &Key) -> Result<Option<Vec<u8>>> {
        self.snapshot.read(key)
    }
}

pub struct MvccSnapshot<'a> {
    snapshot: &'a Snapshot,
    start_ts: u64,
}

impl<'a> fmt::Debug for MvccSnapshot<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "snapshot txn @{}", self.start_ts)
    }
}

impl<'a> MvccReader for MvccSnapshot<'a> {
    fn read(&mut self, key: &Key) -> Result<Option<Vec<u8>>> {
        Ok(try!(self.snapshot.get(key)))
    }
}

impl<'a> MvccSnapshot<'a> {
    pub fn new(snapshot: &'a Snapshot, start_ts: u64) -> MvccSnapshot<'a> {
        MvccSnapshot {
            snapshot: snapshot,
            start_ts: start_ts,
        }
    }

    pub fn get(&mut self, row: Vec<u8>, cols: Vec<Vec<u8>>) -> Result<Row> {
        let ts = self.start_ts;
        self.get_row(row, cols, ts)
    }
}

pub struct MvccCursor<'a> {
    cursor: &'a mut Cursor,
    start_ts: u64,
}

impl<'a> MvccReader for MvccCursor<'a> {
    fn read(&mut self, key: &Key) -> Result<Option<Vec<u8>>> {
        Ok(try!(self.cursor.get(key)).map(|x| x.to_owned()))
    }
}

impl<'a> MvccCursor<'a> {
    pub fn new(cursor: &'a mut Cursor, start_ts: u64) -> MvccCursor {
        MvccCursor {
            cursor: cursor,
            start_ts: start_ts,
        }
    }

    pub fn get(&mut self, row: Vec<u8>, cols: Vec<Vec<u8>>) -> Result<Row> {
        let ts = self.start_ts;
        self.get_row(row, cols, ts)
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::Context;
    use storage::engine::{self, Engine, Dsn, TEMP_DIR};
    use storage::mvcc::*;
    use storage::Key;

    #[test]
    fn test_mvcc_txn_read() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();

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

        // insert bad format data
        engine.put(&Context::new(),
                 Key::from_raw(b"y").append_ts(0),
                 b"dummy".to_vec())
            .unwrap();
        must_get_err(engine.as_ref(), b"y", 100);
    }

    #[test]
    fn test_mvcc_txn_prewrite() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();

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
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();
        must_prewrite_put(engine.as_ref(), b"x", b"x10", b"x", 10);
        must_commit(engine.as_ref(), b"x", 10, 15);
        // commit should be idempotent
        must_commit(engine.as_ref(), b"x", 10, 15);
    }

    #[test]
    fn test_mvcc_txn_commit_err() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();

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
    fn test_mvcc_txn_commit_then_get() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit_then_get(engine.as_ref(), b"x", 5, 10, 15, b"x5");
        must_commit_then_get(engine.as_ref(), b"x", 5, 10, 15, b"x5");
        must_commit_then_get_err(engine.as_ref(), b"x", 25, 30, 35);
    }

    #[test]
    fn test_mvcc_txn_rollback() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();

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
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit(engine.as_ref(), b"x", 5, 10);
        must_rollback_err(engine.as_ref(), b"x", 5);
    }

    #[test]
    fn test_mvcc_txn_rollback_then_get() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit(engine.as_ref(), b"x", 5, 10);
        must_rollback_then_get_err(engine.as_ref(), b"x", 5);
        must_prewrite_put(engine.as_ref(), b"x", b"x15", b"x", 15);
        must_rollback_then_get(engine.as_ref(), b"x", 15, b"x5");
        must_rollback_then_get(engine.as_ref(), b"x", 15, b"x5");
    }

    fn to_fake_ts(ts: u64) -> u64 {
        TEST_TS_BASE + ts
    }

    #[test]
    fn test_mvcc_txn_meta_split() {
        let engine = engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap();
        for i in 1u64..300 {
            let val = format!("x{}", i);
            must_prewrite_put(engine.as_ref(), b"x", val.as_bytes(), b"x", 5 * i);
            must_commit(engine.as_ref(), b"x", 5 * i, 5 * i + 1)
        }
        must_get(engine.as_ref(), b"x", 9, b"x1");
        must_get_none(engine.as_ref(), b"x", 5);
    }

    fn must_get(engine: &Engine, key: &[u8], ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(ts));
        let row_value = txn.get(key.to_vec(), default_cols()).unwrap();
        assert_eq!(default_row_value(&row_value).unwrap(), expect);
    }

    fn must_get_none(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(ts));
        let row_value = txn.get(key.to_vec(), default_cols()).unwrap();
        assert!(default_row_value(&row_value).is_none());
    }

    fn must_get_err(engine: &Engine, key: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(ts));
        assert!(txn.get(key.to_vec(), default_cols()).is_err());
    }

    fn must_prewrite_put(engine: &Engine, key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(ts));
        txn.prewrite(default_put(key, value), pk).unwrap();
        txn.submit().unwrap();
    }

    fn must_prewrite_delete(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(ts));
        txn.prewrite(default_del(key), pk).unwrap();
        txn.submit().unwrap();
    }

    fn must_prewrite_lock(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(ts));
        txn.prewrite(default_lock(key), pk).unwrap();
        txn.submit().unwrap();
    }

    fn must_prewrite_lock_err(engine: &Engine, key: &[u8], pk: &[u8], ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(ts));
        assert!(txn.prewrite(default_lock(key), pk).is_err());
    }

    fn must_commit(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(start_ts));
        txn.commit(key, to_fake_ts(commit_ts)).unwrap();
        txn.submit().unwrap();
    }

    fn must_commit_err(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(start_ts));
        assert!(txn.commit(key, to_fake_ts(commit_ts)).is_err());
    }

    fn must_commit_then_get(engine: &Engine,
                            key: &[u8],
                            lock_ts: u64,
                            commit_ts: u64,
                            get_ts: u64,
                            expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(lock_ts));
        let row_value = txn.commit_then_get(key.to_vec(),
                             default_cols(),
                             to_fake_ts(commit_ts),
                             to_fake_ts(get_ts))
            .unwrap();
        txn.submit().unwrap();
        assert_eq!(default_row_value(&row_value).unwrap(), expect);
    }

    fn must_commit_then_get_err(engine: &Engine,
                                key: &[u8],
                                lock_ts: u64,
                                commit_ts: u64,
                                get_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(lock_ts));
        assert!(txn.commit_then_get(key.to_vec(),
                             default_cols(),
                             to_fake_ts(commit_ts),
                             to_fake_ts(get_ts))
            .is_err());
    }

    fn must_rollback(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(start_ts));
        txn.rollback(key).unwrap();
        txn.submit().unwrap();
    }

    fn must_rollback_err(engine: &Engine, key: &[u8], start_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(start_ts));
        assert!(txn.rollback(key).is_err());
    }

    fn must_rollback_then_get(engine: &Engine, key: &[u8], lock_ts: u64, expect: &[u8]) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(lock_ts));
        let row_value = txn.rollback_then_get(key.to_vec(), default_cols()).unwrap();
        txn.submit().unwrap();
        assert_eq!(default_row_value(&row_value).unwrap(), expect);
    }

    fn must_rollback_then_get_err(engine: &Engine, key: &[u8], lock_ts: u64) {
        let ctx = Context::new();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(engine, snapshot.as_ref(), &ctx, to_fake_ts(lock_ts));
        assert!(txn.rollback_then_get(key.to_vec(), default_cols()).is_err());
    }
}
