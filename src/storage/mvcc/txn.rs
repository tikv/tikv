use std::fmt;

use storage::{Key, RefKey, Value, Mutation};
use storage::engine::{Engine, Modify};
use proto::mvccpb::{MetaLock, MetaLockType, MetaItem};
use util::codec::bytes;
use super::meta::Meta;
use super::codec;
use super::{Error, Result};

fn meta_lock_type(mutation: &Mutation) -> MetaLockType {
    match *mutation {
        Mutation::Put(_) | Mutation::Delete(_) => MetaLockType::ReadWrite,
        Mutation::Lock(_) => MetaLockType::ReadOnly,
    }
}

pub struct MvccTxn<'a, T: Engine + ?Sized + 'a> {
    engine: &'a T,
    start_ts: u64,
    writes: Vec<Modify>,
}

impl<'a, T: Engine + ?Sized> fmt::Debug for MvccTxn<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "txn @{} - {:?}", self.start_ts, self.engine)
    }
}

impl<'a, T: Engine + ?Sized> MvccTxn<'a, T> {
    pub fn new(engine: &'a T, start_ts: u64) -> MvccTxn<'a, T> {
        MvccTxn {
            engine: engine,
            start_ts: start_ts,
            writes: vec![],
        }
    }

    fn load_meta(&self, key: RefKey) -> Result<(Key, Meta)> {
        let meta_key = bytes::encode_bytes(key);
        let meta = match try!(self.engine.get(&meta_key)) {
            Some(x) => try!(Meta::parse(&x)),
            None => Meta::new(),
        };
        Ok((meta_key, meta))
    }

    pub fn submit(&mut self) -> Result<()> {
        let batch = self.writes.drain(..).collect();
        try!(self.engine.write(batch));
        Ok(())
    }

    pub fn get(&self, key: RefKey) -> Result<Option<Value>> {
        let (_, meta) = try!(self.load_meta(key));
        self.get_impl(key, &meta, self.start_ts.to_owned())
    }

    fn get_impl(&self, key: RefKey, meta: &Meta, ts: u64) -> Result<Option<Value>> {
        // Check for locks that signal concurrent writes.
        if let Some(lock) = meta.get_lock() {
            if lock.get_start_ts() <= ts {
                // There is a pending lock. Client should wait or clean it.
                return Err(Error::KeyIsLocked {
                    key: key.to_owned(),
                    primary: lock.get_primary_key().to_vec(),
                    ts: lock.get_start_ts(),
                });
            }
        }
        // Find the latest write below our start timestamp.
        match meta.iter_items().find(|x| x.get_commit_ts() <= ts) {
            Some(x) => {
                let data_key = codec::encode_key(&key, x.get_start_ts());
                Ok(try!(self.engine.get(&data_key)))
            }
            None => Ok(None),
        }
    }

    pub fn prewrite(&mut self, mutation: Mutation, primary: RefKey) -> Result<()> {
        let key = mutation.key();
        let (meta_key, mut meta) = try!(self.load_meta(key));
        // Abort on writes after our start timestamp ...
        if let Some(latest) = meta.iter_items().nth(0) {
            if latest.get_commit_ts() >= self.start_ts {
                return Err(Error::WriteConflict);
            }
        }
        // ... or locks at any timestamp.
        if let Some(lock) = meta.get_lock() {
            return Err(Error::KeyIsLocked {
                key: key.to_owned(),
                primary: lock.get_primary_key().to_vec(),
                ts: lock.get_start_ts(),
            });
        }

        let mut lock = MetaLock::new();
        lock.set_field_type(meta_lock_type(&mutation));
        lock.set_primary_key(primary.to_vec());
        lock.set_start_ts(self.start_ts);
        meta.set_lock(lock);
        let modify = Modify::Put((meta_key.clone(), meta.to_bytes()));
        self.writes.push(modify);

        if let Mutation::Put((_, ref value)) = mutation {
            let value_key = codec::encode_key(key, self.start_ts);
            self.writes.push(Modify::Put((value_key, value.clone())));
        }
        Ok(())
    }

    pub fn commit(&mut self, key: RefKey, commit_ts: u64) -> Result<()> {
        let (meta_key, mut meta) = try!(self.load_meta(key));
        self.commit_impl(commit_ts, meta_key, &mut meta)
    }

    fn commit_impl(&mut self, commit_ts: u64, meta_key: Key, meta: &mut Meta) -> Result<()> {
        let lock_type = match meta.get_lock() {
            Some(lock) if lock.get_start_ts() == self.start_ts => lock.get_field_type(),
            _ => {
                return match meta.get_item_by_start_ts(self.start_ts) {
                    // Committed by concurrent transaction.
                    Some(_) => Ok(()),
                    // Rollbacked by concurrent transaction.
                    None => Err(Error::TxnLockNotFound),
                };
            }
        };
        if lock_type == MetaLockType::ReadWrite {
            let mut item = MetaItem::new();
            item.set_start_ts(self.start_ts);
            item.set_commit_ts(commit_ts);
            meta.push_item(item);
        }
        meta.clear_lock();
        let modify = Modify::Put((meta_key, meta.to_bytes()));
        self.writes.push(modify);
        Ok(())
    }

    pub fn commit_then_get(&mut self,
                           key: RefKey,
                           commit_ts: u64,
                           get_ts: u64)
                           -> Result<Option<Value>> {
        let (meta_key, mut meta) = try!(self.load_meta(key));
        try!(self.commit_impl(commit_ts, meta_key, &mut meta));
        self.get_impl(key, &meta, get_ts)
    }

    pub fn rollback(&mut self, key: RefKey) -> Result<()> {
        let (meta_key, mut meta) = try!(self.load_meta(key));
        self.rollback_impl(key, meta_key, &mut meta)
    }

    fn rollback_impl(&mut self, key: RefKey, meta_key: Key, meta: &mut Meta) -> Result<()> {
        match meta.get_lock() {
            Some(lock) if lock.get_start_ts() == self.start_ts => {
                let value_key = codec::encode_key(key, lock.get_start_ts());
                self.writes.push(Modify::Delete(value_key));
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
        let modify = Modify::Put((meta_key, meta.to_bytes()));
        self.writes.push(modify);
        Ok(())
    }

    pub fn rollback_then_get(&mut self, key: RefKey) -> Result<Option<Value>> {
        let (meta_key, mut meta) = try!(self.load_meta(key));
        try!(self.rollback_impl(key, meta_key, &mut meta));
        self.get_impl(key, &meta, self.start_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::MvccTxn;
    use storage::Mutation;
    use storage::engine::{self, Engine, Dsn};
    use util::codec::bytes;

    #[test]
    fn test_mvcc_txn_read() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();

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
        engine.put(bytes::encode_bytes(b"y"), b"dummy".to_vec()).unwrap();
        must_get_err(engine.as_ref(), b"y", 100);
    }

    #[test]
    fn test_mvcc_txn_prewrite() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();

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
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        must_prewrite_put(engine.as_ref(), b"x", b"x10", b"x", 10);
        must_commit(engine.as_ref(), b"x", 10, 15);
        // commit should be idempotent
        must_commit(engine.as_ref(), b"x", 10, 15);
    }

    #[test]
    fn test_mvcc_txn_commit_err() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();

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
        let engine = engine::new_engine(Dsn::Memory).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit_then_get(engine.as_ref(), b"x", 5, 10, 15, b"x5");
        must_commit_then_get(engine.as_ref(), b"x", 5, 10, 15, b"x5");
        must_commit_then_get_err(engine.as_ref(), b"x", 25, 30, 35);
    }

    #[test]
    fn test_mvcc_txn_rollback() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();

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
        let engine = engine::new_engine(Dsn::Memory).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit(engine.as_ref(), b"x", 5, 10);
        must_rollback_err(engine.as_ref(), b"x", 5);
    }

    #[test]
    fn test_mvcc_txn_rollback_then_get() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();

        must_prewrite_put(engine.as_ref(), b"x", b"x5", b"x", 5);
        must_commit(engine.as_ref(), b"x", 5, 10);
        must_rollback_then_get_err(engine.as_ref(), b"x", 5);
        must_prewrite_put(engine.as_ref(), b"x", b"x15", b"x", 15);
        must_rollback_then_get(engine.as_ref(), b"x", 15, b"x5");
        must_rollback_then_get(engine.as_ref(), b"x", 15, b"x5");
    }

    fn must_get<T: Engine + ?Sized>(engine: &T, key: &[u8], ts: u64, expect: &[u8]) {
        let txn = MvccTxn::new(engine, ts);
        assert_eq!(txn.get(key).unwrap().unwrap(), expect);
    }

    fn must_get_none<T: Engine + ?Sized>(engine: &T, key: &[u8], ts: u64) {
        let txn = MvccTxn::new(engine, ts);
        assert!(txn.get(key).unwrap().is_none());
    }

    fn must_get_err<T: Engine + ?Sized>(engine: &T, key: &[u8], ts: u64) {
        let txn = MvccTxn::new(engine, ts);
        assert!(txn.get(key).is_err());
    }

    fn must_prewrite_put<T: Engine + ?Sized>(engine: &T,
                                             key: &[u8],
                                             value: &[u8],
                                             pk: &[u8],
                                             ts: u64) {
        let mut txn = MvccTxn::new(engine, ts);
        txn.prewrite(Mutation::Put((key.to_vec(), value.to_vec())), pk).unwrap();
        txn.submit().unwrap();
    }

    fn must_prewrite_delete<T: Engine + ?Sized>(engine: &T, key: &[u8], pk: &[u8], ts: u64) {
        let mut txn = MvccTxn::new(engine, ts);
        txn.prewrite(Mutation::Delete(key.to_vec()), pk).unwrap();
        txn.submit().unwrap();
    }

    fn must_prewrite_lock<T: Engine + ?Sized>(engine: &T, key: &[u8], pk: &[u8], ts: u64) {
        let mut txn = MvccTxn::new(engine, ts);
        txn.prewrite(Mutation::Lock(key.to_vec()), pk).unwrap();
        txn.submit().unwrap();
    }

    fn must_prewrite_lock_err<T: Engine + ?Sized>(engine: &T, key: &[u8], pk: &[u8], ts: u64) {
        let mut txn = MvccTxn::new(engine, ts);
        assert!(txn.prewrite(Mutation::Lock(key.to_vec()), pk).is_err());
    }

    fn must_commit<T: Engine + ?Sized>(engine: &T, key: &[u8], start_ts: u64, commit_ts: u64) {
        let mut txn = MvccTxn::new(engine, start_ts);
        txn.commit(key, commit_ts).unwrap();
        txn.submit().unwrap();
    }

    fn must_commit_err<T: Engine + ?Sized>(engine: &T, key: &[u8], start_ts: u64, commit_ts: u64) {
        let mut txn = MvccTxn::new(engine, start_ts);
        assert!(txn.commit(key, commit_ts).is_err());
    }

    fn must_commit_then_get<T: Engine + ?Sized>(engine: &T,
                                                key: &[u8],
                                                lock_ts: u64,
                                                commit_ts: u64,
                                                get_ts: u64,
                                                expect: &[u8]) {
        let mut txn = MvccTxn::new(engine, lock_ts);
        assert_eq!(txn.commit_then_get(key, commit_ts, get_ts).unwrap().unwrap(),
                   expect);
        txn.submit().unwrap();
    }

    fn must_commit_then_get_err<T: Engine + ?Sized>(engine: &T,
                                                    key: &[u8],
                                                    lock_ts: u64,
                                                    commit_ts: u64,
                                                    get_ts: u64) {
        let mut txn = MvccTxn::new(engine, lock_ts);
        assert!(txn.commit_then_get(key, commit_ts, get_ts).is_err());
    }

    fn must_rollback<T: Engine + ?Sized>(engine: &T, key: &[u8], start_ts: u64) {
        let mut txn = MvccTxn::new(engine, start_ts);
        txn.rollback(key).unwrap();
        txn.submit().unwrap();
    }

    fn must_rollback_err<T: Engine + ?Sized>(engine: &T, key: &[u8], start_ts: u64) {
        let mut txn = MvccTxn::new(engine, start_ts);
        assert!(txn.rollback(key).is_err());
    }

    fn must_rollback_then_get<T: Engine + ?Sized>(engine: &T,
                                                  key: &[u8],
                                                  lock_ts: u64,
                                                  expect: &[u8]) {
        let mut txn = MvccTxn::new(engine, lock_ts);
        assert_eq!(txn.rollback_then_get(key).unwrap().unwrap(), expect);
        txn.submit().unwrap();
    }

    fn must_rollback_then_get_err<T: Engine + ?Sized>(engine: &T, key: &[u8], lock_ts: u64) {
        let mut txn = MvccTxn::new(engine, lock_ts);
        assert!(txn.rollback_then_get(key).is_err());
    }
}
