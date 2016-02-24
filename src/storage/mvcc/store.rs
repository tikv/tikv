use std::sync::{Mutex, MutexGuard};
use std::hash::{Hash, SipHasher, Hasher};
use std::fmt;

use storage::{Key, RefKey, Value};
use storage::engine::{Engine, Modify};
use proto::mvccpb::{MetaLock, MetaLock_Type, MetaItem};
use util::codec::bytes;
use super::meta::Meta;
use super::codec;
use super::{Error, Result};

const SHARD_LOCK_SIZE: usize = 256;

fn shard_index(key: RefKey) -> usize {
    let mut s = SipHasher::new();
    key.hash(&mut s);
    (s.finish() as usize) % SHARD_LOCK_SIZE
}

#[derive(Debug)]
pub struct MvccStore {
    engine: Box<Engine>,
    shard_locks: Vec<Mutex<()>>,
}

impl MvccStore {
    pub fn new(engine: Box<Engine>) -> MvccStore {
        let mut locks = Vec::<_>::with_capacity(SHARD_LOCK_SIZE);
        for _ in 0..SHARD_LOCK_SIZE {
            locks.push(Mutex::new(()));
        }
        MvccStore {
            engine: engine,
            shard_locks: locks,
        }
    }

    pub fn start_row_transaction(&self, key: Key) -> Result<RowTxn> {
        let index = shard_index(&key);
        let guard = self.shard_locks[index].lock().unwrap();
        RowTxn::new(self.engine.as_ref(), key, guard)
    }

    pub fn get(&self, key: Key, ts: u64) -> Result<Option<Value>> {
        let txn = try!(self.start_row_transaction(key));
        txn.get(ts)
    }

    pub fn prewrite(&self, key: Key, args: Prewrite, primary: Key, ts: u64) -> Result<()> {
        let mut txn = try!(self.start_row_transaction(key));
        try!(txn.prewrite(args, primary, ts));
        try!(txn.write());
        Ok(())
    }

    pub fn commit(&self, key: Key, start_ts: u64, commit_ts: u64) -> Result<()> {
        let mut txn = try!(self.start_row_transaction(key));
        try!(txn.commit(start_ts, commit_ts));
        try!(txn.write());
        Ok(())
    }

    pub fn rollback(&self, key: Key, start_ts: u64) -> Result<()> {
        let mut txn = try!(self.start_row_transaction(key));
        try!(txn.rollback(start_ts));
        try!(txn.write());
        Ok(())
    }
}

#[allow(dead_code)]
pub struct RowTxn<'a> {
    engine: &'a Engine,
    row_key: Key,
    row_lock: MutexGuard<'a, ()>,
    meta_key: Key,
    meta: Meta,
    writes: Vec<Modify>,
}

impl<'a> fmt::Debug for RowTxn<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "txn: {:?} - {:?}", self.row_key, self.engine)
    }
}

impl<'a> RowTxn<'a> {
    fn new(engine: &'a Engine, row_key: Key, lock: MutexGuard<'a, ()>) -> Result<RowTxn<'a>> {
        let meta_key = bytes::encode_bytes(&row_key);
        let meta = match try!(engine.get(&meta_key)) {
            Some(x) => try!(Meta::parse(&x)),
            None => Meta::new(),
        };
        Ok(RowTxn {
            engine: engine,
            row_key: row_key,
            row_lock: lock,
            meta_key: meta_key,
            meta: meta,
            writes: vec![],
        })
    }

    pub fn write(&mut self) -> Result<()> {
        let batch = self.writes.drain(..).collect();
        try!(self.engine.write(batch));
        Ok(())
    }

    pub fn get(&self, ts: u64) -> Result<Option<Value>> {
        if let Some(lock) = self.meta.get_lock() {
            if lock.get_start_ts() <= ts {
                return Err(Error::KeyIsLocked {
                    primary: lock.get_primary_key().to_vec(),
                    ts: lock.get_start_ts(),
                });
            }
        }
        match self.meta.iter_items().find(|x| x.get_commit_ts() <= ts) {
            Some(x) => {
                let data_key = codec::encode_key(&self.row_key, x.get_start_ts());
                Ok(try!(self.engine.get(&data_key)))
            }
            None => Ok(None),
        }
    }

    pub fn prewrite(&mut self, args: Prewrite, primary: Key, ts: u64) -> Result<()> {
        if let Some(lock) = self.meta.get_lock() {
            return Err(Error::KeyIsLocked {
                primary: lock.get_primary_key().to_vec(),
                ts: lock.get_start_ts(),
            });
        }
        match self.meta.iter_items().nth(0) {
            Some(item) if item.get_commit_ts() >= ts => return Err(Error::WriteConflict),
            _ => {}
        }

        let mut lock = MetaLock::new();
        lock.set_field_type(match args {
            Prewrite::Lock => MetaLock_Type::ReadOnly,
            _ => MetaLock_Type::ReadWrite,
        });
        lock.set_primary_key(primary);
        lock.set_start_ts(ts);
        self.meta.set_lock(lock);
        let modify = Modify::Put((self.meta_key.clone(), self.meta_bytes()));
        self.writes.push(modify);

        if let Prewrite::Put(value) = args {
            let value_key = codec::encode_key(&self.row_key, ts);
            self.writes.push(Modify::Put((value_key, value)));
        }
        Ok(())
    }

    pub fn commit(&mut self, start_ts: u64, commit_ts: u64) -> Result<()> {
        let lock_type = match self.meta.get_lock() {
            Some(lock) if lock.get_start_ts() == start_ts => lock.get_field_type(),
            _ => return Err(Error::TxnAbortedWhileWorking),
        };
        if lock_type == MetaLock_Type::ReadWrite {
            let mut item = MetaItem::new();
            item.set_start_ts(start_ts);
            item.set_commit_ts(commit_ts);
            self.meta.push_item(item);
        }
        self.meta.clear_lock();
        let modify = Modify::Put((self.meta_key.clone(), self.meta_bytes()));
        self.writes.push(modify);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn rollback(&mut self, start_ts: u64) -> Result<()> {
        match self.meta.get_lock() {
            Some(lock) if lock.get_start_ts() == start_ts => {
                if lock.get_field_type() == MetaLock_Type::ReadWrite {
                    let value_key = codec::encode_key(&self.row_key, lock.get_start_ts());
                    self.writes.push(Modify::Delete(value_key));
                }
            }
            _ => return Err(Error::TxnAbortedWhileWorking),
        }
        self.meta.clear_lock();
        let modify = Modify::Put((self.meta_key.clone(), self.meta_bytes()));
        self.writes.push(modify);
        Ok(())
    }

    fn meta_bytes(&self) -> Value {
        let mut data = vec![];
        self.meta.write_to(&mut data);
        data
    }
}

#[derive(Debug)]
pub enum Prewrite {
    Put(Value),
    Delete,
    Lock,
}

#[cfg(test)]
mod tests {
    use super::{MvccStore, Prewrite};
    use storage::engine::{self, Engine, Dsn};

    #[test]
    fn test_mvcc_store_read() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = MvccStore::new(engine);

        must_get_none(&store, b"x", 1);

        must_prewrite_put(&store, b"x", b"x5", b"x", 5);
        must_get_none(&store, b"x", 3);
        must_get_err(&store, b"x", 7);

        must_commit(&store, b"x", 5, 10);
        must_get_none(&store, b"x", 3);
        must_get_none(&store, b"x", 7);
        must_get(&store, b"x", 13, b"x5");

        must_prewrite_delete(&store, b"x", b"x", 15);
        must_commit(&store, b"x", 15, 20);
        must_get_none(&store, b"x", 3);
        must_get_none(&store, b"x", 7);
        must_get(&store, b"x", 13, b"x5");
        must_get(&store, b"x", 17, b"x5");
        must_get_none(&store, b"x", 23);
    }

    #[test]
    fn test_mvcc_store_write() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = MvccStore::new(engine);

        must_prewrite_put(&store, b"x", b"x5", b"x", 5);
        must_prewrite_lock_err(&store, b"x", b"x", 6);
        must_commit(&store, b"x", 5, 10);
        must_prewrite_lock_err(&store, b"x", b"x", 6);
        must_prewrite_lock(&store, b"x", b"x", 12);
    }

    #[test]
    fn test_mvcc_store_rollback() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = MvccStore::new(engine);

        must_prewrite_put(&store, b"x", b"x5", b"x", 5);
        must_rollback(&store, b"x", 5);
        must_prewrite_lock(&store, b"x", b"x", 6);
    }

    fn must_get(store: &MvccStore, key: &[u8], ts: u64, expect: &[u8]) {
        assert_eq!(store.get(key.to_vec(), ts).unwrap().unwrap(), expect);
    }

    fn must_get_none(store: &MvccStore, key: &[u8], ts: u64) {
        assert!(store.get(key.to_vec(), ts).unwrap().is_none());
    }

    fn must_get_err(store: &MvccStore, key: &[u8], ts: u64) {
        assert!(store.get(key.to_vec(), ts).is_err());
    }

    fn must_prewrite_put(store: &MvccStore, key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
        store.prewrite(key.to_vec(), Prewrite::Put(value.to_vec()), pk.to_vec(), ts).unwrap();
    }

    fn must_prewrite_delete(store: &MvccStore, key: &[u8], pk: &[u8], ts: u64) {
        store.prewrite(key.to_vec(), Prewrite::Delete, pk.to_vec(), ts).unwrap();
    }

    fn must_prewrite_lock(store: &MvccStore, key: &[u8], pk: &[u8], ts: u64) {
        store.prewrite(key.to_vec(), Prewrite::Lock, pk.to_vec(), ts).unwrap();
    }

    fn must_prewrite_lock_err(store: &MvccStore, key: &[u8], pk: &[u8], ts: u64) {
        assert!(store.prewrite(key.to_vec(), Prewrite::Lock, pk.to_vec(), ts).is_err());
    }

    fn must_commit(store: &MvccStore, key: &[u8], start_ts: u64, commit_ts: u64) {
        store.commit(key.to_vec(), start_ts, commit_ts).unwrap();
    }

    fn must_rollback(store: &MvccStore, key: &[u8], start_ts: u64) {
        store.rollback(key.to_vec(), start_ts).unwrap();
    }
}
