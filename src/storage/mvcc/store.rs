use std::sync::{Mutex, MutexGuard};
use std::hash::{Hash, SipHasher, Hasher};
use std::fmt;

use storage::{Key, RefKey, Value};
use storage::engine::{Engine, Modify};
use proto::mvccpb::{MetaLock, MetaItem};
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

    pub fn start_row_transaction(&self, row_key: Key) -> Result<RowTxn> {
        let index = shard_index(&row_key);
        let guard = self.shard_locks[index].lock().unwrap();
        RowTxn::new(self.engine.as_ref(), row_key, guard)
    }
}

#[allow(dead_code)]
pub struct RowTxn<'a> {
    engine: &'a Engine,
    row_key: Key,
    row_lock: MutexGuard<'a, ()>,
    meta_key: Key,
    meta: Meta,
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
        })
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
        if let Some(item) = self.meta.iter_items().find(|x| x.get_commit_ts() <= ts) {
            if item.has_key_suffix() {
                let data_key = codec::encode_key(&self.row_key, item.get_key_suffix());
                let data = match try!(self.engine.get(&data_key)) {
                    Some(x) => x,
                    None => return Err(Error::DataMissing),
                };
                return Ok(Some(data));
            }
        }
        Ok(None)
    }

    pub fn prewrite(&mut self, args: Prewrite, primary: Key, ts: u64) -> Result<()> {
        if let Some(lock) = self.meta.get_lock() {
            return Err(Error::KeyIsLocked {
                primary: lock.get_primary_key().to_vec(),
                ts: lock.get_start_ts(),
            });
        }
        if self.meta.iter_items().any(|x| x.get_commit_ts() >= ts) {
            return Err(Error::WriteConflict);
        }

        let mut lock = MetaLock::new();
        lock.set_primary_key(primary);
        lock.set_start_ts(ts);

        match args {
            Prewrite::Put(value) => {
                let suffix = self.meta.alloc_suffix();
                let value_key = codec::encode_key(&self.row_key, suffix);
                lock.set_key_suffix(suffix);
                lock.set_read_only(false);
                self.meta.set_lock(lock);

                let batch = vec![Modify::Put((value_key, value)),
                                 Modify::Put((self.meta_key.clone(), self.meta_bytes()))];
                Ok(try!(self.engine.write(batch)))
            }
            Prewrite::Delete => {
                lock.set_read_only(false);
                self.meta.set_lock(lock);
                Ok(try!(self.engine.put(self.meta_key.clone(), self.meta_bytes())))
            }
            Prewrite::Lock => {
                lock.set_read_only(true);
                self.meta.set_lock(lock);
                Ok(try!(self.engine.put(self.meta_key.clone(), self.meta_bytes())))
            }
        }
    }

    pub fn commit(&mut self, start_ts: u64, commit_ts: u64) -> Result<()> {
        match self.meta.get_lock() {
            Some(lock) if lock.get_start_ts() == start_ts => {}
            _ => return Err(Error::TxnAbortedWhileWorking),
        }
        let meta_item = {
            let lock = self.meta.get_lock().unwrap();
            let mut item = MetaItem::new();
            item.set_start_ts(lock.get_start_ts());
            item.set_commit_ts(commit_ts);
            if lock.has_key_suffix() {
                item.set_key_suffix(lock.get_key_suffix());
            }
            item
        };
        self.meta.push_item(meta_item);
        self.meta.clear_lock();
        Ok(try!(self.engine.put(self.meta_key.clone(), self.meta_bytes())))
    }

    #[allow(dead_code)]
    pub fn rollback(&mut self) -> Result<()> {
        let mut batch = vec![];
        {
            let lock = match self.meta.get_lock() {
                Some(x) => x,
                None => return Err(Error::TxnAbortedWhileWorking),
            };
            if !lock.get_read_only() && lock.has_key_suffix() {
                let value_key = codec::encode_key(&self.row_key, lock.get_key_suffix());
                batch.push(Modify::Delete(value_key));
            }
        }
        self.meta.clear_lock();
        batch.push(Modify::Put((self.meta_key.clone(), self.meta_bytes())));
        Ok(try!(self.engine.write(batch)))
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

        {
            let txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            assert!(txn.get(1).unwrap().is_none());
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.prewrite(Prewrite::Put(b"x5".to_vec()), b"x".to_vec(), 5).unwrap();
        }
        {
            let txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            assert!(txn.get(3).unwrap().is_none());
            assert!(txn.get(7).is_err());
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.commit(5, 10).unwrap();
        }
        {
            let txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            assert!(txn.get(3).unwrap().is_none());
            assert!(txn.get(7).unwrap().is_none());
            assert_eq!(txn.get(13).unwrap().unwrap(), b"x5");
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.prewrite(Prewrite::Delete, b"x".to_vec(), 15).unwrap();
            txn.commit(15, 20).unwrap();
        }
        {
            let txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            assert!(txn.get(3).unwrap().is_none());
            assert!(txn.get(7).unwrap().is_none());
            assert_eq!(txn.get(13).unwrap().unwrap(), b"x5");
            assert_eq!(txn.get(17).unwrap().unwrap(), b"x5");
            assert!(txn.get(23).unwrap().is_none());
        }
    }

    #[test]
    fn test_mvcc_store_write() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = MvccStore::new(engine);

        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.prewrite(Prewrite::Put(b"x5".to_vec()), b"x".to_vec(), 5).unwrap();
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            assert!(txn.prewrite(Prewrite::Lock, b"x".to_vec(), 6).is_err());
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.commit(5, 10).unwrap();
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            assert!(txn.prewrite(Prewrite::Lock, b"x".to_vec(), 6).is_err());
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.prewrite(Prewrite::Lock, b"x".to_vec(), 12).unwrap();
        }
    }

    #[test]
    fn test_mvcc_store_rollback() {
        let engine = engine::new_engine(Dsn::Memory).unwrap();
        let store = MvccStore::new(engine);

        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.prewrite(Prewrite::Put(b"x5".to_vec()), b"x".to_vec(), 5).unwrap();
            txn.rollback().unwrap();
        }
        {
            let mut txn = store.start_row_transaction(b"x".to_vec()).unwrap();
            txn.prewrite(Prewrite::Lock, b"x".to_vec(), 6).unwrap();
        }
    }
}
