use storage::{Key, RefKey, Value, KvPair};
use storage::Engine;
use storage::mvcc::{MvccTxn, Prewrite};
use super::shard_lock::ShardLock;
use super::{Error, Result};

pub struct TxnStore {
    engine: Box<Engine>,
    shard_lock: ShardLock,
}

const SHARD_LOCK_SIZE: usize = 256;

impl TxnStore {
    pub fn new(engine: Box<Engine>) -> TxnStore {
        TxnStore {
            engine: engine,
            shard_lock: ShardLock::new(SHARD_LOCK_SIZE),
        }
    }

    pub fn get(&self, key: RefKey, start_ts: u64) -> Result<Option<Value>> {
        let _guard = self.shard_lock.lock(&[key]);
        let txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        Ok(try!(txn.get(key)))
    }

    #[allow(dead_code)]
    pub fn batch_get(&self, keys: &[RefKey], start_ts: u64) -> Vec<Result<Option<Value>>> {
        let txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        let mut results = Vec::<_>::with_capacity(keys.len());
        for k in keys {
            let _guard = self.shard_lock.lock(keys);
            results.push(txn.get(k).map_err(Error::from));
        }
        results
    }

    pub fn scan(&self, key: RefKey, limit: usize, start_ts: u64) -> Result<Vec<Result<KvPair>>> {
        let mut results = vec![];
        let mut key = key.to_vec();
        let txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        while results.len() < limit {
            let mut next_key = match try!(self.engine.seek(&key)) {
                Some((key, _)) => key,
                None => break,
            };
            let _guard = self.shard_lock.lock(&next_key);
            match txn.get(&next_key) {
                Ok(Some(value)) => results.push(Ok((next_key.clone(), value))),
                Ok(None) => {}
                Err(e) => results.push(Err(Error::from(e))),
            };
            next_key.push(b'\0');
            key = next_key;
        }
        Ok(results)
    }

    pub fn prewrite(&self,
                    primary: Key,
                    puts: Vec<KvPair>,
                    deletes: Vec<Key>,
                    locks: Vec<Key>,
                    start_ts: u64)
                    -> Result<Vec<Key>> {
        let mut locked_keys = vec![];
        locked_keys.extend(puts.iter().map(|&(ref x, _)| x.clone()));
        locked_keys.extend(deletes.iter().cloned());
        locked_keys.extend(locks.iter().cloned());

        let _guard = self.shard_lock.lock(&locked_keys);
        let mut txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        for (k, v) in puts {
            try!(txn.prewrite(Prewrite::Put(k, v), &primary));
        }
        for k in deletes {
            try!(txn.prewrite(Prewrite::Delete(k), &primary));
        }
        for k in locks {
            try!(txn.prewrite(Prewrite::Lock(k), &primary));
        }
        try!(txn.commit());
        // TODO(disksing): rollback if error occurs
        Ok(locked_keys)
    }

    pub fn commit(&self, keys: Vec<Key>, start_ts: u64, commit_ts: u64) -> Result<()> {
        let _guard = self.shard_lock.lock(&keys);
        let mut txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        for k in keys {
            try!(txn.write(&k, commit_ts));
        }
        try!(txn.commit());
        Ok(())
    }
}
