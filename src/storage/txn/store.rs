use storage::{Key, RefKey, Value, KvPair};
use storage::Engine;
use storage::mvcc::{MvccTxn, Prewrite};
use super::shard_lock::ShardLock;
use super::Result;

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

    pub fn prewrite(&self,
                    primary: Key,
                    puts: Vec<KvPair>,
                    deletes: Vec<Key>,
                    locks: Vec<Key>,
                    start_ts: u64)
                    -> Result<Vec<Key>> {
        let mut locked_keys = vec![];
        for &(ref k, _) in &puts {
            locked_keys.push(k.clone());
        }
        for k in &deletes {
            locked_keys.push(k.clone());
        }
        for k in &locks {
            locked_keys.push(k.clone());
        }

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
