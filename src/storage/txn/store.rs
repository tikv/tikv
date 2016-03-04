use util::codec::bytes;
use storage::{Key, RefKey, Value, KvPair, Mutation};
use storage::Engine;
use storage::mvcc::MvccTxn;
use super::shard_mutex::ShardMutex;
use super::{Error, Result};

pub struct TxnStore {
    engine: Box<Engine>,
    shard_mutex: ShardMutex,
}

const SHARD_MUTEX_SIZE: usize = 256;

impl TxnStore {
    pub fn new(engine: Box<Engine>) -> TxnStore {
        TxnStore {
            engine: engine,
            shard_mutex: ShardMutex::new(SHARD_MUTEX_SIZE),
        }
    }

    pub fn get(&self, key: RefKey, start_ts: u64) -> Result<Option<Value>> {
        let _guard = self.shard_mutex.lock(&[key]);
        let txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        Ok(try!(txn.get(key)))
    }

    #[allow(dead_code)]
    pub fn batch_get(&self, keys: &[RefKey], start_ts: u64) -> Vec<Result<Option<Value>>> {
        let txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        let mut results = Vec::<_>::with_capacity(keys.len());
        for k in keys {
            let _guard = self.shard_mutex.lock(keys);
            results.push(txn.get(k).map_err(Error::from));
        }
        results
    }

    pub fn scan(&self, key: RefKey, limit: usize, start_ts: u64) -> Result<Vec<Result<KvPair>>> {
        let mut results = vec![];
        let mut key = key.to_vec();
        let txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        while results.len() < limit {
            let mut next_key = match try!(self.engine.seek(&bytes::encode_bytes(&key))) {
                Some((key, _)) => {
                    let (key, _) = try!(bytes::decode_bytes(&key));
                    key
                }
                None => break,
            };
            let _guard = self.shard_mutex.lock(&next_key);
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

    pub fn prewrite(&self, writes: Vec<Mutation>, primary: Key, start_ts: u64) -> Result<Vec<Key>> {
        let locked_keys: Vec<Key> = writes.iter().map(|x| x.key().to_owned()).collect();

        let _guard = self.shard_mutex.lock(&locked_keys);
        let mut txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        for w in writes {
            try!(txn.prewrite(w, &primary));
        }
        try!(txn.submit());
        // TODO(disksing): rollback if error occurs
        Ok(locked_keys)
    }

    pub fn commit(&self, keys: Vec<Key>, start_ts: u64, commit_ts: u64) -> Result<()> {
        let _guard = self.shard_mutex.lock(&keys);
        let mut txn = MvccTxn::new(self.engine.as_ref(), start_ts);
        for k in keys {
            try!(txn.commit(&k, commit_ts));
        }
        try!(txn.submit());
        Ok(())
    }
}
