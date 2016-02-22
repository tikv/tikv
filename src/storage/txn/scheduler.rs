use std::collections::BTreeMap;

use storage::{Engine, MvccStore};
use storage::mvcc::Prewrite;
use storage::{Command, Key, Value, KvPair};
use super::{Result, Error};

pub struct Scheduler {
    store: MvccStore,
    lock_keys: BTreeMap<u64, Vec<Key>>,
}

impl Scheduler {
    pub fn new(engine: Box<Engine>) -> Scheduler {
        Scheduler {
            store: MvccStore::new(engine),
            lock_keys: BTreeMap::new(),
        }
    }

    fn exec_get(&self, key: Key, ts: u64) -> Result<Option<Value>> {
        let txn = try!(self.store.start_row_transaction(key));
        Ok(try!(txn.get(ts)))
    }

    fn exec_prewrite(&mut self,
                     puts: Vec<KvPair>,
                     deletes: Vec<Key>,
                     locks: Vec<Key>,
                     start_ts: u64)
                     -> Result<()> {
        let primary = Self::primary_key(&puts, &deletes, &locks);
        let mut locked_keys = Vec::<Key>::new();
        for (k, v) in puts {
            let mut txn = try!(self.store.start_row_transaction(k.clone()));
            try!(txn.prewrite(Prewrite::Put(v), primary.clone(), start_ts));
            locked_keys.push(k);
        }
        for k in deletes {
            let mut txn = try!(self.store.start_row_transaction(k.clone()));
            try!(txn.prewrite(Prewrite::Delete, primary.clone(), start_ts));
            locked_keys.push(k);
        }
        for k in locks {
            let mut txn = try!(self.store.start_row_transaction(k.clone()));
            try!(txn.prewrite(Prewrite::Lock, primary.clone(), start_ts));
            locked_keys.push(k);
        }
        self.lock_keys.insert(start_ts, locked_keys);
        Ok(())
    }

    fn exec_commit(&mut self, start_ts: u64, commit_ts: u64) -> Result<()> {
        let keys = match self.lock_keys.remove(&start_ts) {
            Some(x) => x,
            None => return Err(Error::TxnNotFound),
        };
        let (primary, secondaries) = match keys.split_first() {
            Some(x) => x,
            None => return Ok(()),
        };
        let mut txn = try!(self.store.start_row_transaction(primary.clone()));
        try!(txn.commit(start_ts, commit_ts));
        for k in secondaries {
            let mut txn = try!(self.store.start_row_transaction(k.clone()));
            try!(txn.commit(start_ts, commit_ts));
        }
        Ok(())
    }

    #[allow(unused_variables)]
    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Get{key, ts, callback} => {
                callback(self.exec_get(key, ts).map_err(::storage::Error::from));
            }
            Command::Scan{start_key, limit, ts, callback} => {
                unimplemented!();
            }
            Command::Prewrite{puts, deletes, locks, start_ts, callback} => {
                callback(self.exec_prewrite(puts, deletes, locks, start_ts)
                             .map_err(::storage::Error::from));
            }
            Command::Commit{start_ts, commit_ts, callback} => {
                callback(self.exec_commit(start_ts, commit_ts).map_err(::storage::Error::from));
            }
        }
    }

    fn primary_key(puts: &[KvPair], deletes: &[Key], locks: &[Key]) -> Key {
        if !puts.is_empty() {
            return puts[0].0.clone();
        }
        if !deletes.is_empty() {
            return deletes[0].clone();
        }
        if !locks.is_empty() {
            return locks[0].clone();
        }
        unreachable!();
    }
}
