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

    fn exec_get(&self, key: Key, start_ts: u64) -> Result<Option<Value>> {
        Ok(try!(self.store.get(key, start_ts)))
    }

    fn exec_prewrite(&mut self,
                     puts: Vec<KvPair>,
                     deletes: Vec<Key>,
                     locks: Vec<Key>,
                     start_ts: u64)
                     -> Result<()> {
        let primary = Self::primary_key(&puts, &deletes);
        let mut locked_keys = Vec::<Key>::new();
        for (k, v) in puts {
            try!(self.store.prewrite(k.clone(), Prewrite::Put(v), primary.clone(), start_ts));
            locked_keys.push(k);
        }
        for k in deletes {
            try!(self.store.prewrite(k.clone(), Prewrite::Delete, primary.clone(), start_ts));
            locked_keys.push(k);
        }
        for k in locks {
            try!(self.store.prewrite(k.clone(), Prewrite::Lock, primary.clone(), start_ts));
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
        try!(self.store.commit(primary.clone(), start_ts, commit_ts));
        for k in secondaries {
            try!(self.store.commit(k.clone(), start_ts, commit_ts));
        }
        Ok(())
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Get{key, start_ts, callback} => {
                callback(self.exec_get(key, start_ts).map_err(::storage::Error::from));
            }
            Command::Scan{..} => {
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

    fn primary_key(puts: &[KvPair], deletes: &[Key]) -> Key {
        if !puts.is_empty() {
            return puts[0].0.clone();
        }
        if !deletes.is_empty() {
            return deletes[0].clone();
        }
        panic!("no puts or deletes");
    }
}
