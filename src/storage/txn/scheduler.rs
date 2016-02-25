use std::collections::BTreeMap;

use storage::Engine;
use storage::{Command, Key, Value, KvPair};
use super::{Result, Error};
use super::store::TxnStore;

pub struct Scheduler {
    store: TxnStore,
    lock_keys: BTreeMap<u64, Vec<Key>>,
}

impl Scheduler {
    pub fn new(engine: Box<Engine>) -> Scheduler {
        Scheduler {
            store: TxnStore::new(engine),
            lock_keys: BTreeMap::new(),
        }
    }

    fn exec_get(&self, key: Key, start_ts: u64) -> Result<Option<Value>> {
        Ok(try!(self.store.get(&key, start_ts)))
    }

    fn exec_scan(&self,
                 start_key: Key,
                 limit: usize,
                 start_ts: u64)
                 -> Result<Vec<Result<KvPair>>> {
        Ok(try!(self.store.scan(&start_key, limit, start_ts)))
    }

    fn exec_prewrite(&mut self,
                     puts: Vec<KvPair>,
                     deletes: Vec<Key>,
                     locks: Vec<Key>,
                     start_ts: u64)
                     -> Result<()> {
        let primary = Self::primary_key(&puts, &deletes);
        let locked_keys = try!(self.store.prewrite(primary, puts, deletes, locks, start_ts));
        self.lock_keys.insert(start_ts, locked_keys);
        Ok(())
    }

    fn exec_commit(&mut self, start_ts: u64, commit_ts: u64) -> Result<()> {
        let keys = match self.lock_keys.remove(&start_ts) {
            Some(x) => x,
            None => return Err(Error::TxnNotFound),
        };
        try!(self.store.commit(keys, start_ts, commit_ts));
        Ok(())
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Get{key, start_ts, callback} => {
                callback(self.exec_get(key, start_ts).map_err(::storage::Error::from));
            }
            Command::Scan{start_key, limit, start_ts, callback} => {
                callback(match self.exec_scan(start_key, limit, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(::storage::Error::from(e)),
                });
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
