use std::collections::BTreeMap;

use storage::Engine;
use storage::{Command, Key, Value, KvPair, Mutation};
use super::{Result, Error};
use super::store::TxnStore;

pub struct Scheduler {
    store: TxnStore,
}

impl Scheduler {
    pub fn new(engine: Box<Engine>) -> Scheduler {
        Scheduler {
            store: TxnStore::new(engine),
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

    fn exec_prewrite(&mut self, mutations: Vec<Mutation>, start_ts: u64) -> Result<()> {
        let primary = Self::primary_key(&mutations);
        let locked_keys = try!(self.store.prewrite(mutations, primary, start_ts));
        self.lock_keys.insert(start_ts, locked_keys);
        Ok(())
    }

    fn exec_commit(&mut self, keys: Vec<Key>, commit_ts: u64) -> Result<()> {
        try!(self.store.commit(keys, start_ts, commit_ts));
        Ok(())
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        debug!("scheduler::handle_cmd: {:?}", cmd);
        match cmd {
            Command::Get{key, start_ts, callback} => {
                callback(
                    self.store.get(&key, start_ts).map_err(::storage::Error::from)
                );
            }
            Command::Scan{start_key, limit, start_ts, callback} => {
                callback(
                    match self.store.scan(&start_key, limit, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(::storage::Error::from(e)),
                });
            }
            Command::Prewrite{mutations, start_ts, callback} => {
                callback(self.exec_prewrite(mutations, start_ts)
                             .map_err(::storage::Error::from));
            }
            Command::Commit{start_ts, commit_ts, callback} => {
                callback(self.exec_commit(start_ts, commit_ts).map_err(::storage::Error::from));
            }
        }
    }
}
