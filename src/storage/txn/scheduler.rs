use std::collections::BTreeMap;

use storage::Engine;
use storage::{Command, Key, Value, KvPair, Write};
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

    fn exec_prewrite(&mut self, writes: Vec<Write>, start_ts: u64) -> Result<()> {
        let primary = Self::primary_key(&writes);
        let locked_keys = try!(self.store.prewrite(writes, primary, start_ts));
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
            Command::Prewrite{writes, start_ts, callback} => {
                callback(self.exec_prewrite(writes, start_ts)
                             .map_err(::storage::Error::from));
            }
            Command::Commit{start_ts, commit_ts, callback} => {
                callback(self.exec_commit(start_ts, commit_ts).map_err(::storage::Error::from));
            }
        }
    }

    #[allow(match_same_arms)]
    fn primary_key(writes: &[Write]) -> Key {
        for w in writes {
            match *w {
                Write::Put((ref key, _)) => return key.to_vec(),
                Write::Delete(ref key) => return key.to_vec(),
                _ => {}
            }
        }
        panic!("no puts or deletes");
    }
}
