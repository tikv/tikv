use storage::Engine;
use storage::Command;
use super::store::TxnStore;

pub struct Scheduler {
    store: TxnStore,
}

impl Scheduler {
    pub fn new(engine: Box<Engine>) -> Scheduler {
        Scheduler { store: TxnStore::new(engine) }
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        debug!("scheduler::handle_cmd: {:?}", cmd);
        match cmd {
            Command::Get{key, start_ts, callback} => {
                callback(self.store.get(&key, start_ts).map_err(::storage::Error::from));
            }
            Command::Scan{start_key, limit, start_ts, callback} => {
                callback(match self.store.scan(&start_key, limit, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(::storage::Error::from(e)),
                });
            }
            Command::Prewrite{mutations, primary, start_ts, callback} => {
                callback(match self.store.prewrite(mutations, primary, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(::storage::Error::from(e)),
                });
            }
            Command::Commit{keys, lock_ts, commit_ts, callback} => {
                callback(self.store
                             .commit(keys, lock_ts, commit_ts)
                             .map_err(::storage::Error::from));
            }
            Command::CommitThenGet{key, lock_ts, commit_ts, get_ts, callback} => {
                callback(self.store
                             .commit_then_get(key, lock_ts, commit_ts, get_ts)
                             .map_err(::storage::Error::from));
            }
            Command::Cleanup{key, start_ts, callback} => {
                callback(self.store.clean_up(key, start_ts).map_err(::storage::Error::from));
            }
            Command::Rollback{keys, start_ts, callback} => {
                callback(self.store.rollback(keys, start_ts).map_err(::storage::Error::from));
            }
            Command::RollbackThenGet{key, lock_ts, callback} => {
                callback(self.store
                             .rollback_then_get(key, lock_ts)
                             .map_err(::storage::Error::from));
            }
        }
    }
}
