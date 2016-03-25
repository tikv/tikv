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
            Command::Get{ctx, key, start_ts, callback} => {
                callback(self.store.get(ctx, &key, start_ts).map_err(::storage::Error::from));
            }
            Command::Scan{ctx, start_key, limit, start_ts, callback} => {
                callback(match self.store.scan(ctx, start_key, limit, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(e.into()),
                });
            }
            Command::Prewrite{ctx, mutations, primary, start_ts, callback} => {
                callback(match self.store.prewrite(ctx, mutations, primary, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(e.into()),
                });
            }
            Command::Commit{ctx, keys, lock_ts, commit_ts, callback} => {
                callback(self.store
                             .commit(ctx, keys, lock_ts, commit_ts)
                             .map_err(::storage::Error::from));
            }
            Command::CommitThenGet{ctx, key, lock_ts, commit_ts, get_ts, callback} => {
                callback(self.store
                             .commit_then_get(ctx, key, lock_ts, commit_ts, get_ts)
                             .map_err(::storage::Error::from));
            }
            Command::Cleanup{ctx, key, start_ts, callback} => {
                callback(self.store.cleanup(ctx, key, start_ts).map_err(::storage::Error::from));
            }
            Command::Rollback{ctx, keys, start_ts, callback} => {
                callback(self.store
                             .rollback(ctx, keys, start_ts)
                             .map_err(::storage::Error::from));
            }
            Command::RollbackThenGet{ctx, key, lock_ts, callback} => {
                callback(self.store
                             .rollback_then_get(ctx, key, lock_ts)
                             .map_err(::storage::Error::from));
            }
        }
    }
}
