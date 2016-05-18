// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use storage::Engine;
use storage::Command;
use super::store::TxnStore;

pub struct Scheduler {
    store: TxnStore,
}

impl Scheduler {
    pub fn new(engine: Arc<Box<Engine>>) -> Scheduler {
        Scheduler { store: TxnStore::new(engine) }
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        debug!("scheduler::handle_cmd: {:?}", cmd);
        match cmd {
            Command::Get { ctx, key, start_ts, callback } => {
                callback(self.store.get(ctx, &key, start_ts).map_err(::storage::Error::from));
            }
            Command::BatchGet { ctx, keys, start_ts, callback } => {
                callback(match self.store.batch_get(ctx, &keys, start_ts) {
                    Ok(results) => {
                        let mut res = vec![];
                        for (k, v) in keys.into_iter().zip(results.into_iter()) {
                            match v {
                                Ok(Some(x)) => res.push(Ok((k.raw().unwrap(), x))),
                                Ok(None) => {}
                                Err(e) => res.push(Err(::storage::Error::from(e))),
                            }
                        }
                        Ok(res)
                    }
                    Err(e) => Err(e.into()),
                });
            }
            Command::Scan { ctx, start_key, limit, start_ts, callback } => {
                callback(match self.store.scan(ctx, start_key, limit, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(e.into()),
                });
            }
            Command::Prewrite { ctx, mutations, primary, start_ts, callback } => {
                callback(match self.store.prewrite(ctx, mutations, primary, start_ts) {
                    Ok(mut results) => {
                        Ok(results.drain(..).map(|x| x.map_err(::storage::Error::from)).collect())
                    }
                    Err(e) => Err(e.into()),
                });
            }
            Command::Commit { ctx, keys, lock_ts, commit_ts, callback } => {
                callback(self.store
                    .commit(ctx, keys, lock_ts, commit_ts)
                    .map_err(::storage::Error::from));
            }
            Command::CommitThenGet { ctx, key, lock_ts, commit_ts, get_ts, callback } => {
                callback(self.store
                    .commit_then_get(ctx, key, lock_ts, commit_ts, get_ts)
                    .map_err(::storage::Error::from));
            }
            Command::Cleanup { ctx, key, start_ts, callback } => {
                callback(self.store.cleanup(ctx, key, start_ts).map_err(::storage::Error::from));
            }
            Command::Rollback { ctx, keys, start_ts, callback } => {
                callback(self.store
                    .rollback(ctx, keys, start_ts)
                    .map_err(::storage::Error::from));
            }
            Command::RollbackThenGet { ctx, key, lock_ts, callback } => {
                callback(self.store
                    .rollback_then_get(ctx, key, lock_ts)
                    .map_err(::storage::Error::from));
            }
        }
    }
}
