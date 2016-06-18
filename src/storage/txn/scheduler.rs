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
use storage::{Engine, Command, CallbackResult};
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
            Command::Get { ctx, row, ts, callback } => {
                let res = self.store.get(ctx, row, ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(extract_row_value(res)));
                }
            }
            Command::BatchGet { ctx, rows, ts, callback } => {
                let res = self.store.batch_get(ctx, rows, ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(extract_row_values(res)));
                }
            }
            Command::Scan { ctx, start_row, limit, ts, callback } => {
                let res = self.store.scan(ctx, start_row, limit, ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(extract_row_values(res)));
                }
            }
            Command::Prewrite { ctx, mutations, primary, ts, callback } => {
                let res = self.store.prewrite(ctx, mutations, primary, ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(extract_key_errors(res)));
                }
            }
            Command::Commit { ctx, rows, start_ts, commit_ts, callback } => {
                let res = self.store.commit(ctx, rows, start_ts, commit_ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(res.err().map(|e| extract_key_error(&e))));
                }
            }
            Command::CommitThenGet { ctx, row, start_ts, commit_ts, get_ts, callback } => {
                let res = self.store.commit_then_get(ctx, row, start_ts, commit_ts, get_ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(extract_row_value(res)));
                }
            }
            Command::Cleanup { ctx, row, ts, callback } => {
                let res = self.store.cleanup(ctx, row, ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else if let Some(ts) = extract_committed(&res) {
                    callback(CallbackResult::Ok((None, Some(ts))));
                } else {
                    callback(CallbackResult::Ok((res.err().map(|e| extract_key_error(&e)), None)))
                }
            }
            Command::Rollback { ctx, rows, ts, callback } => {
                let res = self.store.rollback(ctx, rows, ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(res.err().map(|e| extract_key_error(&e))));
                }
            }
            Command::RollbackThenGet { ctx, row, ts, callback } => {
                let res = self.store.rollback_then_get(ctx, row, ts);
                if let Some(e) = extract_region_error(&res) {
                    callback(CallbackResult::Err(e));
                } else {
                    callback(CallbackResult::Ok(extract_row_value(res)));
                }
            }
        }
    }
}

use super::{Result, Error};
use storage::engine::Error as EngineError;
use storage::mvcc::Error as MvccError;
use kvproto::errorpb::Error as RegionError;
use kvproto::kvpb::{RowValue, KeyError, LockInfo};

fn extract_region_error<T>(res: &Result<T>) -> Option<RegionError> {
    match *res {
        // TODO: use `Error::cause` instead.
        Err(Error::Engine(EngineError::Request(ref e))) |
        Err(Error::Mvcc(MvccError::Engine(EngineError::Request(ref e)))) => Some(e.to_owned()),
        _ => None,
    }
}

fn extract_key_error(err: &Error) -> KeyError {
    let mut key_error = KeyError::new();
    match *err {
        Error::Mvcc(MvccError::KeyIsLocked { ref key, ref primary, ts }) => {
            let mut lock_info = LockInfo::new();
            lock_info.set_row(key.to_owned());
            lock_info.set_primary(primary.to_owned());
            lock_info.set_ts(ts);
            key_error.set_locked(lock_info);
        }
        Error::Mvcc(MvccError::WriteConflict) |
        Error::Mvcc(MvccError::TxnLockNotFound) => {
            key_error.set_retryable(format!("{:?}", err));
        }
        _ => key_error.set_abort(format!("{:?}", err)),
    }
    key_error
}

fn extract_committed(res: &Result<()>) -> Option<u64> {
    match *res {
        Err(Error::Mvcc(MvccError::AlreadyCommitted { commit_ts })) => Some(commit_ts),
        _ => None,
    }
}

fn extract_key_errors(res: Result<Vec<Result<()>>>) -> Vec<KeyError> {
    match res {
        Ok(res) => res.into_iter().filter_map(|x| x.err()).map(|e| extract_key_error(&e)).collect(),
        Err(e) => vec![extract_key_error(&e)],
    }
}

fn extract_row_value(res: Result<RowValue>) -> RowValue {
    match res {
        Ok(r) => r,
        Err(e) => {
            let mut r = RowValue::new();
            r.set_error(extract_key_error(&e));
            r
        }
    }
}

fn extract_row_values(res: Result<Vec<Result<RowValue>>>) -> Vec<RowValue> {
    match res {
        Ok(res) => res.into_iter().map(|x| extract_row_value(x)).collect(),
        Err(e) => vec![extract_row_value(Err(e))],
    }
}
