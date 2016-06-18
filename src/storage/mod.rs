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

use std::boxed::FnBox;
use std::fmt;
use std::error;
use std::thread::{self, JoinHandle};
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use self::txn::Scheduler;
use kvproto::kvpb::{Row, RowValue, Mutation, KeyError};
use kvproto::kvrpcpb::Context;
use kvproto::errorpb::Error as RegionError;

pub mod engine;
pub mod mvcc;
pub mod txn;
mod types;

pub use self::engine::{Engine, Snapshot, Dsn, TEMP_DIR, new_engine, Modify, Cursor,
                       Error as EngineError};
pub use self::engine::raftkv::RaftKv;
pub use self::txn::SnapshotStore;
pub use self::types::{Key, Value};

pub enum CallbackResult<T> {
    Ok(T),
    Err(RegionError),
}
pub type Callback<T> = Box<FnBox(CallbackResult<T>) + Send>;

#[cfg(test)]
pub use self::types::make_key;

#[allow(type_complexity)]
pub enum Command {
    Get {
        ctx: Context,
        row: Row,
        ts: u64,
        callback: Callback<RowValue>,
    },
    BatchGet {
        ctx: Context,
        rows: Vec<Row>,
        ts: u64,
        callback: Callback<Vec<RowValue>>,
    },
    Scan {
        ctx: Context,
        start_row: Row,
        limit: usize,
        ts: u64,
        callback: Callback<Vec<RowValue>>,
    },
    Prewrite {
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        ts: u64,
        callback: Callback<Vec<KeyError>>,
    },
    Commit {
        ctx: Context,
        rows: Vec<Vec<u8>>,
        start_ts: u64,
        commit_ts: u64,
        callback: Callback<Option<KeyError>>,
    },
    CommitThenGet {
        ctx: Context,
        row: Row,
        start_ts: u64,
        commit_ts: u64,
        get_ts: u64,
        callback: Callback<RowValue>,
    },
    Cleanup {
        ctx: Context,
        row: Vec<u8>,
        ts: u64,
        callback: Callback<(Option<KeyError>, Option<u64>)>,
    },
    Rollback {
        ctx: Context,
        rows: Vec<Vec<u8>>,
        ts: u64,
        callback: Callback<Option<KeyError>>,
    },
    RollbackThenGet {
        ctx: Context,
        row: Row,
        ts: u64,
        callback: Callback<RowValue>,
    },
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Command::Get { ref row, ts, .. } => write!(f, "kv::command::get {:?} @ {}", row, ts),
            Command::BatchGet { ref rows, ts, .. } => {
                write!(f, "kv::command_batch_get {} @ {}", rows.len(), ts)
            }
            Command::Scan { ref start_row, limit, ts, .. } => {
                write!(f, "kv::command::scan {:?}({}) @ {}", start_row, limit, ts)
            }
            Command::Prewrite { ref mutations, ts, .. } => {
                write!(f,
                       "kv::command::prewrite mutations({}) @ {}",
                       mutations.len(),
                       ts)
            }
            Command::Commit { ref rows, start_ts, commit_ts, .. } => {
                write!(f,
                       "kv::command::commit {} {} -> {}",
                       rows.len(),
                       start_ts,
                       commit_ts)
            }
            Command::CommitThenGet { ref row, start_ts, commit_ts, get_ts, .. } => {
                write!(f,
                       "kv::command::commit_then_get {:?} {} -> {} @ {}",
                       row,
                       start_ts,
                       commit_ts,
                       get_ts)
            }
            Command::Cleanup { ref row, ts, .. } => {
                write!(f, "kv::command::cleanup {:?} @ {}", row, ts)
            }
            Command::Rollback { ref rows, ts, .. } => {
                write!(f, "kv::command::rollback keys({}) @ {}", rows.len(), ts)
            }
            Command::RollbackThenGet { ref row, ts, .. } => {
                write!(f, "kv::rollback_then_get {:?} @ {}", row, ts)
            }
        }
    }
}

pub struct Storage {
    engine: Arc<Box<Engine>>,
    tx: Sender<Message>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl Storage {
    pub fn from_engine(engine: Box<Engine>) -> Result<Storage> {
        let desc = format!("{:?}", engine);
        let engine = Arc::new(engine);
        let mut scheduler = Scheduler::new(engine.clone());

        let (tx, rx) = mpsc::channel::<Message>();
        let builder = thread::Builder::new().name(thd_name!(format!("storage-{:?}", desc)));
        let handle = box_try!(builder.spawn(move || {
            info!("storage: [{}] started.", desc);
            loop {
                let msg = try!(rx.recv());
                debug!("recv message: {:?}", msg);
                match msg {
                    Message::Command(cmd) => scheduler.handle_cmd(cmd),
                    Message::Close => break,
                }
            }
            info!("storage: [{}] closing.", desc);
            Ok(())
        }));

        Ok(Storage {
            engine: engine,
            tx: tx,
            thread: Some(handle),
        })
    }

    pub fn new(dsn: Dsn) -> Result<Storage> {
        let engine = try!(engine::new_engine(dsn));
        Storage::from_engine(engine)
    }

    pub fn stop(&mut self) -> Result<()> {
        if self.thread.is_none() {
            return Ok(());
        }
        try!(self.tx.send(Message::Close));
        if self.thread.take().unwrap().join().is_err() {
            return Err(box_err!("failed to wait storage thread quit"));
        }
        Ok(())
    }

    pub fn get_engine(&self) -> Arc<Box<Engine>> {
        self.engine.clone()
    }

    pub fn async_get(&self,
                     ctx: Context,
                     row: Row,
                     ts: u64,
                     callback: Callback<RowValue>)
                     -> Result<()> {
        let cmd = Command::Get {
            ctx: ctx,
            row: row,
            ts: ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_batch_get(&self,
                           ctx: Context,
                           rows: Vec<Row>,
                           ts: u64,
                           callback: Callback<Vec<RowValue>>)
                           -> Result<()> {
        let cmd = Command::BatchGet {
            ctx: ctx,
            rows: rows,
            ts: ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_scan(&self,
                      ctx: Context,
                      start_row: Row,
                      limit: usize,
                      ts: u64,
                      callback: Callback<Vec<RowValue>>)
                      -> Result<()> {
        let cmd = Command::Scan {
            ctx: ctx,
            start_row: start_row,
            limit: limit,
            ts: ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_prewrite(&self,
                          ctx: Context,
                          mutations: Vec<Mutation>,
                          primary: Vec<u8>,
                          ts: u64,
                          callback: Callback<Vec<KeyError>>)
                          -> Result<()> {
        let cmd = Command::Prewrite {
            ctx: ctx,
            mutations: mutations,
            primary: primary,
            ts: ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_commit(&self,
                        ctx: Context,
                        rows: Vec<Vec<u8>>,
                        start_ts: u64,
                        commit_ts: u64,
                        callback: Callback<Option<KeyError>>)
                        -> Result<()> {
        let cmd = Command::Commit {
            ctx: ctx,
            rows: rows,
            start_ts: start_ts,
            commit_ts: commit_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_commit_then_get(&self,
                                 ctx: Context,
                                 row: Row,
                                 start_ts: u64,
                                 commit_ts: u64,
                                 get_ts: u64,
                                 callback: Callback<RowValue>)
                                 -> Result<()> {
        let cmd = Command::CommitThenGet {
            ctx: ctx,
            row: row,
            start_ts: start_ts,
            commit_ts: commit_ts,
            get_ts: get_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_cleanup(&self,
                         ctx: Context,
                         row: Vec<u8>,
                         ts: u64,
                         callback: Callback<(Option<KeyError>, Option<u64>)>)
                         -> Result<()> {
        let cmd = Command::Cleanup {
            ctx: ctx,
            row: row,
            ts: ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_rollback(&self,
                          ctx: Context,
                          rows: Vec<Vec<u8>>,
                          ts: u64,
                          callback: Callback<Option<KeyError>>)
                          -> Result<()> {
        let cmd = Command::Rollback {
            ctx: ctx,
            rows: rows,
            ts: ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_rollback_then_get(&self,
                                   ctx: Context,
                                   row: Row,
                                   ts: u64,
                                   callback: Callback<RowValue>)
                                   -> Result<()> {
        let cmd = Command::RollbackThenGet {
            ctx: ctx,
            row: row,
            ts: ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }
}

#[derive(Debug)]
pub enum Message {
    Command(Command),
    Close,
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Recv(err: mpsc::RecvError) {
            from()
            cause(err)
            description(err.description())
        }
        Send(err: mpsc::SendError<Message>) {
            from()
            cause(err)
            description(err.description())
        }
        Engine(err: EngineError) {
            from()
            cause(err)
            description(err.description())
        }
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::kvrpcpb::Context;
    use kvproto::kvpb::{RowValue, KeyError};

    fn expect_get_none() -> Callback<RowValue> {
        Box::new(|res| match res {
            CallbackResult::Ok(x) => assert_eq!(mvcc::default_row_value(&x), None),
            CallbackResult::Err(_) => panic!("expect_get_none got region error."),
        })
    }

    fn expect_get_val(v: Vec<u8>) -> Callback<RowValue> {
        Box::new(move |res| match res {
            CallbackResult::Ok(x) => assert_eq!(mvcc::default_row_value(&x), Some(v)),
            CallbackResult::Err(_) => panic!("expect_get_val got region error."),
        })
    }

    fn expect_ok() -> Callback<Option<KeyError>> {
        Box::new(|res| match res {
            CallbackResult::Ok(x) => assert_eq!(x, None),
            CallbackResult::Err(_) => panic!("expect_ok got region error."),
        })
    }

    fn expect_oks() -> Callback<Vec<KeyError>> {
        Box::new(|res| match res {
            CallbackResult::Ok(x) => assert_eq!(x, vec![]),
            CallbackResult::Err(_) => panic!("expect_oks got region error."),
        })
    }

    fn expect_fails() -> Callback<Vec<KeyError>> {
        Box::new(|res: CallbackResult<Vec<KeyError>>| match res {
            CallbackResult::Ok(x) => assert!(x.len() > 0),
            CallbackResult::Err(_) => panic!("expect_fails got region error."),
        })
    }

    fn expect_scan(pairs: Vec<(&[u8], &[u8])>) -> Callback<Vec<RowValue>> {
        let expect: Vec<(Vec<u8>, Vec<u8>)> =
            pairs.into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        Box::new(move |res: CallbackResult<Vec<RowValue>>| match res {
            CallbackResult::Ok(x) => {
                let result: Vec<(Vec<u8>, Vec<u8>)> = x.into_iter()
                    .map(|x| (x.get_row_key().to_vec(), mvcc::default_row_value(&x).unwrap()))
                    .collect();
                assert_eq!(result, expect);
            }
            CallbackResult::Err(_) => panic!("expect_scan got region error."),
        })
    }

    #[test]
    fn test_get_put() {
        let mut storage = Storage::new(Dsn::RocksDBPath(TEMP_DIR)).unwrap();
        storage.async_get(Context::new(),
                       mvcc::default_row(b"x"),
                       100,
                       expect_get_none())
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![mvcc::default_put(b"x", b"100")],
                            b"x".to_vec(),
                            100,
                            expect_oks())
            .unwrap();
        storage.async_commit(Context::new(), vec![b"x".to_vec()], 100, 101, expect_ok())
            .unwrap();
        storage.async_get(Context::new(),
                       mvcc::default_row(b"x"),
                       100,
                       expect_get_none())
            .unwrap();
        storage.async_get(Context::new(),
                       mvcc::default_row(b"x"),
                       101,
                       expect_get_val(b"100".to_vec()))
            .unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_scan() {
        let mut storage = Storage::new(Dsn::RocksDBPath(TEMP_DIR)).unwrap();
        storage.async_prewrite(Context::new(),
                            vec![mvcc::default_put(b"a", b"aa"),
                                 mvcc::default_put(b"b", b"bb"),
                                 mvcc::default_put(b"c", b"cc")],
                            b"a".to_vec(),
                            1,
                            expect_oks())
            .unwrap();
        storage.async_commit(Context::new(),
                          vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
                          1,
                          2,
                          expect_ok())
            .unwrap();
        storage.async_scan(Context::new(),
                        mvcc::default_row(b"a"),
                        1000,
                        5,
                        expect_scan(vec![(b"a", b"aa"), (b"b", b"bb"), (b"c", b"cc")]))
            .unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let mut storage = Storage::new(Dsn::RocksDBPath(TEMP_DIR)).unwrap();
        storage.async_prewrite(Context::new(),
                            vec![mvcc::default_put(b"x", b"100")],
                            b"x".to_vec(),
                            100,
                            expect_oks())
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![mvcc::default_put(b"y", b"101")],
                            b"y".to_vec(),
                            101,
                            expect_oks())
            .unwrap();
        storage.async_commit(Context::new(), vec![b"x".to_vec()], 100, 110, expect_ok())
            .unwrap();
        storage.async_commit(Context::new(), vec![b"y".to_vec()], 101, 111, expect_ok())
            .unwrap();
        storage.async_get(Context::new(),
                       mvcc::default_row(b"x"),
                       120,
                       expect_get_val(b"100".to_vec()))
            .unwrap();
        storage.async_get(Context::new(),
                       mvcc::default_row(b"y"),
                       120,
                       expect_get_val(b"101".to_vec()))
            .unwrap();
        storage.async_prewrite(Context::new(),
                            vec![mvcc::default_put(b"x", b"105")],
                            b"x".to_vec(),
                            105,
                            expect_fails())
            .unwrap();
        storage.stop().unwrap();
    }
}
