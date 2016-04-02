use std::boxed::FnBox;
use std::fmt;
use std::error;
use std::thread::{self, JoinHandle};
use std::sync::mpsc::{self, Sender};
use self::txn::Scheduler;

pub mod engine;
pub mod mvcc;
pub mod txn;
mod types;

pub use self::engine::{Engine, Dsn, new_engine, Modify, Error as EngineError};
pub use self::engine::raftkv::RaftKv;
pub use self::types::{Key, Value, KvPair};
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

#[cfg(test)]
pub use self::types::make_key;

#[derive(Debug)]
pub enum Mutation {
    Put((Key, Value)),
    Delete(Key),
    Lock(Key),
}

#[allow(match_same_arms)]
impl Mutation {
    pub fn key(&self) -> &Key {
        match *self {
            Mutation::Put((ref key, _)) => key,
            Mutation::Delete(ref key) => key,
            Mutation::Lock(ref key) => key,
        }
    }
}

use kvproto::kvrpcpb::Context;

#[allow(type_complexity)]
pub enum Command {
    Get {
        ctx: Context,
        key: Key,
        start_ts: u64,
        callback: Callback<Option<Value>>,
    },
    Scan {
        ctx: Context,
        start_key: Key,
        limit: usize,
        start_ts: u64,
        callback: Callback<Vec<Result<KvPair>>>,
    },
    Prewrite {
        ctx: Context,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
        callback: Callback<Vec<Result<()>>>,
    },
    Commit {
        ctx: Context,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
        callback: Callback<()>,
    },
    CommitThenGet {
        ctx: Context,
        key: Key,
        lock_ts: u64,
        commit_ts: u64,
        get_ts: u64,
        callback: Callback<Option<Value>>,
    },
    Cleanup {
        ctx: Context,
        key: Key,
        start_ts: u64,
        callback: Callback<()>,
    },
    Rollback {
        ctx: Context,
        keys: Vec<Key>,
        start_ts: u64,
        callback: Callback<()>,
    },
    RollbackThenGet {
        ctx: Context,
        key: Key,
        lock_ts: u64,
        callback: Callback<Option<Value>>,
    },
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Command::Get{ref key, start_ts, ..} => {
                write!(f, "kv::command::get {:?} @ {}", key, start_ts)
            }
            Command::Scan{ref start_key, limit, start_ts, ..} => {
                write!(f,
                       "kv::command::scan {:?}({}) @ {}",
                       start_key,
                       limit,
                       start_ts)
            }
            Command::Prewrite {ref mutations, start_ts, ..} => {
                write!(f,
                       "kv::command::prewrite mutations({}) @ {}",
                       mutations.len(),
                       start_ts)
            }
            Command::Commit{ref keys, lock_ts, commit_ts, ..} => {
                write!(f,
                       "kv::command::commit {} {} -> {}",
                       keys.len(),
                       lock_ts,
                       commit_ts)
            }
            Command::CommitThenGet{ref key, lock_ts, commit_ts, get_ts, ..} => {
                write!(f,
                       "kv::command::commit_then_get {:?} {} -> {} @ {}",
                       key,
                       lock_ts,
                       commit_ts,
                       get_ts)
            }
            Command::Cleanup{ref key, start_ts, ..} => {
                write!(f, "kv::command::cleanup {:?} @ {}", key, start_ts)
            }
            Command::Rollback{ref keys, start_ts, ..} => {
                write!(f,
                       "kv::command::rollback keys({}) @ {}",
                       keys.len(),
                       start_ts)
            }
            Command::RollbackThenGet{ref key, lock_ts, ..} => {
                write!(f, "kv::rollback_then_get {:?} @ {}", key, lock_ts)
            }
        }
    }
}

pub struct Storage {
    tx: Sender<Message>,
    thread: JoinHandle<Result<()>>,
}

impl Storage {
    pub fn from_engine(engine: Box<Engine>) -> Result<Storage> {
        let desc = format!("{:?}", engine);
        let mut scheduler = Scheduler::new(engine);

        let (tx, rx) = mpsc::channel::<Message>();
        let handle = thread::spawn(move || {
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
        });
        Ok(Storage {
            tx: tx,
            thread: handle,
        })
    }

    pub fn new(dsn: Dsn) -> Result<Storage> {
        let engine = try!(engine::new_engine(dsn));
        Storage::from_engine(engine)
    }

    pub fn stop(self) -> Result<()> {
        try!(self.tx.send(Message::Close));
        if self.thread.join().is_err() {
            return Err(box_err!("failed to wait storage thread quit"));
        }
        Ok(())
    }

    pub fn async_get(&self,
                     ctx: Context,
                     key: Key,
                     start_ts: u64,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Command::Get {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_scan(&self,
                      ctx: Context,
                      start_key: Key,
                      limit: usize,
                      start_ts: u64,
                      callback: Callback<Vec<Result<KvPair>>>)
                      -> Result<()> {
        let cmd = Command::Scan {
            ctx: ctx,
            start_key: start_key,
            limit: limit,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_prewrite(&self,
                          ctx: Context,
                          mutations: Vec<Mutation>,
                          primary: Vec<u8>,
                          start_ts: u64,
                          callback: Callback<Vec<Result<()>>>)
                          -> Result<()> {
        let cmd = Command::Prewrite {
            ctx: ctx,
            mutations: mutations,
            primary: primary,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_commit(&self,
                        ctx: Context,
                        keys: Vec<Key>,
                        lock_ts: u64,
                        commit_ts: u64,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Commit {
            ctx: ctx,
            keys: keys,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_commit_then_get(&self,
                                 ctx: Context,
                                 key: Key,
                                 lock_ts: u64,
                                 commit_ts: u64,
                                 get_ts: u64,
                                 callback: Callback<Option<Value>>)
                                 -> Result<()> {
        let cmd = Command::CommitThenGet {
            ctx: ctx,
            key: key,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
            get_ts: get_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_cleanup(&self,
                         ctx: Context,
                         key: Key,
                         start_ts: u64,
                         callback: Callback<()>)
                         -> Result<()> {
        let cmd = Command::Cleanup {
            ctx: ctx,
            key: key,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_rollback(&self,
                          ctx: Context,
                          keys: Vec<Key>,
                          start_ts: u64,
                          callback: Callback<()>)
                          -> Result<()> {
        let cmd = Command::Rollback {
            ctx: ctx,
            keys: keys,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_rollback_then_get(&self,
                                   ctx: Context,
                                   key: Key,
                                   lock_ts: u64,
                                   callback: Callback<Option<Value>>)
                                   -> Result<()> {
        let cmd = Command::RollbackThenGet {
            ctx: ctx,
            key: key,
            lock_ts: lock_ts,
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

pub trait MaybeLocked {
    fn is_locked(&self) -> bool;
    fn get_lock(&self) -> Option<(Vec<u8>, Vec<u8>, u64)>;
}

impl<T> MaybeLocked for Result<T> {
    fn is_locked(&self) -> bool {
        match *self {
            Err(Error::Txn(txn::Error::Mvcc(mvcc::Error::KeyIsLocked{..}))) => true,
            _ => false,
        }
    }

    fn get_lock(&self) -> Option<(Vec<u8>, Vec<u8>, u64)> {
        match *self {
            Err(Error::Txn(txn::Error::Mvcc(mvcc::Error::KeyIsLocked{
                ref key,
                ref primary,
                ts}))) => Some((key.to_owned(), primary.to_owned(), ts)),
            _ => None,
        }
    }
}

pub trait MaybeComitted {
    fn is_committed(&self) -> bool;
    fn get_commit(&self) -> Option<u64>;
}

impl<T> MaybeComitted for Result<T> {
    fn is_committed(&self) -> bool {
        match *self {
            Err(Error::Txn(txn::Error::Mvcc(mvcc::Error::AlreadyCommitted{..}))) => true,
            _ => false,
        }
    }

    fn get_commit(&self) -> Option<u64> {
        match *self {
            Err(Error::Txn(txn::Error::Mvcc(mvcc::Error::AlreadyCommitted{commit_ts}))) => {
                Some(commit_ts)
            }
            _ => None,
        }
    }
}

pub trait MaybeRolledback {
    fn is_rolledback(&self) -> bool;
}

impl<T> MaybeRolledback for Result<T> {
    fn is_rolledback(&self) -> bool {
        match *self {
            Err(Error::Txn(txn::Error::Mvcc(mvcc::Error::AlreadyRolledback))) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::kvrpcpb::Context;
    use util::codec::bytes;

    fn expect_get_none() -> Callback<Option<Value>> {
        Box::new(|x: Result<Option<Value>>| assert_eq!(x.unwrap(), None))
    }

    fn expect_get_val(v: Vec<u8>) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| assert_eq!(x.unwrap().unwrap(), v))
    }

    fn expect_ok<T>() -> Callback<T> {
        Box::new(|x: Result<T>| assert!(x.is_ok()))
    }

    fn expect_fail<T>() -> Callback<T> {
        Box::new(|x: Result<T>| assert!(x.is_err()))
    }

    fn expect_scan(pairs: Vec<Option<KvPair>>) -> Callback<Vec<Result<KvPair>>> {
        Box::new(move |rlt: Result<Vec<Result<KvPair>>>| {
            let rlt: Vec<Option<KvPair>> = rlt.unwrap()
                                              .into_iter()
                                              .map(Result::ok)
                                              .collect();
            assert_eq!(rlt, pairs);
        })
    }

    #[test]
    fn test_get_put() {
        let storage = Storage::new(Dsn::Memory).unwrap();
        storage.async_get(Context::new(), make_key(b"x"), 100, expect_get_none()).unwrap();
        storage.async_prewrite(Context::new(),
                               vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                               b"x".to_vec(),
                               100,
                               expect_ok())
               .unwrap();
        storage.async_commit(Context::new(), vec![make_key(b"x")], 100, 101, expect_ok())
               .unwrap();
        storage.async_get(Context::new(), make_key(b"x"), 100, expect_get_none()).unwrap();
        storage.async_get(Context::new(),
                          make_key(b"x"),
                          101,
                          expect_get_val(b"100".to_vec()))
               .unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_scan() {
        let storage = Storage::new(Dsn::Memory).unwrap();
        storage.async_prewrite(Context::new(),
                               vec![
            Mutation::Put((make_key(b"a"), b"aa".to_vec())),
            Mutation::Put((make_key(b"b"), b"bb".to_vec())),
            Mutation::Put((make_key(b"c"), b"cc".to_vec())),
            ],
                               b"a".to_vec(),
                               1,
                               expect_ok())
               .unwrap();
        storage.async_commit(Context::new(),
                             vec![make_key(b"a"),make_key(b"b"),make_key(b"c"),],
                             1,
                             2,
                             expect_ok())
               .unwrap();
        storage.async_scan(Context::new(),
                           make_key(b"\x00"),
                           1000,
                           5,
                           expect_scan(vec![
            Some((bytes::encode_bytes(b"a"), b"aa".to_vec())),
            Some((bytes::encode_bytes(b"b"), b"bb".to_vec())),
            Some((bytes::encode_bytes(b"c"), b"cc".to_vec())),
            ]))
               .unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let storage = Storage::new(Dsn::Memory).unwrap();
        storage.async_prewrite(Context::new(),
                               vec![Mutation::Put((make_key(b"x"), b"100".to_vec()))],
                               b"x".to_vec(),
                               100,
                               expect_ok())
               .unwrap();
        storage.async_prewrite(Context::new(),
                               vec![Mutation::Put((make_key(b"y"), b"101".to_vec()))],
                               b"y".to_vec(),
                               101,
                               expect_ok())
               .unwrap();
        storage.async_commit(Context::new(), vec![make_key(b"x")], 100, 110, expect_ok())
               .unwrap();
        storage.async_commit(Context::new(), vec![make_key(b"y")], 101, 111, expect_ok())
               .unwrap();
        storage.async_get(Context::new(),
                          make_key(b"x"),
                          120,
                          expect_get_val(b"100".to_vec()))
               .unwrap();
        storage.async_get(Context::new(),
                          make_key(b"y"),
                          120,
                          expect_get_val(b"101".to_vec()))
               .unwrap();
        storage.async_prewrite(Context::new(),
                               vec![Mutation::Put((make_key(b"x"), b"105".to_vec()))],
                               b"x".to_vec(),
                               105,
                               expect_fail())
               .unwrap();
        storage.stop().unwrap();
    }
}
