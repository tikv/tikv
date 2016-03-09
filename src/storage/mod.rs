use std::boxed::FnBox;
use std::fmt;
use std::thread::{self, JoinHandle};
use std::sync::mpsc::{self, Sender};
use self::txn::Scheduler;

mod engine;
mod mvcc;
mod txn;

pub use self::engine::{Engine, Dsn};

pub type Key = Vec<u8>;
pub type RefKey<'a> = &'a [u8];
pub type Value = Vec<u8>;
pub type KvPair = (Key, Value);
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

#[derive(Debug)]
pub enum Mutation {
    Put(KvPair),
    Delete(Key),
    Lock(Key),
}

#[allow(match_same_arms)]
impl Mutation {
    pub fn key(&self) -> RefKey {
        match *self {
            Mutation::Put((ref key, _)) => key,
            Mutation::Delete(ref key) => key,
            Mutation::Lock(ref key) => key,
        }
    }
}

#[allow(type_complexity)]
pub enum Command {
    Get {
        key: Key,
        start_ts: u64,
        callback: Callback<Option<Value>>,
    },
    Scan {
        start_key: Key,
        limit: usize,
        start_ts: u64,
        callback: Callback<Vec<Result<KvPair>>>,
    },
    Prewrite {
        mutations: Vec<Mutation>,
        primary: Key,
        start_ts: u64,
        callback: Callback<Vec<Result<()>>>,
    },
    Commit {
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
        callback: Callback<()>,
    },
    CommitThenGet {
        key: Key,
        lock_ts: u64,
        commit_ts: u64,
        get_ts: u64,
        callback: Callback<Option<Value>>,
    },
    Cleanup {
        key: Key,
        start_ts: u64,
        callback: Callback<()>,
    },
    Rollback {
        keys: Vec<Key>,
        start_ts: u64,
        callback: Callback<()>,
    },
    RollbackThenGet {
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
                write!(f, "kv::command::commit {} {} -> {}", keys.len(), lock_ts, commit_ts)
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
                write!(f, "kv::command::clean_up {:?} @ {}", key, start_ts)
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
    pub fn new(dsn: Dsn) -> Result<Storage> {
        let mut scheduler = {
            let engine = try!(engine::new_engine(dsn));
            Scheduler::new(engine)
        };
        let (tx, rx) = mpsc::channel::<Message>();
        let desc = format!("{:?}", dsn);
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

    pub fn stop(self) -> Result<()> {
        try!(self.tx.send(Message::Close));
        try!(try!(self.thread.join()));
        Ok(())
    }

    pub fn async_get(&self,
                     key: Key,
                     start_ts: u64,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Command::Get {
            key: key,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_scan(&self,
                      start_key: Key,
                      limit: usize,
                      start_ts: u64,
                      callback: Callback<Vec<Result<KvPair>>>)
                      -> Result<()> {
        let cmd = Command::Scan {
            start_key: start_key,
            limit: limit,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_prewrite(&self,
                          mutations: Vec<Mutation>,
                          primary: Key,
                          start_ts: u64,
                          callback: Callback<Vec<Result<()>>>)
                          -> Result<()> {
        let cmd = Command::Prewrite {
            mutations: mutations,
            primary: primary,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_commit(&self,
                        keys: Vec<Key>,
                        lock_ts: u64,
                        commit_ts: u64,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Commit {
            keys: keys,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_commit_then_get(&self,
                                 key: Key,
                                 lock_ts: u64,
                                 commit_ts: u64,
                                 get_ts: u64,
                                 callback: Callback<Option<Value>>)
                                 -> Result<()> {
        let cmd = Command::CommitThenGet {
            key: key,
            lock_ts: lock_ts,
            commit_ts: commit_ts,
            get_ts: get_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_clean_up(&self, key: Key, start_ts: u64, callback: Callback<()>) -> Result<()> {
        let cmd = Command::Cleanup {
            key: key,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_rollback(&self,
                          keys: Vec<Key>,
                          start_ts: u64,
                          callback: Callback<()>)
                          -> Result<()> {
        let cmd = Command::Rollback {
            keys: keys,
            start_ts: start_ts,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_rollback_then_get(&self,
                                   key: Key,
                                   lock_ts: u64,
                                   callback: Callback<Option<Value>>)
                                   -> Result<()> {
        let cmd = Command::RollbackThenGet {
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
        Engine(err: engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<::std::any::Any + Send>) {
            from()
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

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
        storage.async_get(vec![b'x'], 100, expect_get_none()).unwrap();
        storage.async_prewrite(vec![Mutation::Put((b"x".to_vec(), b"100".to_vec()))],
                               b"x".to_vec(),
                               100,
                               expect_ok())
               .unwrap();
        storage.async_commit(vec![b"x".to_vec()], 100, 101, expect_ok()).unwrap();
        storage.async_get(vec![b'x'], 100, expect_get_none()).unwrap();
        storage.async_get(vec![b'x'], 101, expect_get_val(b"100".to_vec())).unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_scan() {
        let storage = Storage::new(Dsn::Memory).unwrap();
        storage.async_prewrite(vec![
            Mutation::Put((b"a".to_vec(), b"aa".to_vec())),
            Mutation::Put((b"b".to_vec(), b"bb".to_vec())),
            Mutation::Put((b"c".to_vec(), b"cc".to_vec())),
            ], b"a".to_vec(),
                               1,
                               expect_ok())
               .unwrap();
        storage.async_commit(vec![b"a".to_vec(),b"b".to_vec(),b"c".to_vec(),],
        1,
                             2,
                             expect_ok())
               .unwrap();
        storage.async_scan(b"\x00".to_vec(),
                           1000,
                           5,
                           expect_scan(vec![
            Some((b"a".to_vec(), b"aa".to_vec())),
            Some((b"b".to_vec(), b"bb".to_vec())),
            Some((b"c".to_vec(), b"cc".to_vec())),
            ]))
               .unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let storage = Storage::new(Dsn::Memory).unwrap();
        storage.async_prewrite(vec![Mutation::Put((b"x".to_vec(), b"100".to_vec()))],b"x".to_vec(),
                               100,
                               expect_ok())
               .unwrap();
        storage.async_prewrite(vec![Mutation::Put((b"y".to_vec(), b"101".to_vec()))],b"y".to_vec(),
                               101,
                               expect_ok())
               .unwrap();
        storage.async_commit(vec![b"x".to_vec()],100, 110, expect_ok()).unwrap();
        storage.async_commit(vec![b"y".to_vec()],101, 111, expect_ok()).unwrap();
        storage.async_get(vec![b'x'], 120, expect_get_val(b"100".to_vec())).unwrap();
        storage.async_get(vec![b'y'], 120, expect_get_val(b"101".to_vec())).unwrap();
        storage.async_prewrite(vec![Mutation::Put((b"x".to_vec(), b"105".to_vec()))],b"x".to_vec(),
                               105,
                               expect_fail())
               .unwrap();
        storage.stop().unwrap();
    }
}
