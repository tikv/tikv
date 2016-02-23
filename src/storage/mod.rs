use std::boxed::FnBox;
use std::fmt;
use std::thread::{self, JoinHandle};
use std::sync::mpsc::{self, Sender};
use self::txn::Scheduler;

mod engine;
mod mvcc;
mod txn;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type KvPair = (Key, Value);
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

#[allow(type_complexity)]
pub enum Command {
    Get {
        key: Key,
        version: u64,
        callback: Callback<Option<Value>>,
    },
    Scan {
        start_key: Key,
        limit: usize,
        version: u64,
        callback: Callback<Vec<KvPair>>,
    },
    Prewrite {
        puts: Vec<KvPair>,
        deletes: Vec<Key>,
        locks: Vec<Key>,
        start_version: u64,
        callback: Callback<()>,
    },
    Commit {
        start_version: u64,
        commit_version: u64,
        callback: Callback<()>,
    },
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Command::Get{ref key, version, ..} => {
                write!(f, "kv::command::get {:?} @ {}", key, version)
            }
            Command::Scan{ref start_key, limit, version, ..} => {
                write!(f,
                       "kv::command::scan {:?}({}) @ {}",
                       start_key,
                       limit,
                       version)
            }
            Command::Prewrite {ref puts, ref deletes, ref locks, start_version, ..} => {
                write!(f,
                       "kv::command::prewrite puts({}), deletes({}), locks({}) @ {}",
                       puts.len(),
                       deletes.len(),
                       locks.len(),
                       start_version)
            }
            Command::Commit{start_version, commit_version, ..} => {
                write!(f,
                       "kv::command::commit {} -> {}",
                       start_version,
                       commit_version)
            }
        }
    }
}

pub use self::engine::{Engine, Dsn};

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
                     version: u64,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Command::Get {
            key: key,
            version: version,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_scan(&self,
                      start_key: Key,
                      limit: usize,
                      version: u64,
                      callback: Callback<Vec<KvPair>>)
                      -> Result<()> {
        let cmd = Command::Scan {
            start_key: start_key,
            limit: limit,
            version: version,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_prewrite(&self,
                          puts: Vec<KvPair>,
                          deletes: Vec<Key>,
                          locks: Vec<Key>,
                          start_version: u64,
                          callback: Callback<()>)
                          -> Result<()> {
        let cmd = Command::Prewrite {
            puts: puts,
            deletes: deletes,
            locks: locks,
            start_version: start_version,
            callback: callback,
        };
        try!(self.tx.send(Message::Command(cmd)));
        Ok(())
    }

    pub fn async_commit(&self,
                        start_version: u64,
                        commit_version: u64,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Commit {
            start_version: start_version,
            commit_version: commit_version,
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
    use super::{Dsn, Storage, Result, Value, Callback};

    fn expect_get_none() -> Callback<Option<Value>> {
        Box::new(|x: Result<Option<Value>>| assert_eq!(x.unwrap(), None))
    }

    fn expect_get_val(v: Vec<u8>) -> Callback<Option<Value>> {
        Box::new(move |x: Result<Option<Value>>| assert_eq!(x.unwrap().unwrap(), v))
    }

    fn expect_ok() -> Callback<()> {
        Box::new(|x: Result<()>| assert!(x.is_ok()))
    }

    fn expect_fail() -> Callback<()> {
        Box::new(|x: Result<()>| assert!(x.is_err()))
    }

    #[test]
    fn test_get_put() {
        let storage = Storage::new(Dsn::Memory).unwrap();
        storage.async_get(vec![b'x'], 100u64, expect_get_none()).unwrap();
        storage.async_prewrite(vec![(vec![b'x'], b"100".to_vec())],
                               vec![],
                               vec![],
                               100u64,
                               expect_ok())
               .unwrap();
        storage.async_commit(100u64, 101u64, expect_ok()).unwrap();
        storage.async_get(vec![b'x'], 100u64, expect_get_none()).unwrap();
        storage.async_get(vec![b'x'], 101u64, expect_get_val(b"100".to_vec())).unwrap();
        storage.stop().unwrap();
    }

    #[test]
    fn test_txn() {
        let storage = Storage::new(Dsn::Memory).unwrap();
        storage.async_prewrite(vec![(vec![b'x'], b"100".to_vec())],
                               vec![],
                               vec![],
                               100u64,
                               expect_ok())
               .unwrap();
        storage.async_prewrite(vec![(vec![b'y'], b"101".to_vec())],
                               vec![],
                               vec![],
                               101u64,
                               expect_ok())
               .unwrap();
        storage.async_commit(100u64, 110u64, expect_ok()).unwrap();
        storage.async_commit(101u64, 111u64, expect_ok()).unwrap();
        storage.async_get(vec![b'x'], 120u64, expect_get_val(b"100".to_vec())).unwrap();
        storage.async_get(vec![b'y'], 120u64, expect_get_val(b"101".to_vec())).unwrap();
        storage.async_prewrite(vec![(vec![b'x'], b"105".to_vec())],
                               vec![],
                               vec![],
                               105u64,
                               expect_fail())
               .unwrap();
        storage.stop().unwrap();
    }
}
