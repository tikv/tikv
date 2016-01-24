use std::boxed::FnBox;
use std::fmt;
use std::thread::{self, JoinHandle};
use mio::{EventLoop, Handler, Sender, NotifyError};
use self::txn::Scheduler;

mod engine;
mod mvcc;
mod txn;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type KvPair = (Key, Value);
pub type Version = u64;
pub type Limit = usize;
pub type Puts = Vec<KvPair>;
pub type Deletes = Vec<Key>;
pub type Locks = Vec<Key>;
pub type Callback<T> = Box<FnBox(Result<T>) + Send>;

pub enum Command {
    Get(((Key, Version), Callback<Option<Value>>)),
    Scan(((Key, Limit, Version), Callback<Vec<KvPair>>)),
    Commit(((Puts, Deletes, Locks, Version), Callback<()>)),
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Command::Get(((ref key, version), _)) => {
                write!(f, "kv::command::get {:?} @ {}", key, version)
            }
            Command::Scan(((ref start_key, limit, version), _)) => {
                write!(f,
                       "kv::command::scan {:?}({}) @ {}",
                       start_key,
                       limit,
                       version)
            }
            Command::Commit(((ref puts, ref deletes, ref locks, version), _)) => {
                write!(f,
                       "kv::command::commit puts({}), deletes({}), locks({}) @ {}",
                       puts.len(),
                       deletes.len(),
                       locks.len(),
                       version)
            }
        }
    }
}

pub use self::engine::{Engine, Dsn};

pub struct Storage {
    sender: Sender<Message>,
    thread: JoinHandle<Result<()>>,
}

impl Storage {
    pub fn new(dsn: Dsn) -> Result<Storage> {
        let mut scheduler = {
            let engine = try!(engine::new_engine(dsn));
            Scheduler::new(engine)
        };
        let mut event_loop = try!(EventLoop::new());
        let sender = event_loop.channel();
        let thread_handle = thread::spawn(move || {
            event_loop.run(&mut scheduler).map_err(|e| Error::Io(e))
        });
        Ok(Storage {
            sender: sender,
            thread: thread_handle,
        })
    }

    pub fn stop(self) -> Result<()> {
        try!(self.sender.send(Message::Close));
        try!(try!(self.thread.join()));
        Ok(())
    }

    pub fn async_get(&self,
                     key: Key,
                     version: Version,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Command::Get(((key, version), callback));
        try!(self.sender.send(Message::Command((cmd))));
        Ok(())
    }

    pub fn async_scan(&self,
                      start_key: Key,
                      limit: Limit,
                      version: Version,
                      callback: Callback<Vec<KvPair>>)
                      -> Result<()> {
        let cmd = Command::Scan(((start_key, limit, version), callback));
        try!(self.sender.send(Message::Command((cmd))));
        Ok(())
    }

    pub fn async_commit(&self,
                        puts: Vec<KvPair>,
                        deletes: Vec<Key>,
                        locks: Vec<Key>,
                        version: Version,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Commit(((puts, deletes, locks, version), callback));
        try!(self.sender.send(Message::Command((cmd))));
        Ok(())
    }
}

impl Handler for Scheduler {
    type Timeout = ();
    type Message = Message;

    fn notify(&mut self, event_loop: &mut EventLoop<Scheduler>, msg: Message) {
        debug!("recv message: {:?}", msg);
        match msg {
            Message::Command(cmd) => self.handle_cmd(cmd),
            Message::Close => event_loop.shutdown(),
        }
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
        Io(err: ::std::io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Notify(err: NotifyError<Message>) {
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

    fn expect_commit_ok() -> Callback<()> {
        Box::new(|x: Result<()>| assert!(x.is_ok()))
    }

    fn expect_commit_err() -> Callback<()> {
        Box::new(|x: Result<()>| assert!(x.is_err()))
    }

    #[test]
    fn test_callback() {
        let storage = Storage::new(Dsn::Memory).unwrap();

        storage.async_get(b"abc".to_vec(), 1u64, expect_get_none()).unwrap();
        storage.async_commit(vec![(b"abc".to_vec(), b"123".to_vec())],
                             vec![],
                             vec![],
                             100u64,
                             expect_commit_ok())
               .unwrap();
        storage.async_commit(vec![(b"abc".to_vec(), b"123".to_vec())],
                             vec![],
                             vec![],
                             101u64,
                             expect_commit_ok())
               .unwrap();
        storage.async_commit(vec![(b"abc".to_vec(), b"123".to_vec())],
                             vec![],
                             vec![],
                             99u64,
                             expect_commit_err())
               .unwrap();

        storage.stop().unwrap();
    }
}
