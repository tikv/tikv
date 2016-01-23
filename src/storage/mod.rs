mod engine;
mod mvcc;

use std::fmt;
use std::boxed::FnBox;

use mio::{EventLoop, Handler, Sender};

pub use self::engine::Dsn;
use self::engine::Engine;
use self::mvcc::{MvccEngine, Result};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug, Clone)]
pub struct KvPair {
    key: Key,
    value: Value,
}

pub struct Storage {
    event_loop: EventLoop<StorageHandler>,
    handler: StorageHandler,
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            event_loop: EventLoop::new().unwrap(),
            handler: StorageHandler,
        }
    }

    pub fn get_sender(&self) -> StorageSender {
        StorageSender { inner_sender: self.event_loop.channel() }
    }

    pub fn run(&mut self) {
        self.event_loop.run(&mut self.handler).unwrap();
    }
}

pub struct StorageSender {
    inner_sender: Sender<Message>,
}

impl StorageSender {
    pub fn async_get(&self,
                     key: Key,
                     version: u64,
                     callback: Box<FnBox(Result<Option<Value>>) + Send>) {
        self.inner_sender.send(Message::Get(Cmd((key, version), callback))).unwrap();
    }

    pub fn async_scan(&self,
                      start_key: Key,
                      limit: usize,
                      version: u64,
                      callback: Box<FnBox(Result<Vec<KvPair>>) + Send>) {
        self.inner_sender.send(Message::Scan(Cmd((start_key, limit, version), callback))).unwrap();
    }

    pub fn async_commit(&self,
                        puts: Vec<KvPair>,
                        deletes: Vec<Key>,
                        locks: Vec<Key>,
                        version: u64,
                        callback: Box<FnBox(Result<()>) + Send>) {
        self.inner_sender
            .send(Message::Commit(Cmd((puts, deletes, locks, version), callback)))
            .unwrap();
    }

    pub fn close(&self) {
        self.inner_sender.send(Message::Close).unwrap();
    }
}

type Version = u64;
type Limit = usize;
type Puts = Vec<KvPair>;
type Deletes = Vec<Key>;
type Locks = Vec<Key>;

struct Cmd<T, V>(T, Box<FnBox(Result<V>) + Send>);
enum Message {
    Get(Cmd<(Key, Version), Option<Value>>),
    Scan(Cmd<(Key, Limit, Version), Vec<KvPair>>),
    Commit(Cmd<(Puts, Deletes, Locks, Version), ()>),
    Close,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Message::Get(Cmd((ref key, version), _)) => {
                write!(f, "storage::message::get {:?} @ {}", key, version)
            }
            Message::Scan(Cmd((ref start_key, limit, version), _)) => {
                write!(f,
                       "storage::message::scan {:?}({}) @ {}",
                       start_key,
                       limit,
                       version)
            }
            Message::Commit(Cmd((ref puts, ref deletes, ref locks, version), _)) => {
                write!(f,
                       "storage::message::commit puts({}), deletes({}), locks({}) @ {}",
                       puts.len(),
                       deletes.len(),
                       locks.len(),
                       version)
            }
            Message::Close => write!(f, "storage::message::close"),
        }
    }
}

struct StorageHandler;

impl Handler for StorageHandler {
    type Timeout = ();
    type Message = Message;

    fn notify(&mut self, event_loop: &mut EventLoop<StorageHandler>, msg: Message) {
        debug!("recv message: {:?}", msg);
        match msg {
            Message::Get(Cmd(_, callback)) => callback(Ok(None)),
            Message::Scan(Cmd(_, callback)) => callback(Ok(vec![])),
            Message::Commit(Cmd(_, callback)) => callback(Ok(())),
            Message::Close => event_loop.shutdown(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use super::{Storage, Value};

    #[test]
    fn test_async_api() {
        let mut storage = Storage::new();
        let sender = storage.get_sender();
        let storage = thread::spawn(move || storage.run());

        sender.async_get(b"x".to_vec(), 0u64, Box::new(|x| println!("{:?}", x)));
        sender.close();

        storage.join();
    }
}
