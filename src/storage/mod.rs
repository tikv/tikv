mod engine;
mod mvcc;
mod txn;

use std::boxed::FnBox;

use mio::{EventLoop, Handler, Sender};

pub use self::engine::Dsn;
use self::engine::Engine;
use self::mvcc::{MvccEngine, Result};
use self::txn::{Command, Scheduler};

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
            handler: StorageHandler{scheduler: Scheduler::new()},
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
        let cmd = Command::Get(((key, version), callback));
        self.inner_sender.send(Message::Command((cmd))).unwrap();
    }

    pub fn async_scan(&self,
                      start_key: Key,
                      limit: usize,
                      version: u64,
                      callback: Box<FnBox(Result<Vec<KvPair>>) + Send>) {
        let cmd = Command::Scan(((start_key, limit, version), callback));
        self.inner_sender.send(Message::Command((cmd))).unwrap();
    }

    pub fn async_commit(&self,
                        puts: Vec<KvPair>,
                        deletes: Vec<Key>,
                        locks: Vec<Key>,
                        version: u64,
                        callback: Box<FnBox(Result<()>) + Send>) {
        let cmd = Command::Commit(((puts, deletes, locks, version), callback));
        self.inner_sender.send(Message::Command((cmd))).unwrap();
    }

    pub fn close(&self) {
        self.inner_sender.send(Message::Close).unwrap();
    }
}

#[derive(Debug)]
enum Message {
    Command(Command),
    Close,
}

struct StorageHandler {
    scheduler: Scheduler,
}

impl Handler for StorageHandler {
    type Timeout = ();
    type Message = Message;

    fn notify(&mut self, event_loop: &mut EventLoop<StorageHandler>, msg: Message) {
        debug!("recv message: {:?}", msg);
        match msg {
            Message::Command(cmd) => self.scheduler.handle_cmd(cmd),
            Message::Close => event_loop.shutdown(),
        }
    }
}
