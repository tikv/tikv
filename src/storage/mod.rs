use mio::{EventLoop, Handler, Sender};
use self::kv::Command;
use self::txn::Scheduler;

mod kv;
mod engine;
mod mvcc;
mod txn;

pub use self::kv::{Key, Value, KvPair, Version, Limit, Callback};

pub struct Storage {
    event_loop: EventLoop<StorageHandler>,
    handler: StorageHandler,
}

impl Storage {
    pub fn new() -> Result<Storage> {
        let event_loop = try!(EventLoop::new());
        Ok(Storage {
            event_loop: event_loop,
            handler: StorageHandler { scheduler: Scheduler::new() },
        })
    }

    pub fn get_sender(&self) -> StorageSender {
        StorageSender { inner_sender: self.event_loop.channel() }
    }

    pub fn run(&mut self) -> Result<()> {
        try!(self.event_loop.run(&mut self.handler));
        Ok(())
    }
}

pub struct StorageSender {
    inner_sender: Sender<Message>,
}

impl StorageSender {
    pub fn async_get(&self,
                     key: Key,
                     version: Version,
                     callback: Callback<Option<Value>>)
                     -> Result<()> {
        let cmd = Command::Get(((key, version), callback));
        self.inner_sender.send(Message::Command((cmd))).map_err(|e| Error::Other(Box::new(e)))
    }

    pub fn async_scan(&self,
                      start_key: Key,
                      limit: Limit,
                      version: Version,
                      callback: Callback<Vec<KvPair>>)
                      -> Result<()> {
        let cmd = Command::Scan(((start_key, limit, version), callback));
        self.inner_sender.send(Message::Command((cmd))).map_err(|e| Error::Other(Box::new(e)))
    }

    pub fn async_commit(&self,
                        puts: Vec<KvPair>,
                        deletes: Vec<Key>,
                        locks: Vec<Key>,
                        version: Version,
                        callback: Callback<()>)
                        -> Result<()> {
        let cmd = Command::Commit(((puts, deletes, locks, version), callback));
        self.inner_sender.send(Message::Command((cmd))).map_err(|e| Error::Other(Box::new(e)))
    }

    pub fn close(&self) -> Result<()> {
        self.inner_sender.send(Message::Close).map_err(|e| Error::Other(Box::new(e)))
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

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: ::std::io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Engine(err: engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Other(err: Box<::std::error::Error>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
