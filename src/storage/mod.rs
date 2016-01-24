use std::thread::{self, JoinHandle};
use mio::{EventLoop, Handler, Sender, NotifyError};
use self::kv::Command;
use self::txn::Scheduler;

mod kv;
mod engine;
mod mvcc;
mod txn;

pub use self::kv::{Key, Value, KvPair, Version, Limit, Callback};
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
        let thread_handle = thread::spawn(move || event_loop.run(&mut scheduler).map_err(|e| Error::Io(e)));
        Ok(Storage{
            sender: sender,
            thread: thread_handle,
        })
    }

    pub fn stop(self) -> Result<()> {
        try!(self.sender.send(Message::Close));
        self.thread.join().unwrap().unwrap(); // TODO(disksing): check error
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
        Txn(err: txn::Error) {
            from()
            cause(err)
            description(err.description())
        }

        AlreadyStarted {description("already started")}
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
