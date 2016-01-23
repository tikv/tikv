
use std::fmt;
use std::boxed::FnBox;
use super::{Key, Value, KvPair};
use super::mvcc::Result;

pub type Version = u64;
pub type Limit = usize;
pub type Puts = Vec<KvPair>;
pub type Deletes = Vec<Key>;
pub type Locks = Vec<Key>;

pub enum Command {
    Get(((Key, Version), Box<FnBox(Result<Option<Value>>) + Send>)),
    Scan(((Key, Limit, Version), Box<FnBox(Result<Vec<KvPair>>) + Send>)),
    Commit(((Puts, Deletes, Locks, Version), Box<FnBox(Result<()>) + Send>)),
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Command::Get(((ref key, version), _)) => {
                write!(f, "txn::command::get {:?} @ {}", key, version)
            }
            Command::Scan(((ref start_key, limit, version), _)) => {
                write!(f,
                       "txn::command::scan {:?}({}) @ {}",
                       start_key,
                       limit,
                       version)
            }
            Command::Commit(((ref puts, ref deletes, ref locks, version), _)) => {
                write!(f,
                       "txn::command::commit puts({}), deletes({}), locks({}) @ {}",
                       puts.len(),
                       deletes.len(),
                       locks.len(),
                       version)
            }
        }
    }
}

pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Get((_, callback)) => callback(Ok(None)),
            Command::Scan((_, callback)) => callback(Ok(vec![])),
            Command::Commit((_, callback)) => callback(Ok(())),
        }
    }
}
