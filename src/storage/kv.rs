use std::fmt;
use std::boxed::FnBox;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Clone, Debug)]
pub struct KvPair {
    key: Key,
    value: Value,
}

pub type Version = u64;
pub type Limit = usize;
pub type Puts = Vec<KvPair>;
pub type Deletes = Vec<Key>;
pub type Locks = Vec<Key>;
pub type Callback<T> = Box<FnBox(super::Result<T>) + Send>;

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
