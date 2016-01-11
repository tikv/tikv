use std::error;
use std::result;
use std::fmt::{self, Display, Formatter};
use self::engine::Engine;
pub use self::engine::Dsn;

mod engine;
mod mvcc;

pub struct Storage {
    engine: Box<Engine>,
}

impl Storage {
    pub fn new(desc: Dsn) -> Result<Storage> {
        engine::new_engine(desc).map(|e| Storage { engine: e }).map_err(|e| Error::from(e))
    }

    pub fn get(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>> {
        trace!("storage: get {:?}@{}", key, version);
        // TODO (disksing)
        self.engine.get(key).map_err(|e| Error::from(e))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], version: u64) -> Result<()> {
        trace!("storage: put {:?}@{}", key, version);
        // TODO (disksing)
        self.engine.put(key, value).map_err(|e| Error::from(e))
    }
}
