mod engine;
mod mvcc;

pub use self::engine::Dsn;

use self::engine::Engine;
use self::mvcc::{MvccEngine, Result};

pub struct Storage {
    engine: Box<Engine>,
}

impl Storage {
    pub fn new(desc: Dsn) -> Result<Storage> {
        let eng = try!(engine::new_engine(desc));
        Ok(Storage { engine: eng })
    }

    pub fn get(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>> {
        trace!("storage: get {:?}@{}", key, version);
        self.engine.as_ref().mvcc_get(key, version)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], version: u64) -> Result<()> {
        trace!("storage: put {:?}@{}", key, version);
        self.engine.as_mut().mvcc_put(key, value, version)
    }

    pub fn delete(&mut self, key: &[u8], version: u64) -> Result<()> {
        trace!("storage: delete {:?}@{}", key, version);
        self.engine.as_mut().mvcc_delete(key, version)
    }

    pub fn scan(&self,
                start_key: &[u8],
                limit: usize,
                version: u64)
                -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        trace!("storage: scan {:?}({})@{}", start_key, limit, version);
        self.engine.as_ref().mvcc_scan(start_key, limit, version)
    }
}
