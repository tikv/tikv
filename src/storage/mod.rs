use self::engine::Engine;
pub use self::engine::Dsn;
use self::mvcc::Result;

mod engine;
mod mvcc;

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
        mvcc::get(&*self.engine, key, version)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], version: u64) -> Result<()> {
        trace!("storage: put {:?}@{}", key, version);
        mvcc::put(&mut *self.engine, key, value, version)
    }

    pub fn delete(&mut self, key: &[u8], version: u64) -> Result<()> {
        trace!("storage: delete {:?}@{}", key, version);
        mvcc::delete(&mut *self.engine, key, version)
    }

    pub fn scan(&self,
                start_key: &[u8],
                limit: usize,
                version: u64)
                -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        trace!("storage: scan {:?}({})@{}", start_key, limit, version);
        mvcc::scan(&*self.engine, start_key, limit, version)
    }
}
