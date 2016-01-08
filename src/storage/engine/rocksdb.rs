extern crate rocksdb;

use self::rocksdb::{DB, Writable, IteratorMode, Direction};
use super::{Engine, Modify};

pub struct RocksEngine {
    db: DB,
}

impl RocksEngine {
    pub fn new(path: &str) -> Result<RocksEngine, String> {
        info!("RocksEngine: creating for path {}", path);
        DB::open_default(path).map(|db| RocksEngine { db: db })
    }
}

impl Engine for RocksEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        trace!("RocksEngine: get {:?}", key);
        self.db.get(key).map(|r| r.map(|v| v.to_owned()))
    }

    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, String> {
        trace!("RocksEngine: seek {:?}", key);
        let mode = IteratorMode::From(key, Direction::forward);
        let pair = self.db.iterator(mode).next().map(|(k, v)| (k.into_vec(), v.into_vec()));
        Ok(pair)
    }

    fn write(&mut self, batch: Vec<Modify>) -> Result<(), String> {
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("RocksEngine: delete {:?}", k);
                    try!(self.db.delete(k));
                }
                Modify::Put((k, v)) => {
                    trace!("RocksEngine: put {:?},{:?}", k, v);
                    try!(self.db.put(k, v));
                }
            }
        }
        Ok(())
    }
}
