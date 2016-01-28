use std::fmt::{self, Display, Formatter};
use std::error::Error;

use rocksdb::{DB, Writable, IteratorMode, Direction};

use super::{Engine, Modify, Result};

pub struct EngineRocksdb {
    db: DB,
}

impl EngineRocksdb {
    pub fn new(path: &str) -> Result<EngineRocksdb> {
        info!("EngineRocksdb: creating for path {}", path);
        DB::open_default(path).map(|db| EngineRocksdb { db: db }).map_err(|e| RocksDBError::new(e))
    }
}

impl Engine for EngineRocksdb {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        trace!("EngineRocksdb: get {:?}", key);
        self.db.get(key).map(|r| r.map(|v| v.to_owned())).map_err(|e| RocksDBError::new(e))
    }

    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        trace!("EngineRocksdb: seek {:?}", key);
        let mode = IteratorMode::From(key, Direction::forward);
        let pair = self.db.iterator(mode).next().map(|(k, v)| (k.into_vec(), v.into_vec()));
        Ok(pair)
    }

    fn write(&self, batch: Vec<Modify>) -> Result<()> {
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineRocksdb: delete {:?}", k);
                    if let Err(msg) = self.db.delete(k) {
                        return Err(RocksDBError::new(msg));
                    }
                }
                Modify::Put((k, v)) => {
                    trace!("EngineRocksdb: put {:?},{:?}", k, v);
                    if let Err(msg) = self.db.put(k, v) {
                        return Err(RocksDBError::new(msg));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct RocksDBError {
    message: String,
}

impl RocksDBError {
    fn new(msg: String) -> super::Error {
        super::Error::Other(Box::new(RocksDBError { message: msg }))
    }
}

impl Display for RocksDBError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for RocksDBError {
    fn description(&self) -> &str {
        &self.message
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
