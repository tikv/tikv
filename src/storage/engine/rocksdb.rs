use std::fmt::{self, Display, Formatter, Debug};
use std::error::Error;

use rocksdb::{DB, Writable, WriteBatch, IteratorMode, Direction};

use storage::{RefKey, Value, KvPair};
use super::{Engine, Modify, Result};

pub struct EngineRocksdb {
    db: DB,
}

impl EngineRocksdb {
    pub fn new(path: &str) -> Result<EngineRocksdb> {
        info!("EngineRocksdb: creating for path {}", path);
        DB::open_default(path)
            .map(|db| EngineRocksdb { db: db })
            .map_err(|e| RocksDBError::new(e).to_engine_error())
    }
}

impl Debug for EngineRocksdb {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Rocksdb") // TODO(disksing): print DSN
    }
}

impl Engine for EngineRocksdb {
    fn get(&self, key: RefKey) -> Result<Option<Value>> {
        trace!("EngineRocksdb: get {:?}", key);
        self.db
            .get(key)
            .map(|r| r.map(|v| v.to_vec()))
            .map_err(|e| RocksDBError::new(e).to_engine_error())
    }

    fn seek(&self, key: RefKey) -> Result<Option<KvPair>> {
        trace!("EngineRocksdb: seek {:?}", key);
        let mode = IteratorMode::From(key, Direction::forward);
        let pair = self.db.iterator(mode).next().map(|(k, v)| (k.into_vec(), v.into_vec()));
        Ok(pair)
    }

    fn write(&self, batch: Vec<Modify>) -> Result<()> {
        let wb = WriteBatch::new();
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineRocksdb: delete {:?}", k);
                    if let Err(msg) = wb.delete(&k) {
                        return Err(RocksDBError::new(msg).to_engine_error());
                    }
                }
                Modify::Put((k, v)) => {
                    trace!("EngineRocksdb: put {:?},{:?}", k, v);
                    if let Err(msg) = wb.put(&k, &v) {
                        return Err(RocksDBError::new(msg).to_engine_error());
                    }
                }
            }
        }
        if let Err(msg) = self.db.write(wb) {
            return Err(RocksDBError::new(msg).to_engine_error());
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RocksDBError {
    message: String,
}

impl RocksDBError {
    fn new(msg: String) -> RocksDBError {
        RocksDBError { message: msg }
    }

    fn to_engine_error(&self) -> super::Error {
        super::Error::Other(Box::new(self.clone()))
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
