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
        DB::open_default(path).map(|db| EngineRocksdb { db: db }).map_err(RocksDBError::new)
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
        self.db.get(key).map(|r| r.map(|v| v.to_vec())).map_err(RocksDBError::new)
    }

    fn seek(&self, key: RefKey) -> Result<Option<KvPair>> {
        trace!("EngineRocksdb: seek {:?}", key);
        let mode = IteratorMode::From(key, Direction::forward);
        let pair = self.db.iterator(mode).next().map(|(k, v)| (k.into_vec(), v.into_vec()));
        Ok(pair)
    }

    fn write(&self, batch: Vec<Modify>) -> Result<()> {
        let write_batch = WriteBatch::new();
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineRocksdb: delete {:?}", k);
                    if let Err(msg) = write_batch.delete(&k) {
                        return Err(RocksDBError::new(msg));
                    }
                }
                Modify::Put((k, v)) => {
                    trace!("EngineRocksdb: put {:?},{:?}", k, v);
                    if let Err(msg) = write_batch.put(&k, &v) {
                        return Err(RocksDBError::new(msg));
                    }
                }
            }
        }
        if let Err(msg) = self.db.write(write_batch) {
            return Err(RocksDBError::new(msg));
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
