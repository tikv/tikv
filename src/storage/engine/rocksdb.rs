// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{self, Display, Formatter, Debug};
use std::error::Error;
use rocksdb::{DB, Writable, WriteBatch, IteratorMode, Direction};
use rocksdb::rocksdb::Snapshot as RocksSnapshot;
use kvproto::kvrpcpb::Context;
use storage::{Key, Value, KvPair};
use util::escape;
use super::{Engine, Snapshot, Modify, Result};

pub struct EngineRocksdb {
    db: DB,
}

impl EngineRocksdb {
    pub fn new(path: &str) -> Result<EngineRocksdb> {
        info!("EngineRocksdb: creating for path {}", path);
        DB::open_default(path)
            .map(|db| EngineRocksdb { db: db })
            .map_err(|e| RocksDBError::new(e).into_engine_error())
    }
}

impl Debug for EngineRocksdb {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Rocksdb") // TODO(disksing): print DSN
    }
}

impl Engine for EngineRocksdb {
    fn get(&self, _: &Context, key: &Key) -> Result<Option<Value>> {
        trace!("EngineRocksdb: get {}", key);
        self.db
            .get(key.raw())
            .map(|r| r.map(|v| v.to_vec()))
            .map_err(|e| RocksDBError::new(e).into_engine_error())
    }

    fn seek(&self, _: &Context, key: &Key) -> Result<Option<KvPair>> {
        trace!("EngineRocksdb: seek {}", key);
        let mode = IteratorMode::From(key.raw(), Direction::Forward);
        let pair = self.db.iterator(mode).next().map(|(k, v)| (k.to_vec(), v.to_vec()));
        Ok(pair)
    }

    fn write(&self, _: &Context, batch: Vec<Modify>) -> Result<()> {
        let wb = WriteBatch::new();
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineRocksdb: delete {}", k);
                    if let Err(msg) = wb.delete(k.raw()) {
                        return Err(RocksDBError::new(msg).into_engine_error());
                    }
                }
                Modify::Put((k, v)) => {
                    trace!("EngineRocksdb: put {},{}", k, escape(&v));
                    if let Err(msg) = wb.put(k.raw(), &v) {
                        return Err(RocksDBError::new(msg).into_engine_error());
                    }
                }
            }
        }
        if let Err(msg) = self.db.write(wb) {
            return Err(RocksDBError::new(msg).into_engine_error());
        }
        Ok(())
    }

    fn snapshot<'a>(&'a self, _: &Context) -> Result<Box<Snapshot + 'a>> {
        let snapshot = RocksSnapshot::new(&self.db);
        Ok(box snapshot)
    }
}

impl<'a> Snapshot for RocksSnapshot<'a> {
    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get {}", key);
        self.get(key.raw())
            .map(|r| r.map(|v| v.to_vec()))
            .map_err(|e| RocksDBError::new(e).into_engine_error())
    }

    fn seek(&self, key: &Key) -> Result<Option<KvPair>> {
        trace!("RocksSnapshot: seek {}", key);
        let mode = IteratorMode::From(key.raw(), Direction::Forward);
        let pair = self.iterator(mode).next().map(|(k, v)| (k.to_vec(), v.to_vec()));
        Ok(pair)
    }

    fn reverse_seek(&self, key: &Key) -> Result<Option<KvPair>> {
        trace!("RocksSnapshot: seek {}", key);
        let mut iter = self.iterator(IteratorMode::From(key.raw(), Direction::Reverse));
        // iter will be positioned at `key` or the kv pair after it. If no such key exists, we need
        // locate it to the end.
        if !iter.valid() {
            iter = self.iterator(IteratorMode::End);
        }
        let pair = iter.skip_while(|&(k, _)| k >= key.raw())
                       .next()
                       .map(|(k, v)| (k.to_vec(), v.to_vec()));
        Ok(pair)
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

    fn into_engine_error(self) -> super::Error {
        super::Error::Other(Box::new(self))
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
