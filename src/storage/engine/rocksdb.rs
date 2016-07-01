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
use rocksdb::{DB, Writable, SeekKey, WriteBatch, DBIterator, Options};
use rocksdb::rocksdb::Snapshot as RocksSnapshot;
use rocksdb::rocksdb_ffi::DBCFHandle;
use kvproto::kvrpcpb::Context;
use storage::{Key, Value, CfName};
use util::escape;
use super::{Engine, Snapshot, Modify, Cursor, TEMP_DIR, Result};
use tempdir::TempDir;


#[allow(dead_code)]
pub struct EngineRocksdb {
    db: DB,
    // only use for memory mode
    temp_dir: Option<TempDir>,
}

impl EngineRocksdb {
    pub fn new(path: &str, cfs: &[CfName]) -> Result<EngineRocksdb> {
        info!("EngineRocksdb: creating for path {}", path);
        let (path, temp_dir) = match path {
            TEMP_DIR => {
                let td = TempDir::new("temp-rocksdb").unwrap();
                (td.path().to_str().unwrap().to_owned(), Some(td))
            }
            _ => (path.to_owned(), None),
        };
        Ok(EngineRocksdb {
            db: try!(Self::open_or_create_db(&path, cfs)),
            temp_dir: temp_dir,
        })
    }

    fn open_or_create_db(path: &str, cfs: &[CfName]) -> Result<DB> {
        // Currently we support 1) Create new db. 2) Open a db with CFs we want. 3) Open db with no
        // CFs.
        // TODO: Support open db with incomplete CFs.

        let mut opts = Options::new();
        opts.create_if_missing(false);
        match DB::open_cf(&opts, path, cfs) {
            Ok(db) => return Ok(db),
            Err(e) => warn!("open rocksdb fail: {}", e),
        }

        opts.create_if_missing(true);
        let mut db = match DB::open(&opts, path) {
            Ok(db) => db,
            Err(e) => return Err(RocksDBError::new(e).into_engine_error()),
        };
        for cf in cfs {
            if let Err(e) = db.create_cf(cf, &opts) {
                return Err(RocksDBError::new(e).into_engine_error());
            }
        }
        Ok(db)
    }

    fn cf_handle(&self, cf: CfName) -> Result<&DBCFHandle> {
        self.db
            .cf_handle(cf)
            .ok_or(RocksDBError::new("cf not found.".to_string()).into_engine_error())
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
            .get(key.encoded())
            .map(|r| r.map(|v| v.to_vec()))
            .map_err(|e| RocksDBError::new(e).into_engine_error())
    }

    fn get_cf(&self, _: &Context, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("EngineRocksdb: get_cf {} {}", key, cf);
        let handle = try!(self.cf_handle(cf));
        self.db
            .get_cf(handle.to_owned(), key.encoded())
            .map(|r| r.map(|v| v.to_vec()))
            .map_err(|e| RocksDBError::new(e).into_engine_error())
    }

    fn write(&self, _: &Context, batch: Vec<Modify>) -> Result<()> {
        let wb = WriteBatch::new();
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineRocksdb: delete {}", k);
                    if let Err(msg) = wb.delete(k.encoded()) {
                        return Err(RocksDBError::new(msg).into_engine_error());
                    }
                }
                Modify::DeleteCf(cf, k) => {
                    trace!("EngineRocksdb: delete_cf {} {}", cf, k);
                    let handle = try!(self.cf_handle(cf));
                    if let Err(msg) = wb.delete_cf(handle.to_owned(), k.encoded()) {
                        return Err(RocksDBError::new(msg).into_engine_error());
                    }
                }
                Modify::Put((k, v)) => {
                    trace!("EngineRocksdb: put {},{}", k, escape(&v));
                    if let Err(msg) = wb.put(k.encoded(), &v) {
                        return Err(RocksDBError::new(msg).into_engine_error());
                    }
                }
                Modify::PutCf((cf, k, v)) => {
                    trace!("EngineRocksdb: put_cf {}, {}, {}", cf, k, escape(&v));
                    let handle = try!(self.cf_handle(cf));
                    if let Err(msg) = wb.put_cf(handle.to_owned(), k.encoded(), &v) {
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

    fn iter<'a>(&'a self, _: &Context) -> Result<Box<Cursor + 'a>> {
        Ok(box self.db.iter())
    }
}

impl<'a> Cursor for DBIterator<'a> {
    fn next(&mut self) -> bool {
        DBIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        DBIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek(self, key.encoded().as_slice().into()))
    }

    fn seek_to_first(&mut self) -> bool {
        DBIterator::seek(self, SeekKey::Start)
    }

    fn seek_to_last(&mut self) -> bool {
        DBIterator::seek(self, SeekKey::End)
    }

    fn valid(&self) -> bool {
        DBIterator::valid(self)
    }

    fn key(&self) -> &[u8] {
        DBIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        DBIterator::value(self)
    }
}

impl<'a> Snapshot for RocksSnapshot<'a> {
    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get {}", key);
        self.get(key.encoded())
            .map(|r| r.map(|v| v.to_vec()))
            .map_err(|e| RocksDBError::new(e).into_engine_error())
    }

    fn iter<'b>(&'b self) -> Result<Box<Cursor + 'b>> {
        trace!("RocksSnapshot: create iterator");
        Ok(box RocksSnapshot::iter(self))
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
