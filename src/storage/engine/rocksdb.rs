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

use std::fmt::{self, Formatter, Debug};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use rocksdb::{DB, Writable, SeekKey, WriteBatch, DBIterator};
use kvproto::kvrpcpb::Context;
use storage::{Key, Value, CfName};
use raftstore::store::engine::{Snapshot as RocksSnapshot, Peekable, Iterable};
use util::escape;
use util::rocksdb;
use super::{Engine, Snapshot, Modify, Cursor, Callback, TEMP_DIR, Result, Error, DEFAULT_CFNAME};
use tempdir::TempDir;


pub struct EngineRocksdb {
    db: Arc<DB>,
    // only use for memory mode
    temp_dir: Option<TempDir>,
    pool: Mutex<ThreadPool>,
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
            db: Arc::new(try!(rocksdb::new_engine(&path, cfs))),
            temp_dir: temp_dir,
            pool: Mutex::new(ThreadPool::new_with_name(thd_name!("engine-rocksdb"), 1)),
        })
    }
}

impl Debug for EngineRocksdb {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Rocksdb [is_temp: {}]", self.temp_dir.is_some()) // TODO(disksing): print DSN
    }
}

fn write_modifies(db: &DB, modifies: Vec<Modify>) -> Result<()> {
    let wb = WriteBatch::new();
    for rev in modifies {
        let res = match rev {
            Modify::Delete(cf, k) => {
                if cf == DEFAULT_CFNAME {
                    trace!("EngineRocksdb: delete {}", k);
                    wb.delete(k.encoded())
                } else {
                    trace!("EngineRocksdb: delete_cf {} {}", cf, k);
                    let handle = try!(rocksdb::get_cf_handle(db, cf));
                    wb.delete_cf(*handle, k.encoded())
                }
            }
            Modify::Put(cf, k, v) => {
                if cf == DEFAULT_CFNAME {
                    trace!("EngineRocksdb: put {},{}", k, escape(&v));
                    wb.put(k.encoded(), &v)
                } else {
                    trace!("EngineRocksdb: put_cf {}, {}, {}", cf, k, escape(&v));
                    let handle = try!(rocksdb::get_cf_handle(db, cf));
                    wb.put_cf(*handle, k.encoded(), &v)
                }
            }
        };
        if let Err(msg) = res {
            return Err(Error::RocksDb(msg));
        }
    }
    if let Err(msg) = db.write(wb) {
        return Err(Error::RocksDb(msg));
    }
    Ok(())
}

impl Engine for EngineRocksdb {
    fn async_write(&self, _: &Context, modifies: Vec<Modify>, cb: Callback<()>) -> Result<()> {
        let db = self.db.clone();
        self.pool.lock().unwrap().execute(move || cb(write_modifies(&db, modifies)));
        Ok(())
    }

    fn async_snapshot(&self, _: &Context, cb: Callback<Box<Snapshot>>) -> Result<()> {
        let db = self.db.clone();
        self.pool.lock().unwrap().execute(move || {
            cb(Ok(box RocksSnapshot::new(db)));
        });
        Ok(())
    }
}

impl Snapshot for RocksSnapshot {
    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get {}", key);
        let v = box_try!(self.get_value(key.encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get_cf {} {}", cf, key);
        let v = box_try!(self.get_value_cf(cf, key.encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter<'b>(&'b self) -> Result<Box<Cursor + 'b>> {
        trace!("RocksSnapshot: create iterator");
        Ok(box self.new_iterator())
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
