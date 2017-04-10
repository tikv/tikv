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

use std::fmt::{self, Formatter, Debug, Display};
use std::sync::{Arc, Mutex};
use rocksdb::{DB, Writable, SeekKey, WriteBatch, DBIterator};
use kvproto::kvrpcpb::Context;
use storage::{Key, Value, CfName, CF_DEFAULT};
use raftstore::store::engine::{SyncSnapshot as RocksSnapshot, Peekable, Iterable, IterOption};
use util::escape;
use util::rocksdb;
use util::worker::{Runnable, Worker, Scheduler};
use super::{Engine, Snapshot, Modify, Cursor, Iterator as EngineIterator, Callback, TEMP_DIR,
            ScanMode, Result, Error, CbContext};
use tempdir::TempDir;

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(Callback<Box<Snapshot>>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Write(..) => write!(f, "write task"),
            Task::Snapshot(_) => write!(f, "snapshot task"),
        }
    }
}

struct Runner(Arc<DB>);

impl Runnable<Task> for Runner {
    fn run(&mut self, t: Task) {
        match t {
            Task::Write(modifies, cb) => cb((CbContext::new(), write_modifies(&self.0, modifies))),
            Task::Snapshot(cb) => {
                cb((CbContext::new(), Ok(box RocksSnapshot::new(self.0.clone()))))
            }
        }
    }
}

struct EngineRocksdbCore {
    // only use for memory mode
    temp_dir: Option<TempDir>,
    worker: Worker<Task>,
}

impl Drop for EngineRocksdbCore {
    fn drop(&mut self) {
        if let Some(h) = self.worker.stop() {
            h.join().unwrap();
        }
    }
}

pub struct EngineRocksdb {
    core: Arc<Mutex<EngineRocksdbCore>>,
    sched: Scheduler<Task>,
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
        let mut worker = Worker::new("engine-rocksdb");
        let db = try!(rocksdb::new_engine(&path, cfs));
        box_try!(worker.start(Runner(Arc::new(db))));
        Ok(EngineRocksdb {
               sched: worker.scheduler(),
               core: Arc::new(Mutex::new(EngineRocksdbCore {
                                             temp_dir: temp_dir,
                                             worker: worker,
                                         })),
           })
    }
}

impl Debug for EngineRocksdb {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "Rocksdb [is_temp: {}]",
               self.core.lock().unwrap().temp_dir.is_some())
    }
}

fn write_modifies(db: &DB, modifies: Vec<Modify>) -> Result<()> {
    let wb = WriteBatch::new();
    for rev in modifies {
        let res = match rev {
            Modify::Delete(cf, k) => {
                if cf == CF_DEFAULT {
                    trace!("EngineRocksdb: delete {}", k);
                    wb.delete(k.encoded())
                } else {
                    trace!("EngineRocksdb: delete_cf {} {}", cf, k);
                    let handle = try!(rocksdb::get_cf_handle(db, cf));
                    wb.delete_cf(handle, k.encoded())
                }
            }
            Modify::Put(cf, k, v) => {
                if cf == CF_DEFAULT {
                    trace!("EngineRocksdb: put {},{}", k, escape(&v));
                    wb.put(k.encoded(), &v)
                } else {
                    trace!("EngineRocksdb: put_cf {}, {}, {}", cf, k, escape(&v));
                    let handle = try!(rocksdb::get_cf_handle(db, cf));
                    wb.put_cf(handle, k.encoded(), &v)
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
        box_try!(self.sched.schedule(Task::Write(modifies, cb)));
        Ok(())
    }

    fn async_snapshot(&self, _: &Context, cb: Callback<Box<Snapshot>>) -> Result<()> {
        box_try!(self.sched.schedule(Task::Snapshot(cb)));
        Ok(())
    }

    fn clone(&self) -> Box<Engine> {
        box EngineRocksdb {
                core: self.core.clone(),
                sched: self.sched.clone(),
            }
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

    #[allow(needless_lifetimes)]
    fn iter<'b>(&'b self, iter_opt: IterOption, mode: ScanMode) -> Result<Cursor<'b>> {
        trace!("RocksSnapshot: create iterator");
        Ok(Cursor::new(self.new_iterator(iter_opt), mode))
    }

    #[allow(needless_lifetimes)]
    fn iter_cf<'b>(&'b self,
                   cf: CfName,
                   iter_opt: IterOption,
                   mode: ScanMode)
                   -> Result<Cursor<'b>> {
        trace!("RocksSnapshot: create cf iterator");
        Ok(Cursor::new(try!(self.new_iterator_cf(cf, iter_opt)), mode))
    }

    fn clone(&self) -> Box<Snapshot> {
        Box::new(RocksSnapshot::clone(self))
    }
}

impl<'a> EngineIterator for DBIterator<'a> {
    fn next(&mut self) -> bool {
        DBIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        DBIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek(self, key.encoded().as_slice().into()))
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek_for_prev(self, key.encoded().as_slice().into()))
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
