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

use super::{
    Callback, CbContext, Cursor, Engine, Error, Iterator as EngineIterator, Modify, Result,
    ScanMode, Snapshot, TEMP_DIR,
};
use kvproto::errorpb::Error as ErrorHeader;
use kvproto::kvrpcpb::Context;
use raftstore::store::engine::{IterOption, Peekable};
use rocksdb::{DBIterator, SeekKey, Writable, WriteBatch, DB};
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use storage::{CfName, Key, Value, CF_DEFAULT};
use tempdir::TempDir;
use util::escape;
use util::rocksdb;
use util::rocksdb::CFOptions;
use util::worker::{Runnable, Scheduler, Worker};

pub use raftstore::store::engine::SyncSnapshot as RocksSnapshot;

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(Callback<RocksSnapshot>),
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
            Task::Snapshot(cb) => cb((
                CbContext::new(),
                Ok(RocksSnapshot::new(Arc::clone(&self.0))),
            )),
        }
    }
}

struct RocksEngineCore {
    // only use for memory mode
    temp_dir: Option<TempDir>,
    worker: Worker<Task>,
}

impl Drop for RocksEngineCore {
    fn drop(&mut self) {
        if let Some(h) = self.worker.stop() {
            h.join().unwrap();
        }
    }
}

#[derive(Clone)]
pub struct RocksEngine {
    core: Arc<Mutex<RocksEngineCore>>,
    sched: Scheduler<Task>,
    db: Arc<DB>,
}

impl RocksEngine {
    pub fn new(
        path: &str,
        cfs: &[CfName],
        cfs_opts: Option<Vec<CFOptions>>,
    ) -> Result<RocksEngine> {
        info!("RocksEngine: creating for path {}", path);
        let (path, temp_dir) = match path {
            TEMP_DIR => {
                let td = TempDir::new("temp-rocksdb").unwrap();
                (td.path().to_str().unwrap().to_owned(), Some(td))
            }
            _ => (path.to_owned(), None),
        };
        let mut worker = Worker::new("engine-rocksdb");
        let db = Arc::new(rocksdb::new_engine(&path, cfs, cfs_opts)?);
        box_try!(worker.start(Runner(Arc::clone(&db))));
        Ok(RocksEngine {
            sched: worker.scheduler(),
            core: Arc::new(Mutex::new(RocksEngineCore { temp_dir, worker })),
            db,
        })
    }

    pub fn get_rocksdb(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }

    pub fn stop(&self) {
        let mut core = self.core.lock().unwrap();
        if let Some(h) = core.worker.stop() {
            h.join().unwrap();
        }
    }
}

impl Debug for RocksEngine {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Rocksdb [is_temp: {}]",
            self.core.lock().unwrap().temp_dir.is_some()
        )
    }
}

fn write_modifies(db: &DB, modifies: Vec<Modify>) -> Result<()> {
    let wb = WriteBatch::new();
    for rev in modifies {
        let res = match rev {
            Modify::Delete(cf, k) => if cf == CF_DEFAULT {
                trace!("RocksEngine: delete {}", k);
                wb.delete(k.as_encoded())
            } else {
                trace!("RocksEngine: delete_cf {} {}", cf, k);
                let handle = rocksdb::get_cf_handle(db, cf)?;
                wb.delete_cf(handle, k.as_encoded())
            },
            Modify::Put(cf, k, v) => if cf == CF_DEFAULT {
                trace!("RocksEngine: put {},{}", k, escape(&v));
                wb.put(k.as_encoded(), &v)
            } else {
                trace!("RocksEngine: put_cf {}, {}, {}", cf, k, escape(&v));
                let handle = rocksdb::get_cf_handle(db, cf)?;
                wb.put_cf(handle, k.as_encoded(), &v)
            },
            Modify::DeleteRange(cf, start_key, end_key) => {
                trace!(
                    "RocksEngine: delete_range_cf {}, {}, {}",
                    cf,
                    escape(start_key.as_encoded()),
                    escape(end_key.as_encoded())
                );
                let handle = rocksdb::get_cf_handle(db, cf)?;
                wb.delete_range_cf(handle, start_key.as_encoded(), end_key.as_encoded())
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

impl Engine for RocksEngine {
    type Iter = DBIterator<Arc<DB>>;
    type Snap = RocksSnapshot;

    fn async_write(&self, _: &Context, modifies: Vec<Modify>, cb: Callback<()>) -> Result<()> {
        if modifies.is_empty() {
            return Err(Error::EmptyRequest);
        }
        box_try!(self.sched.schedule(Task::Write(modifies, cb)));
        Ok(())
    }

    fn async_snapshot(&self, _: &Context, cb: Callback<Self::Snap>) -> Result<()> {
        fail_point!("rockskv_async_snapshot", |_| Err(box_err!(
            "snapshot failed"
        )));
        fail_point!("rockskv_async_snapshot_not_leader", |_| {
            let mut header = ErrorHeader::new();
            header.mut_not_leader().set_region_id(100);
            Err(Error::Request(header))
        });
        box_try!(self.sched.schedule(Task::Snapshot(cb)));
        Ok(())
    }
}

impl Snapshot for RocksSnapshot {
    type Iter = DBIterator<Arc<DB>>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get {}", key);
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get_cf {} {}", cf, key);
        let v = box_try!(self.get_value_cf(cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> Result<Cursor<Self::Iter>> {
        trace!("RocksSnapshot: create iterator");
        let iter = self.db_iterator(iter_opt);
        Ok(Cursor::new(iter, mode))
    }

    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOption,
        mode: ScanMode,
    ) -> Result<Cursor<Self::Iter>> {
        trace!("RocksSnapshot: create cf iterator");
        let iter = self.db_iterator_cf(cf, iter_opt)?;
        Ok(Cursor::new(iter, mode))
    }
}

impl<D: Deref<Target = DB> + Send> EngineIterator for DBIterator<D> {
    fn next(&mut self) -> bool {
        DBIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        DBIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek(self, key.as_encoded().as_slice().into()))
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek_for_prev(
            self,
            key.as_encoded().as_slice().into(),
        ))
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

    fn status(&self) -> Result<()> {
        DBIterator::status(self).map_err(From::from)
    }
}
