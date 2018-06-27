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
    BatchCallback, Callback, CbContext, Cursor, Engine, Error, Iterator as EngineIterator, Modify,
    Result, ScanMode, Snapshot, TEMP_DIR,
};
use kvproto::kvrpcpb::Context;
use raftstore::store::engine::{IterOption, Peekable, SyncSnapshot as RocksSnapshot};
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

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(Callback<Box<Snapshot>>),
    SnapshotBatch(usize, BatchCallback<Box<Snapshot>>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Write(..) => write!(f, "write task"),
            Task::Snapshot(_) => write!(f, "snapshot task"),
            Task::SnapshotBatch(..) => write!(f, "snapshot task batch"),
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
                Ok(box RocksSnapshot::new(Arc::clone(&self.0))),
            )),
            Task::SnapshotBatch(size, on_finished) => {
                let mut results = Vec::with_capacity(size);
                for _ in 0..size {
                    let res = Some((
                        CbContext::new(),
                        Ok(box RocksSnapshot::new(Arc::clone(&self.0)) as Box<Snapshot>),
                    ));
                    results.push(res);
                }
                on_finished(results);
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
    pub fn new(
        path: &str,
        cfs: &[CfName],
        cfs_opts: Option<Vec<CFOptions>>,
    ) -> Result<EngineRocksdb> {
        info!("EngineRocksdb: creating for path {}", path);
        let (path, temp_dir) = match path {
            TEMP_DIR => {
                let td = TempDir::new("temp-rocksdb").unwrap();
                (td.path().to_str().unwrap().to_owned(), Some(td))
            }
            _ => (path.to_owned(), None),
        };
        let mut worker = Worker::new("engine-rocksdb");
        let db = rocksdb::new_engine(&path, cfs, cfs_opts)?;
        box_try!(worker.start(Runner(Arc::new(db))));
        Ok(EngineRocksdb {
            sched: worker.scheduler(),
            core: Arc::new(Mutex::new(EngineRocksdbCore { temp_dir, worker })),
        })
    }

    pub fn stop(&self) {
        let mut core = self.core.lock().unwrap();
        if let Some(h) = core.worker.stop() {
            h.join().unwrap();
        }
    }
}

impl Debug for EngineRocksdb {
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
                trace!("EngineRocksdb: delete {}", k);
                wb.delete(k.encoded())
            } else {
                trace!("EngineRocksdb: delete_cf {} {}", cf, k);
                let handle = rocksdb::get_cf_handle(db, cf)?;
                wb.delete_cf(handle, k.encoded())
            },
            Modify::Put(cf, k, v) => if cf == CF_DEFAULT {
                trace!("EngineRocksdb: put {},{}", k, escape(&v));
                wb.put(k.encoded(), &v)
            } else {
                trace!("EngineRocksdb: put_cf {}, {}, {}", cf, k, escape(&v));
                let handle = rocksdb::get_cf_handle(db, cf)?;
                wb.put_cf(handle, k.encoded(), &v)
            },
            Modify::DeleteRange(cf, start_key, end_key) => {
                trace!(
                    "EngineRocksdb: delete_range_cf {}, {}, {}",
                    cf,
                    escape(start_key.encoded()),
                    escape(end_key.encoded())
                );
                let handle = rocksdb::get_cf_handle(db, cf)?;
                wb.delete_range_cf(handle, start_key.encoded(), end_key.encoded())
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
        if modifies.is_empty() {
            return Err(Error::EmptyRequest);
        }
        box_try!(self.sched.schedule(Task::Write(modifies, cb)));
        Ok(())
    }

    fn async_snapshot(&self, _: &Context, cb: Callback<Box<Snapshot>>) -> Result<()> {
        box_try!(self.sched.schedule(Task::Snapshot(cb)));
        Ok(())
    }

    fn async_batch_snapshot(
        &self,
        batch: Vec<Context>,
        on_finished: BatchCallback<Box<Snapshot>>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Err(Error::EmptyRequest);
        }
        box_try!(
            self.sched
                .schedule(Task::SnapshotBatch(batch.len(), on_finished))
        );
        Ok(())
    }

    fn clone_box(&self) -> Box<Engine> {
        box EngineRocksdb {
            core: Arc::clone(&self.core),
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

    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> Result<Cursor> {
        trace!("RocksSnapshot: create iterator");
        let iter = self.db_iterator(iter_opt);
        Ok(Cursor::new(Box::new(iter), mode))
    }

    #[allow(needless_lifetimes)]
    fn iter_cf(&self, cf: CfName, iter_opt: IterOption, mode: ScanMode) -> Result<Cursor> {
        trace!("RocksSnapshot: create cf iterator");
        let iter = self.db_iterator_cf(cf, iter_opt)?;
        Ok(Cursor::new(Box::new(iter), mode))
    }

    fn clone(&self) -> Box<Snapshot> {
        Box::new(RocksSnapshot::clone(self))
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
        Ok(DBIterator::seek(self, key.encoded().as_slice().into()))
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Ok(DBIterator::seek_for_prev(
            self,
            key.encoded().as_slice().into(),
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
}
