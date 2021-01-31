// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine_rocks::file_system::get_env as get_inspected_env;
use engine_rocks::raw::DBOptions;
use engine_rocks::raw_util::CFOptions;
use engine_rocks::{RocksEngine as BaseRocksEngine, RocksEngineIterator};
use engine_traits::{CfName, CF_DEFAULT};
use engine_traits::{
    Engines, IterOptions, Iterable, Iterator, KvEngine, Mutable, Peekable, ReadOptions, SeekKey,
    WriteBatch, WriteBatchExt,
};
use kvproto::kvrpcpb::Context;
use tempfile::{Builder, TempDir};
use txn_types::{Key, Value};

use tikv_util::escape;
use tikv_util::worker::{Runnable, Scheduler, Worker};

use super::{
    Callback, CbContext, Engine, Error, ErrorInner, ExtCallback, Iterator as EngineIterator,
    Modify, Result, SnapContext, Snapshot, WriteData,
};

pub use engine_rocks::RocksSnapshot;

// Duplicated in test_engine_builder
const TEMP_DIR: &str = "";

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(Callback<Arc<RocksSnapshot>>),
    Pause(Duration),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Write(..) => write!(f, "write task"),
            Task::Snapshot(_) => write!(f, "snapshot task"),
            Task::Pause(_) => write!(f, "pause"),
        }
    }
}

struct Runner(Engines<BaseRocksEngine, BaseRocksEngine>);

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, t: Task) {
        match t {
            Task::Write(modifies, cb) => {
                cb((CbContext::new(), write_modifies(&self.0.kv, modifies)))
            }
            Task::Snapshot(cb) => cb((CbContext::new(), Ok(Arc::new(self.0.kv.snapshot())))),
            Task::Pause(dur) => std::thread::sleep(dur),
        }
    }
}

struct RocksEngineCore {
    // only use for memory mode
    temp_dir: Option<TempDir>,
    worker: Worker,
}

impl Drop for RocksEngineCore {
    fn drop(&mut self) {
        self.worker.stop();
    }
}

/// The RocksEngine is based on `RocksDB`.
///
/// This is intended for **testing use only**.
#[derive(Clone)]
pub struct RocksEngine {
    core: Arc<Mutex<RocksEngineCore>>,
    sched: Scheduler<Task>,
    engines: Engines<BaseRocksEngine, BaseRocksEngine>,
    not_leader: Arc<AtomicBool>,
}

impl RocksEngine {
    pub fn new(
        path: &str,
        cfs: &[CfName],
        cfs_opts: Option<Vec<CFOptions<'_>>>,
        shared_block_cache: bool,
    ) -> Result<RocksEngine> {
        info!("RocksEngine: creating for path"; "path" => path);
        let (path, temp_dir) = match path {
            TEMP_DIR => {
                let td = Builder::new().prefix("temp-rocksdb").tempdir().unwrap();
                (td.path().to_str().unwrap().to_owned(), Some(td))
            }
            _ => (path.to_owned(), None),
        };
        let worker = Worker::new("engine-rocksdb");
        let mut db_opts = DBOptions::new();
        let env = get_inspected_env(None).unwrap();
        db_opts.set_env(env);
        let db = Arc::new(engine_rocks::raw_util::new_engine(
            &path,
            Some(db_opts),
            cfs,
            cfs_opts,
        )?);
        // It does not use the raft_engine, so it is ok to fill with the same
        // rocksdb.
        let mut kv_engine = BaseRocksEngine::from_db(db.clone());
        let mut raft_engine = BaseRocksEngine::from_db(db);
        kv_engine.set_shared_block_cache(shared_block_cache);
        raft_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, raft_engine);
        let sched = worker.start("engine-rocksdb", Runner(engines.clone()));
        Ok(RocksEngine {
            sched,
            core: Arc::new(Mutex::new(RocksEngineCore { temp_dir, worker })),
            not_leader: Arc::new(AtomicBool::new(false)),
            engines,
        })
    }

    pub fn trigger_not_leader(&self) {
        self.not_leader.store(true, Ordering::SeqCst);
    }

    pub fn pause(&self, dur: Duration) {
        self.sched.schedule(Task::Pause(dur)).unwrap();
    }

    pub fn engines(&self) -> Engines<BaseRocksEngine, BaseRocksEngine> {
        self.engines.clone()
    }

    pub fn get_rocksdb(&self) -> BaseRocksEngine {
        self.engines.kv.clone()
    }

    pub fn stop(&self) {
        let core = self.core.lock().unwrap();
        core.worker.stop();
    }
}

impl Display for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RocksDB")
    }
}

impl Debug for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RocksDB [is_temp: {}]",
            self.core.lock().unwrap().temp_dir.is_some()
        )
    }
}

/// Write modifications into a `BaseRocksEngine` instance.
pub fn write_modifies(kv_engine: &BaseRocksEngine, modifies: Vec<Modify>) -> Result<()> {
    fail_point!("rockskv_write_modifies", |_| Err(box_err!("write failed")));

    let mut wb = kv_engine.write_batch();
    for rev in modifies {
        let res = match rev {
            Modify::Delete(cf, k) => {
                if cf == CF_DEFAULT {
                    trace!("RocksEngine: delete"; "key" => %k);
                    wb.delete(k.as_encoded())
                } else {
                    trace!("RocksEngine: delete_cf"; "cf" => cf, "key" => %k);
                    wb.delete_cf(cf, k.as_encoded())
                }
            }
            Modify::Put(cf, k, v) => {
                if cf == CF_DEFAULT {
                    trace!("RocksEngine: put"; "key" => %k, "value" => escape(&v));
                    wb.put(k.as_encoded(), &v)
                } else {
                    trace!("RocksEngine: put_cf"; "cf" => cf, "key" => %k, "value" => escape(&v));
                    wb.put_cf(cf, k.as_encoded(), &v)
                }
            }
            Modify::DeleteRange(cf, start_key, end_key, notify_only) => {
                trace!(
                    "RocksEngine: delete_range_cf";
                    "cf" => cf,
                    "start_key" => %start_key,
                    "end_key" => %end_key,
                    "notify_only" => notify_only,
                );
                if !notify_only {
                    wb.delete_range_cf(cf, start_key.as_encoded(), end_key.as_encoded())
                } else {
                    Ok(())
                }
            }
        };
        // TODO: turn the error into an engine error.
        if let Err(msg) = res {
            return Err(box_err!("{}", msg));
        }
    }
    wb.write()?;
    Ok(())
}

impl Engine for RocksEngine {
    type Snap = Arc<RocksSnapshot>;
    type Local = BaseRocksEngine;

    fn kv_engine(&self) -> BaseRocksEngine {
        self.engines.kv.clone()
    }

    fn snapshot_on_kv_engine(&self, _: &[u8], _: &[u8]) -> Result<Self::Snap> {
        self.snapshot(Default::default())
    }

    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> Result<()> {
        write_modifies(&self.engines.kv, modifies)
    }

    fn async_write(&self, ctx: &Context, batch: WriteData, cb: Callback<()>) -> Result<()> {
        self.async_write_ext(ctx, batch, cb, None, None)
    }

    fn async_write_ext(
        &self,
        _: &Context,
        batch: WriteData,
        cb: Callback<()>,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> Result<()> {
        fail_point!("rockskv_async_write", |_| Err(box_err!("write failed")));

        if batch.modifies.is_empty() {
            return Err(Error::from(ErrorInner::EmptyRequest));
        }
        if let Some(cb) = proposed_cb {
            cb();
        }
        if let Some(cb) = committed_cb {
            cb();
        }
        box_try!(self.sched.schedule(Task::Write(batch.modifies, cb)));
        Ok(())
    }

    fn async_snapshot(&self, _: SnapContext<'_>, cb: Callback<Self::Snap>) -> Result<()> {
        fail_point!("rockskv_async_snapshot", |_| Err(box_err!(
            "snapshot failed"
        )));
        let not_leader = {
            let mut header = kvproto::errorpb::Error::default();
            header.mut_not_leader().set_region_id(100);
            header
        };
        fail_point!("rockskv_async_snapshot_not_leader", |_| {
            Err(Error::from(ErrorInner::Request(not_leader.clone())))
        });
        if self.not_leader.load(Ordering::SeqCst) {
            return Err(Error::from(ErrorInner::Request(not_leader)));
        }
        box_try!(self.sched.schedule(Task::Snapshot(cb)));
        Ok(())
    }
}

impl Snapshot for Arc<RocksSnapshot> {
    type Iter = RocksEngineIterator;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get"; "key" => %key);
        let v = self.get_value(key.as_encoded())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get_cf"; "cf" => cf, "key" => %key);
        let v = self.get_value_cf(cf, key.as_encoded())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get_cf"; "cf" => cf, "key" => %key);
        let v = self.get_value_cf_opt(&opts, cf, key.as_encoded())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter> {
        trace!("RocksSnapshot: create iterator");
        Ok(self.iterator_opt(iter_opt)?)
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        trace!("RocksSnapshot: create cf iterator");
        Ok(self.iterator_cf_opt(cf, iter_opt)?)
    }
}

impl EngineIterator for RocksEngineIterator {
    fn next(&mut self) -> Result<bool> {
        Iterator::next(self).map_err(Error::from)
    }

    fn prev(&mut self) -> Result<bool> {
        Iterator::prev(self).map_err(Error::from)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        Iterator::seek(self, key.as_encoded().as_slice().into()).map_err(Error::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Iterator::seek_for_prev(self, key.as_encoded().as_slice().into()).map_err(Error::from)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        Iterator::seek(self, SeekKey::Start).map_err(Error::from)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        Iterator::seek(self, SeekKey::End).map_err(Error::from)
    }

    fn valid(&self) -> Result<bool> {
        Iterator::valid(self).map_err(Error::from)
    }

    fn key(&self) -> &[u8] {
        Iterator::key(self)
    }

    fn value(&self) -> &[u8] {
        Iterator::value(self)
    }
}

