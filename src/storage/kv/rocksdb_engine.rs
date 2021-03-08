// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine_rocks::file_system::get_env as get_inspected_env;
use engine_rocks::raw::{ColumnFamilyOptions, DBOptions};
use engine_rocks::raw_util::CFOptions;
use engine_rocks::{RocksEngine as BaseRocksEngine, RocksEngineIterator};
use engine_traits::{CfName, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use engine_traits::{
    Engines, IterOptions, Iterable, Iterator, KvEngine, Mutable, Peekable, ReadOptions, SeekKey,
    WriteBatch, WriteBatchExt,
};
use kvproto::kvrpcpb::Context;
use tempfile::{Builder, TempDir};
use txn_types::{Key, Value};

use crate::storage::config::BlockCacheConfig;
use tikv_util::escape;
use tikv_util::worker::{Runnable, Scheduler, Worker};

use super::{
    Callback, CbContext, Engine, Error, ErrorInner, ExtCallback, Iterator as EngineIterator,
    Modify, Result, SnapContext, Snapshot, WriteData,
};

pub use engine_rocks::RocksSnapshot;

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

/// A builder to build a temporary `RocksEngine`.
///
/// Only used for test purpose.
#[must_use]
pub struct TestEngineBuilder {
    path: Option<PathBuf>,
    cfs: Option<Vec<CfName>>,
}

impl TestEngineBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            cfs: None,
        }
    }

    /// Customize the data directory of the temporary engine.
    ///
    /// By default, TEMP_DIR will be used.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Customize the CFs that engine will have.
    ///
    /// By default, engine will have all CFs.
    pub fn cfs(mut self, cfs: impl AsRef<[CfName]>) -> Self {
        self.cfs = Some(cfs.as_ref().to_vec());
        self
    }

    /// Build a `RocksEngine`.
    pub fn build(self) -> Result<RocksEngine> {
        let cfg_rocksdb = crate::config::DbConfig::default();
        self.build_with_cfg(&cfg_rocksdb)
    }

    pub fn build_with_cfg(self, cfg_rocksdb: &crate::config::DbConfig) -> Result<RocksEngine> {
        let path = match self.path {
            None => TEMP_DIR.to_owned(),
            Some(p) => p.to_str().unwrap().to_owned(),
        };
        let cfs = self.cfs.unwrap_or_else(|| crate::storage::ALL_CFS.to_vec());
        let cache = BlockCacheConfig::default().build_shared_cache();
        let cfs_opts = cfs
            .iter()
            .map(|cf| match *cf {
                CF_DEFAULT => {
                    CFOptions::new(CF_DEFAULT, cfg_rocksdb.defaultcf.build_opt(&cache, None))
                }
                CF_LOCK => CFOptions::new(CF_LOCK, cfg_rocksdb.lockcf.build_opt(&cache)),
                CF_WRITE => CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt(&cache, None)),
                CF_RAFT => CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt(&cache)),
                _ => CFOptions::new(*cf, ColumnFamilyOptions::new()),
            })
            .collect();
        RocksEngine::new(&path, &cfs, Some(cfs_opts), cache.is_some())
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

#[cfg(test)]
mod tests {
    use super::super::perf_context::PerfStatisticsInstant;
    use super::super::tests::*;
    use super::super::CfStatistics;
    use super::*;
    use crate::storage::{Cursor, CursorBuilder, ScanMode};
    use txn_types::TimeStamp;

    #[test]
    fn test_rocksdb() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_base_curd_options(&engine)
    }

    #[test]
    fn test_rocksdb_linear() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_linear(&engine);
    }

    #[test]
    fn test_rocksdb_statistic() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_cfs_statistics(&engine);
    }

    #[test]
    fn rocksdb_reopen() {
        let dir = tempfile::Builder::new()
            .prefix("rocksdb_test")
            .tempdir()
            .unwrap();
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            must_put_cf(&engine, "cf", b"k", b"v1");
        }
        {
            let engine = TestEngineBuilder::new()
                .path(dir.path())
                .cfs(TEST_ENGINE_CFS)
                .build()
                .unwrap();
            assert_has_cf(&engine, "cf", b"k", b"v1");
        }
    }

    #[test]
    fn test_rocksdb_perf_statistics() {
        let engine = TestEngineBuilder::new()
            .cfs(TEST_ENGINE_CFS)
            .build()
            .unwrap();
        test_perf_statistics(&engine);
    }

    #[test]
    fn test_max_skippable_internal_keys_error() {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_put(&engine, b"foo", b"bar");
        must_delete(&engine, b"foo");
        must_put(&engine, b"foo1", b"bar1");
        must_delete(&engine, b"foo1");
        must_put(&engine, b"foo2", b"bar2");

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let iter_opt = IterOptions::default().set_max_skippable_internal_keys(1);
        let mut iter = Cursor::new(snapshot.iter(iter_opt).unwrap(), ScanMode::Forward, false);

        let mut statistics = CfStatistics::default();
        let res = iter.seek(&Key::from_raw(b"foo"), &mut statistics);
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Result incomplete: Too many internal keys skipped"));
    }

    fn test_perf_statistics<E: Engine>(engine: &E) {
        must_put(engine, b"foo", b"bar1");
        must_put(engine, b"foo2", b"bar2");
        must_put(engine, b"foo3", b"bar3"); // deleted
        must_put(engine, b"foo4", b"bar4");
        must_put(engine, b"foo42", b"bar42"); // deleted
        must_put(engine, b"foo5", b"bar5"); // deleted
        must_put(engine, b"foo6", b"bar6");
        must_delete(engine, b"foo3");
        must_delete(engine, b"foo42");
        must_delete(engine, b"foo5");

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Forward,
            false,
        );

        let mut statistics = CfStatistics::default();

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(&Key::from_raw(b"foo30"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.near_seek(&Key::from_raw(b"foo55"), &mut statistics)
            .unwrap();
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 2);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);

        iter.prev(&mut statistics);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 3);
    }

    #[test]
    fn test_prefix_seek_skip_tombstone() {
        let engine = TestEngineBuilder::new().build().unwrap();
        engine
            .put_cf(
                &Context::default(),
                "write",
                Key::from_raw(b"aoo").append_ts(TimeStamp::zero()),
                b"ba".to_vec(),
            )
            .unwrap();
        for key in &[
            b"foo".to_vec(),
            b"foo1".to_vec(),
            b"foo2".to_vec(),
            b"foo3".to_vec(),
        ] {
            engine
                .put_cf(
                    &Context::default(),
                    "write",
                    Key::from_raw(key).append_ts(TimeStamp::zero()),
                    b"bar".to_vec(),
                )
                .unwrap();
            engine
                .delete_cf(
                    &Context::default(),
                    "write",
                    Key::from_raw(key).append_ts(TimeStamp::zero()),
                )
                .unwrap();
        }

        engine
            .put_cf(
                &Context::default(),
                "write",
                Key::from_raw(b"foo4").append_ts(TimeStamp::zero()),
                b"bar4".to_vec(),
            )
            .unwrap();

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter = CursorBuilder::new(&snapshot, CF_WRITE)
            .prefix_seek(true)
            .scan_mode(ScanMode::Forward)
            .build()
            .unwrap();

        let mut statistics = CfStatistics::default();
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"aoo").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);

        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"foo").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"foo1").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        iter.seek(
            &Key::from_raw(b"foo2").append_ts(TimeStamp::zero()),
            &mut statistics,
        )
        .unwrap();
        assert_eq!(iter.valid().unwrap(), false);
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 1);
        let perf_statistics = PerfStatisticsInstant::new();
        assert_eq!(
            iter.seek(
                &Key::from_raw(b"foo4").append_ts(TimeStamp::zero()),
                &mut statistics
            )
            .unwrap(),
            true
        );
        assert_eq!(iter.valid().unwrap(), true);
        assert_eq!(
            iter.key(&mut statistics),
            Key::from_raw(b"foo4")
                .append_ts(TimeStamp::zero())
                .as_encoded()
                .as_slice()
        );
        assert_eq!(perf_statistics.delta().0.internal_delete_skipped_count, 0);
    }
}
