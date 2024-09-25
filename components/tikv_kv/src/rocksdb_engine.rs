// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Display, Formatter},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::Poll,
    time::Duration,
};

use collections::HashMap;
pub use engine_rocks::RocksSnapshot;
use engine_rocks::{
    get_env, RocksCfOptions, RocksDbOptions, RocksEngine as BaseRocksEngine, RocksEngineIterator,
};
use engine_traits::{
    CfName, Engines, IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions,
};
use file_system::IoRateLimiter;
use futures::{
    channel::{mpsc, oneshot},
    stream, Future, Stream,
};
use kvproto::{kvrpcpb::Context, metapb, raft_cmdpb};
use raftstore::coprocessor::CoprocessorHost;
use tempfile::{Builder, TempDir};
use tikv_util::worker::{Runnable, Scheduler, Worker};
use txn_types::{Key, Value};

use super::{
    write_modifies, Callback, DummySnapshotExt, Engine, Error, ErrorInner,
    Iterator as EngineIterator, Modify, Result, SnapContext, Snapshot, WriteData,
};
use crate::{FakeExtension, OnAppliedCb, RaftExtension, WriteEvent};

// Duplicated in test_engine_builder
const TEMP_DIR: &str = "";

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(oneshot::Sender<Arc<RocksSnapshot>>),
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
            Task::Write(modifies, cb) => cb(write_modifies(&self.0.kv, modifies)),
            Task::Snapshot(sender) => {
                let _ = sender.send(Arc::new(self.0.kv.snapshot()));
            }
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
pub struct RocksEngine<RE = FakeExtension> {
    core: Arc<Mutex<RocksEngineCore>>,
    sched: Scheduler<Task>,
    engines: Engines<BaseRocksEngine, BaseRocksEngine>,
    not_leader: Arc<AtomicBool>,
    coprocessor: CoprocessorHost<BaseRocksEngine>,
    ext: RE,
}

impl<RE> RocksEngine<RE> {
    pub fn with_raft_extension<NRE>(self, ext: NRE) -> RocksEngine<NRE> {
        RocksEngine {
            core: self.core,
            sched: self.sched,
            engines: self.engines,
            not_leader: self.not_leader,
            coprocessor: self.coprocessor,
            ext,
        }
    }
}

impl RocksEngine {
    pub fn new(
        path: &str,
        db_opts: Option<RocksDbOptions>,
        cfs_opts: Vec<(CfName, RocksCfOptions)>,
        io_rate_limiter: Option<Arc<IoRateLimiter>>,
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
        let mut db_opts = db_opts.unwrap_or_default();
        if io_rate_limiter.is_some() {
            db_opts.set_env(get_env(None /* key_manager */, io_rate_limiter).unwrap());
        }

        let db = engine_rocks::util::new_engine_opt(&path, db_opts, cfs_opts)?;
        // It does not use the raft_engine, so it is ok to fill with the same
        // rocksdb.
        let engines = Engines::new(db.clone(), db);
        let sched = worker.start("engine-rocksdb", Runner(engines.clone()));
        Ok(RocksEngine {
            sched,
            core: Arc::new(Mutex::new(RocksEngineCore { temp_dir, worker })),
            not_leader: Arc::new(AtomicBool::new(false)),
            engines,
            coprocessor: CoprocessorHost::default(),
            ext: FakeExtension,
        })
    }
}

impl<RE> RocksEngine<RE> {
    pub fn trigger_not_leader(&self) {
        self.not_leader.store(true, Ordering::SeqCst);
    }

    fn not_leader_error(&self) -> Error {
        let not_leader = {
            let mut header = kvproto::errorpb::Error::default();
            header.mut_not_leader().set_region_id(100);
            header
        };
        Error::from(ErrorInner::Request(not_leader))
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

    pub fn register_observer(&mut self, f: impl FnOnce(&mut CoprocessorHost<BaseRocksEngine>)) {
        f(&mut self.coprocessor);
    }

    /// `pre_propose` is called before propose.
    /// It's used to trigger "pre_propose_query" observers for RawKV API V2 by
    /// now.
    fn pre_propose(&self, mut batch: WriteData) -> Result<WriteData> {
        let requests = batch
            .modifies
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let mut cmd_req = raft_cmdpb::RaftCmdRequest::default();
        cmd_req.set_requests(requests.into());

        let mut region = metapb::Region::default();
        region.set_id(1);
        self.coprocessor
            .pre_propose(&region, &mut cmd_req)
            .map_err(|err| Error::from(ErrorInner::Other(box_err!(err))))?;

        batch.modifies = cmd_req
            .take_requests()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        Ok(batch)
    }
}

impl<RE> Display for RocksEngine<RE> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RocksDB")
    }
}

impl<RE> Debug for RocksEngine<RE> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RocksDB [is_temp: {}]",
            self.core.lock().unwrap().temp_dir.is_some()
        )
    }
}

impl<RE: RaftExtension + 'static> Engine for RocksEngine<RE> {
    type Snap = Arc<RocksSnapshot>;
    type Local = BaseRocksEngine;

    fn kv_engine(&self) -> Option<BaseRocksEngine> {
        Some(self.engines.kv.clone())
    }

    type RaftExtension = RE;
    fn raft_extension(&self) -> Self::RaftExtension {
        self.ext.clone()
    }

    fn modify_on_kv_engine(&self, region_modifies: HashMap<u64, Vec<Modify>>) -> Result<()> {
        let modifies = region_modifies.into_values().flatten().collect();
        write_modifies(&self.engines.kv, modifies)
    }

    fn precheck_write_with_ctx(&self, _ctx: &Context) -> Result<()> {
        if self.not_leader.load(Ordering::SeqCst) {
            return Err(self.not_leader_error());
        }
        Ok(())
    }

    type WriteRes = impl Stream<Item = crate::WriteEvent> + Send + 'static;
    fn async_write(
        &self,
        _ctx: &Context,
        batch: WriteData,
        subscribed: u8,
        on_applied: Option<OnAppliedCb>,
    ) -> Self::WriteRes {
        let (mut tx, mut rx) = mpsc::channel::<WriteEvent>(WriteEvent::event_capacity(subscribed));
        let res = (move || {
            fail_point!("rockskv_async_write", |_| Err(box_err!("write failed")));

            if batch.modifies.is_empty() {
                return Err(Error::from(ErrorInner::EmptyRequest));
            }

            let batch = self.pre_propose(batch)?;

            if WriteEvent::subscribed_proposed(subscribed) {
                let _ = tx.try_send(WriteEvent::Proposed);
            }
            if WriteEvent::subscribed_committed(subscribed) {
                let _ = tx.try_send(WriteEvent::Committed);
            }
            let cb = Box::new(move |mut res| {
                if let Some(cb) = on_applied {
                    cb(&mut res);
                }
                let _ = tx.try_send(WriteEvent::Finished(res));
            });
            box_try!(self.sched.schedule(Task::Write(batch.modifies, cb)));
            Ok(())
        })();
        let mut res = Some(res);
        stream::poll_fn(move |cx| {
            if res.as_ref().map_or(false, |r| r.is_err()) {
                return Poll::Ready(res.take().map(WriteEvent::Finished));
            }
            // If it's none, it means an error is returned, it should not be polled again.
            assert!(res.is_some());
            Pin::new(&mut rx).poll_next(cx)
        })
    }

    type SnapshotRes = impl Future<Output = Result<Self::Snap>> + Send;
    fn async_snapshot(&mut self, _: SnapContext<'_>) -> Self::SnapshotRes {
        let res = (|| {
            fail_point!("rockskv_async_snapshot", |_| Err(box_err!(
                "snapshot failed"
            )));
            if self.not_leader.load(Ordering::SeqCst) {
                return Err(self.not_leader_error());
            }
            let (tx, rx) = oneshot::channel();
            if self.sched.schedule(Task::Snapshot(tx)).is_err() {
                return Err(box_err!("failed to schedule snapshot"));
            }
            Ok(rx)
        })();

        async move { Ok(res?.await.unwrap()) }
    }

    type IMSnap = Self::Snap;
    type IMSnapshotRes = Self::SnapshotRes;
    fn async_in_memory_snapshot(&mut self, ctx: SnapContext<'_>) -> Self::IMSnapshotRes {
        self.async_snapshot(ctx)
    }
}

impl Snapshot for Arc<RocksSnapshot> {
    type Iter = RocksEngineIterator;
    type Ext<'a> = DummySnapshotExt;

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

    fn iter(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        trace!("RocksSnapshot: create cf iterator");
        Ok(self.iterator_opt(cf, iter_opt)?)
    }

    fn ext(&self) -> DummySnapshotExt {
        DummySnapshotExt
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
        Iterator::seek(self, key.as_encoded()).map_err(Error::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Iterator::seek_for_prev(self, key.as_encoded()).map_err(Error::from)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        Iterator::seek_to_first(self).map_err(Error::from)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        Iterator::seek_to_last(self).map_err(Error::from)
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
