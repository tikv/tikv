// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This module contains types for asynchronously applying the write batches
//! into the storage.

use std::{
    collections::HashMap, error::Error, marker::PhantomData, rc::Rc, sync::Arc, time::Duration,
};

use futures::{Future, FutureExt, Stream, StreamExt};
use kvproto::kvrpcpb::Context;
use sst_importer::metrics::{
    ACTIVE_RAFT_APPLIER, APPLIER_ENGINE_REQUEST, APPLIER_EVENT, IMPORTER_APPLY_BYTES,
};
use tikv_kv::{Engine, WriteData, WriteEvent};
use tikv_util::{fut, time::Instant};
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    oneshot, Semaphore,
};

use crate::storage;

const MAX_INFLIGHT_RAFT_MESSAGE: usize = 8;
const REGION_WRITER_MAX_IDLE_TIME: Duration = Duration::from_secs(30);
const DEFAULT_CONFIG: Config = Config {
    max_inflight_raft_message: MAX_INFLIGHT_RAFT_MESSAGE,
    region_writer_max_idle_time: REGION_WRITER_MAX_IDLE_TIME,
};

const SHARED_CHANNEL_SIZE: usize = 1024;
const BUFFER_SIZE: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Config {
    max_inflight_raft_message: usize,
    region_writer_max_idle_time: Duration,
}

impl Default for Config {
    fn default() -> Self {
        DEFAULT_CONFIG
    }
}

pub struct TokioLocalSpawner;

impl Spawner for TokioLocalSpawner {
    fn spawn<F>(task: F)
    where
        F: Future<Output = ()> + 'static,
    {
        tokio::task::spawn_local(task);
    }
}

pub trait Spawner: 'static {
    fn spawn<F: Future<Output = ()> + 'static>(task: F);
}

async fn wait_write(mut s: impl Stream<Item = WriteEvent> + Send + Unpin) -> storage::Result<()> {
    match s.next().await {
        Some(WriteEvent::Finished(Ok(()))) => Ok(()),
        Some(WriteEvent::Finished(Err(e))) => Err(e.into()),
        Some(e) => Err(box_err!("unexpected event: {:?}", e)),
        None => Err(box_err!("stream closed")),
    }
}

fn extract_unsent_message<T>(x: TrySendError<T>) -> T {
    match x {
        TrySendError::Full(x) => x,
        TrySendError::Closed(x) => x,
    }
}

type OnComplete<T> = oneshot::Sender<T>;

#[derive(Debug)]
enum GlobalMessage {
    Apply {
        wb: WriteData,
        ctx: Context,
        cb: OnComplete<storage::Result<()>>,
        start: Instant,
    },
    Clear {
        cb: OnComplete<storage::Result<()>>,
    },
    UpdateConfig {
        cfg: Config,
        cb: OnComplete<()>,
    },
    MaybeGc {
        cb: OnComplete<()>,
    },
    #[cfg(test)]
    InspectWorkerStatus {
        cb: OnComplete<(
            usize, // Running worker
            usize, // Worker reference count
        )>,
    },
    #[cfg(test)]
    InspectPendingRaftCmd {
        cb: OnComplete<HashMap<u64, usize>>,
    },
}

#[derive(Debug)]
enum RegionMessage {
    Apply {
        wb: WriteData,
        ctx: Context,
        cb: OnComplete<storage::Result<()>>,
        start: Instant,
    },
    #[cfg(test)]
    InspectPendingRaftCmd { cb: mpsc::Sender<(u64, usize)> },
}

pub struct Global<E, S> {
    incoming: mpsc::Receiver<GlobalMessage>,
    regions: HashMap<u64, RegionHandle>,
    shared: Arc<Semaphore>,

    engine: Rc<E>,
    cfg: Config,
    _spawner: PhantomData<S>,
}

pub struct Region<E, S> {
    #[allow(dead_code)]
    region_id: u64,
    incoming: mpsc::Receiver<RegionMessage>,
    quota: Arc<Semaphore>,

    engine: Rc<E>,
    cfg: Config,
    _spawner: PhantomData<S>,
}

#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<GlobalMessage>,
}

#[derive(Clone)]
pub struct RegionHandle {
    tx: mpsc::Sender<RegionMessage>,
}

impl Handle {
    fn try_msg<E>(
        &self,
        msg: impl FnOnce(OnComplete<Result<(), E>>) -> GlobalMessage,
    ) -> fut![Result<(), E>]
    where
        E: From<Box<dyn Error + Send + Sync>> + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        let msg_tx = self.tx.clone();
        let msg = msg(tx);
        let msg = msg_tx.try_send(msg).err().map(|x| {
            APPLIER_EVENT
                .with_label_values(&["global-channel-full"])
                .inc();
            extract_unsent_message(x)
        });
        async move {
            if let Some(msg) = msg {
                msg_tx
                    .send(msg)
                    .await
                    .map_err(|err| -> E { box_err!("failed to send: {}", err) })?;
            }
            rx.await
                .map_err(|err| -> E { box_err!("failed to send: {}", err) })?
        }
    }

    fn msg<T>(&self, msg: impl FnOnce(OnComplete<T>) -> GlobalMessage) -> fut![Option<T>]
    where
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let msg_tx = self.tx.clone();
        let msg = msg(tx);

        let msg = msg_tx.try_send(msg).err().map(|x| {
            APPLIER_EVENT
                .with_label_values(&["global-channel-full"])
                .inc();
            extract_unsent_message(x)
        });
        async move {
            if let Some(msg) = msg {
                msg_tx.send(msg).await.ok()?;
            }
            rx.await.ok()
        }
    }

    pub fn write(&self, wb: WriteData, cx: Context) -> fut![storage::Result<()>] {
        let start = Instant::now_coarse();
        self.try_msg(move |cb| GlobalMessage::Apply {
            wb,
            ctx: cx,
            cb,
            start,
        })
    }

    pub fn clear(&self) -> fut![storage::Result<()>] {
        self.try_msg(|cb| GlobalMessage::Clear { cb })
    }

    #[allow(dead_code)]
    pub fn gc_hint(&self) -> fut![bool] {
        self.msg(|cb| GlobalMessage::MaybeGc { cb })
            .map(|x| x.is_some())
    }

    #[allow(dead_code)]
    pub fn update_config(&self, cfg: Config) -> fut![bool] {
        self.msg(move |cb| GlobalMessage::UpdateConfig { cfg, cb })
            .map(|x| x.is_some())
    }

    #[cfg(test)]
    pub fn inspect_worker(&self) -> fut![(usize, usize)] {
        self.msg(|cb| GlobalMessage::InspectWorkerStatus { cb })
            .map(|x| x.unwrap())
    }

    #[cfg(test)]
    pub fn inspect_inflight(&self) -> fut![HashMap<u64, usize>] {
        self.msg(|cb| GlobalMessage::InspectPendingRaftCmd { cb })
            .map(|x| x.unwrap())
    }
}

impl RegionHandle {
    fn forward_write(
        &self,
        wb: WriteData,
        ctx: Context,
        cb: OnComplete<storage::Result<()>>,
        start: Instant,
    ) -> Option<impl Future<Output = ()>> {
        let remained = self
            .tx
            .try_send(RegionMessage::Apply { wb, ctx, cb, start })
            .err()
            .map(extract_unsent_message);

        let forward_to = move |tx: mpsc::Sender<RegionMessage>, message| async move {
            if let Err(err) = tx.send(message).await {
                let (cb, ctx) = match err.0 {
                    RegionMessage::Apply { cb, ctx, .. } => (cb, ctx),
                    #[cfg(test)]
                    _ => unreachable!(),
                };

                // Or the receiver may gone, that was ok.
                let _ = cb.send(Err(box_err!(
                    "failed to send message into region writer (ctx = {:?})",
                    ctx
                )));
            }
        };

        remained.map(move |x| {
            let msg_tx = self.tx.clone();
            APPLIER_EVENT
                .with_label_values(&["region-channel-full"])
                .inc();
            forward_to(msg_tx, x)
        })
    }
}

impl<E: Engine> Global<E, TokioLocalSpawner> {
    pub fn spawn_tokio(engine: E, rt: &tokio::runtime::Handle) -> Handle {
        let (tx, rx) = tokio::sync::oneshot::channel();
        rt.spawn_blocking(move || {
            let (app, handle) = Global::<E, TokioLocalSpawner>::create(engine);
            // Once it fail, the outer rx would know.
            let _ = tx.send(handle);
            let local = tokio::task::LocalSet::new();
            tokio::runtime::Handle::current().block_on(local.run_until(app.main_loop()))
        });
        rx.blocking_recv()
            .expect("failed to initialize the raft writer")
    }
}

impl<E: Engine, S: Spawner> Global<E, S> {
    pub fn create(engine: E) -> (Self, Handle) {
        let (tx, rx) = mpsc::channel(BUFFER_SIZE);
        let this = Self {
            incoming: rx,
            engine: Rc::new(engine),
            regions: Default::default(),
            shared: Arc::new(Semaphore::new(SHARED_CHANNEL_SIZE)),
            cfg: DEFAULT_CONFIG,
            _spawner: PhantomData,
        };

        (this, Handle { tx })
    }

    pub async fn main_loop(mut self) {
        while let Some(msg) = self.incoming.recv().await {
            match msg {
                GlobalMessage::Apply { wb, ctx, cb, start } => {
                    self.forward_write(wb, ctx, cb, start).await;
                }
                GlobalMessage::Clear { cb } => {
                    self.regions.clear();
                    let _ = cb.send(Ok(()));
                }
                GlobalMessage::UpdateConfig { cb, cfg } => {
                    self.regions.clear();
                    self.cfg = cfg;
                    let _ = cb.send(());
                }
                GlobalMessage::MaybeGc { cb } => {
                    self.gc();
                    let _ = cb.send(());
                }
                #[cfg(test)]
                GlobalMessage::InspectWorkerStatus { cb } => {
                    let _ = cb.send((
                        self.regions.values().filter(|x| !x.tx.is_closed()).count(),
                        self.regions.len(),
                    ));
                }
                #[cfg(test)]
                GlobalMessage::InspectPendingRaftCmd { cb } => {
                    let (tx, mut rx) = mpsc::channel(self.regions.len());
                    for v in self.regions.values() {
                        let msg = RegionMessage::InspectPendingRaftCmd { cb: tx.clone() };
                        // Don't wait: we may get stuck when there are some failpoints.
                        v.tx.try_send(msg).unwrap();
                    }
                    S::spawn(async move {
                        let mut h = HashMap::new();
                        while let Some((rid, cnt)) = rx.recv().await {
                            h.insert(rid, cnt);
                        }
                        cb.send(h).unwrap();
                    })
                }
            }
        }
    }

    fn gc(&mut self) {
        self.regions.retain(|_, hnd| !hnd.tx.is_closed());
    }

    async fn forward_write(
        &mut self,
        wb: WriteData,
        ctx: Context,
        cb: OnComplete<storage::Result<()>>,
        start: Instant,
    ) {
        let rid = ctx.get_region_id();
        let eng = &self.engine;
        let cfg = &self.cfg;
        let rgn = &mut self.regions;

        let hnd = rgn
            .entry(rid)
            .and_modify(|hnd| {
                if hnd.tx.is_closed() {
                    *hnd = Region::<E, S>::spawn(Rc::clone(eng), *cfg, rid);
                }
            })
            .or_insert_with(|| Region::<E, S>::spawn(Rc::clone(eng), *cfg, rid));
        if let Some(tsk) = hnd.forward_write(wb, ctx, cb, start) {
            let permit = match Semaphore::try_acquire_owned(Arc::clone(&self.shared)) {
                Ok(permit) => Ok(permit),
                Err(_) => {
                    APPLIER_EVENT
                        .with_label_values(&["shared-channel-full"])
                        .inc();
                    Semaphore::acquire_owned(Arc::clone(&self.shared)).await
                }
            };
            // Once it returns `err`, the Semaphore should be already closed (perhaps the
            // server is shutting down), do nothing.
            if let Ok(permit) = permit {
                S::spawn(async move {
                    tsk.await;
                    drop(permit);
                })
            }
        }
    }
}

impl<E: Engine, S: Spawner> Region<E, S> {
    fn spawn(engine: Rc<E>, cfg: Config, region_id: u64) -> RegionHandle {
        // NOTE: When the queuing request is 2x the max inflight message count, it may
        // stall all requests (for every region). Should we make the channel buffer
        // longer?
        let (tx, rx) = mpsc::channel(cfg.max_inflight_raft_message);
        let quota = Arc::new(Semaphore::new(cfg.max_inflight_raft_message));
        let handle = RegionHandle { tx };
        let this = Self {
            region_id,
            incoming: rx,
            quota,
            engine,
            cfg,
            _spawner: PhantomData,
        };
        S::spawn(this.main_loop());
        handle
    }

    async fn main_loop(mut self) {
        ACTIVE_RAFT_APPLIER.inc();
        defer! {{ ACTIVE_RAFT_APPLIER.dec(); }}
        while let Ok(Some(msg)) =
            tokio::time::timeout(self.cfg.region_writer_max_idle_time, self.incoming.recv()).await
        {
            match msg {
                RegionMessage::Apply { wb, cb, ctx, start } => {
                    APPLIER_ENGINE_REQUEST
                        .with_label_values(&["queuing"])
                        .observe(start.saturating_elapsed_secs());
                    let start = Instant::now();

                    let q = Arc::clone(&self.quota)
                        .acquire_owned()
                        .await
                        .expect("semaphore closed");
                    let write_size = wb.size();
                    let fut = self
                        .engine
                        .async_write(&ctx, wb, WriteEvent::BASIC_EVENT, None);

                    IMPORTER_APPLY_BYTES.observe(write_size as _);
                    APPLIER_ENGINE_REQUEST
                        .with_label_values(&["get_permit"])
                        .observe(start.saturating_elapsed_secs());
                    let start = Instant::now();

                    S::spawn(async move {
                        let _ = cb.send(wait_write(fut).await);
                        drop(q);

                        APPLIER_ENGINE_REQUEST
                            .with_label_values(&["apply"])
                            .observe(start.saturating_elapsed_secs());
                    });
                }
                #[cfg(test)]
                RegionMessage::InspectPendingRaftCmd { cb } => {
                    cb.send((
                        self.region_id,
                        self.cfg.max_inflight_raft_message
                            - self.quota.as_ref().available_permits(),
                    ))
                    .await
                    .unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{convert::identity, iter::IntoIterator, time::Duration};

    use engine_rocks::RocksEngineIterator;
    use engine_traits::{Iterator, ALL_CFS, CF_DEFAULT, CF_WRITE};
    use futures::{future::join_all, Future};
    use kvproto::kvrpcpb::Context;
    use tempfile::TempDir;
    use tikv_kv::{Engine, Modify, RocksEngine, SnapContext, Snapshot, WriteData, WriteEvent};
    use tokio::runtime::{Builder, Runtime};
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::{Config, Global, Handle as GlobalHandle};
    use crate::storage::TestEngineBuilder;

    struct Suite {
        handle: GlobalHandle,
        rt: Runtime,
        eng: RocksEngine,

        tso: u64,
        mirror: RocksEngine,

        _temp_dirs: [TempDir; 2],
    }

    impl Suite {
        fn wait<T>(&self, fut: impl Future<Output = T>) -> T {
            self.rt.block_on(fut)
        }

        fn batch<'a, 'b, 'this: 'a + 'b>(
            &mut self,
            region_id: u64,
            f: impl FnOnce(&mut dyn FnMut(&'a str, &'b str)) + 'this,
        ) -> (WriteData, Context) {
            let mut ctx = Context::default();
            ctx.set_region_id(region_id);
            let mut b = vec![];
            let mut t = |key, value| txn(key, value, &mut self.tso, &mut b);
            f(&mut t);
            let batch = WriteData::new(b.clone(), Default::default());
            let batch2 = WriteData::new(b, Default::default());
            self.wait(write_to_engine(&ctx, &self.mirror, batch));
            (batch2, ctx)
        }

        fn send_to_applier(
            &self,
            args: impl std::iter::Iterator<Item = (WriteData, Context)>,
        ) -> fut![()] {
            let fut = args
                .map(|arg| self.rt.spawn(self.handle.write(arg.0, arg.1)))
                .collect::<Vec<_>>();
            async {
                join_all(
                    fut.into_iter()
                        .map(|fut| async move { fut.await.unwrap().unwrap() }),
                )
                .await;
            }
        }

        fn check(&mut self, name: &str) {
            for cf in ALL_CFS {
                let the_mirror = iterate_over(&mut self.mirror, cf);
                let real_world = iterate_over(&mut self.eng, cf);
                compare_iter(the_mirror, real_world)
                    .map_err(|err| format!("case {name}: {err}"))
                    .unwrap();
            }
        }
    }

    fn create_applier(cfg: Config) -> Suite {
        let temp_dirs = [TempDir::new().unwrap(), TempDir::new().unwrap()];
        let engine = TestEngineBuilder::new()
            .path(temp_dirs[0].path())
            .build()
            .unwrap();
        let eng = engine.clone();
        let mirror = TestEngineBuilder::new()
            .path(temp_dirs[1].path())
            .build()
            .unwrap();
        let rt = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .unwrap();
        let handle = Global::spawn_tokio(engine, rt.handle());
        assert!(rt.block_on(handle.update_config(cfg)));
        Suite {
            handle,
            rt,
            eng,
            tso: 1u64,
            mirror,
            _temp_dirs: temp_dirs,
        }
    }

    async fn write_to_engine(ctx: &Context, e: &RocksEngine, batch: WriteData) {
        use futures_util::StreamExt;
        e.async_write(ctx, batch, WriteEvent::BASIC_EVENT, None)
            .next()
            .await
            .unwrap();
    }

    fn iterate_over(e: &mut RocksEngine, cf: &'static str) -> RocksEngineIterator {
        let snap = e.snapshot(SnapContext::default()).unwrap();
        let mut iter = snap.iter(cf, Default::default()).unwrap();
        iter.seek_to_first().unwrap();
        iter
    }

    fn check_eq<T: Eq, D: std::fmt::Display>(
        a: T,
        b: T,
        tag: &str,
        show: impl Fn(T) -> D,
    ) -> Result<(), String> {
        if a != b {
            return Err(format!("{} not match: {} vs {}", tag, show(a), show(b)));
        }
        Ok(())
    }

    fn compare_iter(mut i1: impl Iterator, mut i2: impl Iterator) -> Result<(), String> {
        while i1.valid().unwrap() && i2.valid().unwrap() {
            check_eq(i1.key(), i2.key(), "key", <[u8]>::escape_ascii)?;
            check_eq(i1.value(), i2.value(), "value", <[u8]>::escape_ascii)?;
            i1.next().unwrap();
            i2.next().unwrap();
        }
        check_eq(i1.valid().unwrap(), i2.valid().unwrap(), "length", identity)?;
        Ok(())
    }

    fn write(key: &[u8], ty: WriteType, commit_ts: u64, start_ts: u64) -> (Vec<u8>, Vec<u8>) {
        let k = Key::from_raw(key).append_ts(TimeStamp::new(commit_ts));
        let v = Write::new(ty, TimeStamp::new(start_ts), None);
        (k.into_encoded(), v.as_ref().to_bytes())
    }

    fn default(key: &[u8], val: &[u8], start_ts: u64) -> (Vec<u8>, Vec<u8>) {
        let k = Key::from_raw(key).append_ts(TimeStamp::new(start_ts));
        (k.into_encoded(), val.to_owned())
    }

    fn default_req(key: &[u8], val: &[u8], start_ts: u64) -> Modify {
        let (k, v) = default(key, val, start_ts);
        Modify::Put(CF_DEFAULT, Key::from_encoded(k), v)
    }

    fn write_req(key: &[u8], ty: WriteType, commit_ts: u64, start_ts: u64) -> Modify {
        let (k, v) = write(key, ty, commit_ts, start_ts);
        if ty == WriteType::Delete {
            Modify::Delete(CF_WRITE, Key::from_encoded(k))
        } else {
            Modify::Put(CF_WRITE, Key::from_encoded(k), v)
        }
    }

    fn txn(key: &str, value: &str, tso: &mut u64, append_to: &mut Vec<Modify>) {
        let start = *tso;
        let commit = *tso + 1;
        *tso += 2;
        append_to.extend([
            default_req(key.as_bytes(), value.as_bytes(), start),
            write_req(key.as_bytes(), WriteType::Put, start, commit),
        ])
    }

    #[test]
    fn test_basic() {
        let mut suite = create_applier(Config::default());
        let b1 = suite.batch(1, |t| {
            t("1", "amazing world in my dream");
            t("2", "gazing at the abyss");
        });
        let b2 = suite.batch(2, |t| {
            t("3", "the forest leaves drop");
            t("4", "the meaningless words in a test case");
        });
        let fut = suite.send_to_applier(vec![b1, b2].into_iter());
        suite.wait(fut);

        suite.check("basic");
    }

    #[test]
    // Clippy doesn't know about the romantic relationship between lazy evaluation and
    // side-effective ;)
    #[allow(clippy::needless_collect)]
    fn test_inflight_max() {
        let mut suite = create_applier(Config {
            max_inflight_raft_message: 3,
            ..Default::default()
        });

        let b1 = (1..6)
            .map(|_| {
                suite.batch(1, move |t| {
                    t("al-kīmiyā", "following the light of the moon and stars, the guide of the sun and winds.");
                })
            })
            .collect::<Vec<_>>();
        let b2 = (1..3)
            .map(|_| {
                suite.batch(2, move |t| {
                    t(
                        "sole key to this mystery",
                        "fib this n = if n < 2 then n else this (n-1) + this (n-2)",
                    );
                })
            })
            .collect::<Vec<_>>();
        fail::cfg("rockskv_write_modifies", "sleep(5000)").unwrap();
        let fut = suite.send_to_applier(b1.into_iter().chain(b2));
        let pending_requests = suite.wait(suite.handle.inspect_inflight());
        assert_eq!(*pending_requests.get(&1).unwrap(), 3usize);
        assert_eq!(*pending_requests.get(&2).unwrap(), 2usize);
        fail::cfg("rockskv_write_modifies", "off").unwrap();
        suite.wait(fut);

        suite.check("inflight_max");
    }

    #[test]
    fn test_gc() {
        let mut suite = create_applier(Config {
            region_writer_max_idle_time: Duration::from_millis(100),
            ..Default::default()
        });
        let b1 = suite.batch(1, |t| {
            t("where is the sun", "it is in the clear sky");
            t("where are the words", "they are in some language model");
            t(
                "where is the language model",
                "I dunno, these sentences are generated by a human.",
            );
        });
        let b2 = suite.batch(2, |t| {
            t("...and this case needs two batches", "why?");
            t(
                "It is by... tradition.",
                "If a case is TOO short, who will believe it is effective?",
            );
            t(
                "Perhaps we should make the `RocksEngine` be able to distinguish requests.",
                "So...",
            );
            t(
                "We can block `b2` but not for `b1`",
                "then we can check there should be only one running worker.",
            );
        });
        assert_eq!(suite.wait(suite.handle.inspect_worker()), (0, 0));
        fail::cfg("rockskv_async_write", "sleep(5000)").unwrap();
        let fut = suite.send_to_applier(std::iter::once(b1));
        assert_eq!(suite.wait(suite.handle.inspect_worker()), (1, 1));
        let fut2 = suite.send_to_applier(std::iter::once(b2));
        assert_eq!(suite.wait(suite.handle.inspect_worker()), (2, 2));

        fail::cfg("rockskv_async_write", "off").unwrap();
        suite.wait(async move {
            fut.await;
            fut2.await;
        });
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(suite.wait(suite.handle.inspect_worker()), (0, 2));

        suite.wait(suite.handle.gc_hint());
        assert_eq!(suite.wait(suite.handle.inspect_worker()), (0, 0));
    }
}
