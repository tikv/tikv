// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This module contains types for asynchronously applying the write batches
//! into the storage.

use std::{
    collections::HashMap, error::Error, marker::PhantomData, rc::Rc, sync::Arc, time::Duration,
};

use futures::{Future, Stream, StreamExt};
use kvproto::kvrpcpb::Context;
use tikv_kv::{Engine, WriteData, WriteEvent};
use tikv_util::fut;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::storage;

#[cfg(test)]
lazy_static! {
    pub static ref PENDING_RAFT_REQUEST_HINT: dashmap::DashMap<u64, usize> =
        dashmap::DashMap::new();
}

const MAX_INFLIGHT_RAFT_MESSAGE: usize = 64;
const MAX_PENDING_RAFT_MESSAGE: usize = 1024;
const REGION_WRITER_MAX_IDLE_TIME: Duration = Duration::from_secs(30);
const DEFAULT_CONFIG: Config = Config {
    max_inflight_raft_message: MAX_INFLIGHT_RAFT_MESSAGE,
    max_pending_raft_message: MAX_PENDING_RAFT_MESSAGE,
    region_writer_max_idle_time: REGION_WRITER_MAX_IDLE_TIME,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Config {
    max_inflight_raft_message: usize,
    max_pending_raft_message: usize,
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

type OnComplete<T> = oneshot::Sender<T>;

enum GlobalMessage {
    Apply {
        wb: WriteData,
        ctx: Context,
        cb: OnComplete<storage::Result<()>>,
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
}

enum RegionMessage {
    Apply {
        wb: WriteData,
        ctx: Context,
        cb: OnComplete<storage::Result<()>>,
    },
}

pub struct Global<E, S> {
    incoming: mpsc::Receiver<GlobalMessage>,
    regions: HashMap<u64, RegionHandle>,

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
        async move {
            msg_tx
                .send(msg)
                .await
                .map_err(|err| -> E { box_err!("failed to send: {}", err) })?;
            rx.await
                .map_err(|err| -> E { box_err!("failed to send: {}", err) })?
        }
    }

    fn msg(&self, msg: impl FnOnce(OnComplete<()>) -> GlobalMessage) -> fut![bool] {
        let (tx, rx) = oneshot::channel();
        let msg_tx = self.tx.clone();
        let msg = msg(tx);

        async move { msg_tx.send(msg).await.is_ok() && rx.await.is_ok() }
    }

    pub fn write(&self, wb: WriteData, cx: Context) -> fut![storage::Result<()>] {
        self.try_msg(|cb| GlobalMessage::Apply { wb, ctx: cx, cb })
    }

    pub fn clear(&self) -> fut![storage::Result<()>] {
        self.try_msg(|cb| GlobalMessage::Clear { cb })
    }

    #[allow(dead_code)]
    pub fn gc_hint(&self) -> fut![bool] {
        self.msg(|cb| GlobalMessage::MaybeGc { cb })
    }

    #[allow(dead_code)]
    pub fn update_config(&self, cfg: Config) -> fut![bool] {
        self.msg(move |cb| GlobalMessage::UpdateConfig { cfg, cb })
    }
}

impl RegionHandle {
    async fn forward_write(
        &self,
        wb: WriteData,
        ctx: Context,
        cb: OnComplete<storage::Result<()>>,
    ) {
        if let Err(err) = self.tx.send(RegionMessage::Apply { wb, ctx, cb }).await {
            let RegionMessage::Apply { cb, ctx, .. } = err.0;
            // Or the receiver may gone, that was ok.
            let _ = cb.send(Err(box_err!(
                "failed to send message into region writer (ctx = {:?})",
                ctx
            )));
        }
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
        let (tx, rx) = mpsc::channel(MAX_PENDING_RAFT_MESSAGE);
        let this = Self {
            incoming: rx,
            engine: Rc::new(engine),
            regions: Default::default(),
            cfg: DEFAULT_CONFIG,
            _spawner: PhantomData,
        };

        (this, Handle { tx })
    }

    pub async fn main_loop(mut self) {
        while let Some(msg) = self.incoming.recv().await {
            match msg {
                GlobalMessage::Apply { wb, ctx, cb } => {
                    self.forward_write(wb, ctx, cb).await;
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
        hnd.forward_write(wb, ctx, cb).await;
    }
}

impl<E: Engine, S: Spawner> Region<E, S> {
    fn spawn(engine: Rc<E>, cfg: Config, region_id: u64) -> RegionHandle {
        let (tx, rx) = mpsc::channel(cfg.max_pending_raft_message);
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
        while let Ok(Some(msg)) =
            tokio::time::timeout(self.cfg.region_writer_max_idle_time, self.incoming.recv()).await
        {
            match msg {
                RegionMessage::Apply { wb, cb, ctx } => {
                    let q = Arc::clone(&self.quota)
                        .acquire_owned()
                        .await
                        .expect("semaphore closed");
                    #[cfg(test)]
                    let rid = self.region_id;
                    #[cfg(test)]
                    {
                        *PENDING_RAFT_REQUEST_HINT
                            .entry(rid)
                            .or_insert(0)
                            .value_mut() += 1;
                    }
                    let fut = self
                        .engine
                        .async_write(&ctx, wb, WriteEvent::BASIC_EVENT, None);

                    S::spawn(async move {
                        let _ = cb.send(wait_write(fut).await);
                        #[cfg(test)]
                        PENDING_RAFT_REQUEST_HINT.entry(rid).and_modify(|x| *x -= 1);
                        drop(q)
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::{iter::IntoIterator, time::Duration};

    use engine_rocks::RocksEngineIterator;
    use engine_traits::{Iterator, ALL_CFS, CF_DEFAULT, CF_WRITE};
    use futures::{future::join_all, Future};
    use kvproto::kvrpcpb::Context;
    use tempfile::TempDir;
    use tikv_kv::{Engine, Modify, RocksEngine, SnapContext, Snapshot, WriteData, WriteEvent};
    use tokio::runtime::{Builder, Runtime};
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::{Config, Global, Handle as GlobalHandle};
    use crate::{import::raft_applier::PENDING_RAFT_REQUEST_HINT, storage::TestEngineBuilder};

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
            &'this mut self,
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
        check_eq(i1.valid().unwrap(), i2.valid().unwrap(), "length", |x| x)?;
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
    // Clippy doesn't know about the romantic lazy evaluation ;)
    #[allow(clippy::needless_collect)]
    fn test_inflight_max() {
        let mut suite = create_applier(Config {
            max_inflight_raft_message: 3,
            ..Default::default()
        });

        let b1 = (1..10)
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
                        "fib this n = if n < 1 then n else this n",
                    );
                })
            })
            .collect::<Vec<_>>();
        fail::cfg("rockskv_write_modifies", "sleep(5000)").unwrap();
        let fut = suite.send_to_applier(b1.into_iter().chain(b2));
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(*PENDING_RAFT_REQUEST_HINT.get(&1).unwrap().value(), 3usize);
        assert_eq!(*PENDING_RAFT_REQUEST_HINT.get(&2).unwrap().value(), 2usize);
        suite.wait(fut);

        suite.check("inflight_max");
    }
}
