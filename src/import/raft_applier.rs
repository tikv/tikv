// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
//! This module contains types for asynchronously applying the write batches
//! into the storage.

use std::{collections::HashMap, marker::PhantomData, rc::Rc, sync::Arc, time::Duration};

use futures::{Future, Stream, StreamExt};
use kvproto::kvrpcpb::Context;
use tikv_kv::{Engine, WriteData, WriteEvent};
use tikv_util::fut;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::storage;

const MAX_INFLIGHT_RAFT_MESSAGE: usize = 64;
const MAX_PENDING_RAFT_MESSAGE: usize = 1024;
const REGION_WRITER_MAX_IDLE_TIME: Duration = Duration::from_secs(30);

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

type OnComplete = oneshot::Sender<storage::Result<()>>;

enum GlobalMessage {
    Apply {
        wb: WriteData,
        ctx: Context,
        cb: OnComplete,
    },
    Clear {
        cb: OnComplete,
    },
}

enum RegionMessage {
    Apply {
        wb: WriteData,
        ctx: Context,
        cb: OnComplete,
    },
}

pub struct Global<E, S> {
    incoming: mpsc::Receiver<GlobalMessage>,
    regions: HashMap<u64, RegionHandle>,
    engine: Rc<E>,
    _spawner: PhantomData<S>,
}

pub struct Region<E, S> {
    incoming: mpsc::Receiver<RegionMessage>,
    quota: Arc<Semaphore>,
    engine: Rc<E>,
    _spawner: PhantomData<S>,
}

#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<GlobalMessage>,
}

pub struct RegionHandle {
    tx: mpsc::Sender<RegionMessage>,
}

impl Handle {
    fn msg(&self, msg: impl FnOnce(OnComplete) -> GlobalMessage) -> fut![storage::Result<()>] {
        let (tx, rx) = oneshot::channel();

        let msg_tx = self.tx.clone();
        let msg = msg(tx);
        async move {
            msg_tx
                .send(msg)
                .await
                .map_err(|err| -> storage::Error { box_err!("failed to send: {}", err) })?;
            rx.await
                .map_err(|err| -> storage::Error { box_err!("failed to send: {}", err) })?
        }
    }

    pub fn write(&self, wb: WriteData, cx: Context) -> fut![storage::Result<()>] {
        self.msg(|cb| GlobalMessage::Apply { wb, ctx: cx, cb })
    }

    pub fn clear(&self) -> fut![storage::Result<()>] {
        self.msg(|cb| GlobalMessage::Clear { cb })
    }
}

impl RegionHandle {
    async fn forward_write(&self, wb: WriteData, ctx: Context, cb: OnComplete) {
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
            _spawner: PhantomData,
        };

        (this, Handle { tx })
    }

    pub async fn main_loop(mut self) {
        while let Some(msg) = self.incoming.recv().await {
            match msg {
                GlobalMessage::Apply { wb, ctx, cb } => {
                    let rid = ctx.get_region_id();
                    let eng = &self.engine;
                    let rgn = &mut self.regions;

                    let hnd = rgn
                        .entry(rid)
                        .and_modify(|hnd| {
                            if hnd.tx.is_closed() {
                                *hnd = Region::<E, S>::spawn(Rc::clone(eng));
                            }
                        })
                        .or_insert_with(|| Region::<E, S>::spawn(Rc::clone(eng)));
                    hnd.forward_write(wb, ctx, cb).await;
                }
                GlobalMessage::Clear { cb } => {
                    self.regions.clear();
                    let _ = cb.send(Ok(()));
                }
            }
        }
    }
}

impl<E: Engine, S: Spawner> Region<E, S> {
    fn spawn(engine: Rc<E>) -> RegionHandle {
        let (tx, rx) = mpsc::channel(MAX_PENDING_RAFT_MESSAGE);
        let this = Self {
            incoming: rx,
            quota: Arc::new(Semaphore::new(MAX_INFLIGHT_RAFT_MESSAGE)),
            engine,
            _spawner: PhantomData,
        };
        S::spawn(this.main_loop());
        RegionHandle { tx }
    }

    async fn main_loop(mut self) {
        while let Ok(Some(msg)) =
            tokio::time::timeout(REGION_WRITER_MAX_IDLE_TIME, self.incoming.recv()).await
        {
            match msg {
                RegionMessage::Apply { wb, cb, ctx } => {
                    let q = Arc::clone(&self.quota)
                        .acquire_owned()
                        .await
                        .expect("semaphore closed");
                    let fut = self
                        .engine
                        .async_write(&ctx, wb, WriteEvent::BASIC_EVENT, None);

                    S::spawn(async move {
                        let _ = cb.send(wait_write(fut).await);
                        drop(q)
                    })
                }
            }
        }
    }
}
