// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server;
use crate::server::metrics::*;
use crate::server::snap::Task as SnapTask;
use crate::server::{Config, StoreAddrResolver};
use crossbeam::queue::{ArrayQueue, PushError};
use futures::sync::oneshot;
use futures::task::{self, Task};
use futures::try_ready;
use futures::{Async, AsyncSink, Future, Poll, Sink};
use grpcio::{ChannelBuilder, ClientCStreamReceiver, ClientCStreamSender, Environment, WriteFlags};
use kvproto::raft_serverpb::{Done, RaftMessage};
use kvproto::tikvpb::TikvClient;
use raft::SnapshotStatus;
use raftstore::errors::DiscardReason;
use raftstore::router::RaftStoreRouter;
use std::collections::VecDeque;
use std::ffi::CString;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tikv_util::collections::HashMap;
use tikv_util::security::SecurityManager;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::Scheduler;
use tikv_util::Either;
use tokio_threadpool::ThreadPool;
use tokio_timer::Delay;

const MAX_GRPC_RECV_MSG_LEN: i32 = 10 * 1024 * 1024;
const MAX_GRPC_SEND_MSG_LEN: i32 = 10 * 1024 * 1024;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

struct SnapshotReporter<T: RaftStoreRouter + 'static> {
    raft_router: T,
    region_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl<T: RaftStoreRouter + 'static> SnapshotReporter<T> {
    pub fn report(&self, status: SnapshotStatus) {
        debug!(
            "send snapshot";
            "to_peer_id" => self.to_peer_id,
            "region_id" => self.region_id,
            "status" => ?status
        );

        if status == SnapshotStatus::Failure {
            let store = self.to_store_id.to_string();
            REPORT_FAILURE_MSG_COUNTER
                .with_label_values(&["snapshot", &*store])
                .inc();
        };

        if let Err(e) =
            self.raft_router
                .report_snapshot_status(self.region_id, self.to_peer_id, status)
        {
            error!(
                "report snapshot to peer failes";
                "to_peer_id" => self.to_peer_id,
                "to_store_id" => self.to_store_id,
                "region_id" => self.region_id,
                "err" => ?e
            );
        }
    }
}

struct StreamCore {
    buf: ArrayQueue<RaftMessage>,
    task: Mutex<Option<Task>>,
}

struct RaftCall<R> {
    sender: ClientCStreamSender<RaftMessage>,
    receiver: ClientCStreamReceiver<Done>,
    core: Arc<StreamCore>,
    last_messages: VecDeque<RaftMessage>,
    router: R,
    snap_scheduler: Scheduler<SnapTask>,
    lifetime: Option<oneshot::Sender<()>>,
    store_id: u64,
    addr: String,
}

impl<R: RaftStoreRouter + 'static> RaftCall<R> {
    fn new_snapshot_reporter(&self, msg: &RaftMessage) -> SnapshotReporter<R> {
        let region_id = msg.get_region_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let to_store_id = msg.get_to_peer().get_store_id();

        SnapshotReporter {
            raft_router: self.router.clone(),
            region_id,
            to_peer_id,
            to_store_id,
        }
    }

    fn send_snapshot_sock(&self, msg: RaftMessage) {
        let rep = self.new_snapshot_reporter(&msg);
        let cb = Box::new(move |res: Result<_, _>| {
            if res.is_err() {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        });
        if let Err(e) = self.snap_scheduler.schedule(SnapTask::Send {
            addr: self.addr.clone(),
            msg,
            cb,
        }) {
            if let SnapTask::Send { cb, .. } = e.into_inner() {
                error!(
                    "channel is unavailable, failed to schedule snapshot";
                    "to_addr" => &self.addr
                );
                cb(Err(box_err!("failed to schedule snapshot")));
            }
        }
    }

    fn pop_msg(&mut self) -> Option<RaftMessage> {
        match self.core.buf.pop() {
            Ok(msg) => Some(msg),
            Err(_) => {
                {
                    let mut task = self.core.task.lock().unwrap();
                    *task = Some(task::current());
                }
                match self.core.buf.pop() {
                    Ok(msg) => Some(msg),
                    Err(_) => None,
                }
            }
        }
    }

    fn fill_msg(&mut self) {
        if self.last_messages.len() == 2 {
            return;
        }

        loop {
            let msg = match self.pop_msg() {
                Some(msg) => msg,
                None => return,
            };
            if msg.get_message().has_snapshot() {
                self.send_snapshot_sock(msg);
                continue;
            } else {
                self.last_messages.push_back(msg);
                if self.last_messages.len() == 2 {
                    return;
                }
            }
        }
    }

    fn clean_up(&mut self) {
        while let Some(msg) = self.last_messages.pop_front() {
            let _ = self
                .router
                .report_unreachable(msg.get_region_id(), msg.get_to_peer().get_id());
        }
        self.lifetime.take();
    }
}

impl<R: RaftStoreRouter + 'static> Future for RaftCall<R> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            self.fill_msg();
            let msg = match self.last_messages.pop_front() {
                Some(msg) => msg,
                None => {
                    if let Err(e) = self.sender.poll_complete() {
                        error!("connection fail"; "store_id" => self.store_id, "error" => ?e, "addr" => %self.addr);
                        self.clean_up();
                        return Err(());
                    }
                    match self.receiver.poll() {
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Ok(Async::Ready(_)) => {
                            info!("connection close"; "store_id" => self.store_id, "addr" => %self.addr);
                            return Ok(Async::Ready(()));
                        }
                        Err(e) => {
                            error!("connection abort"; "store_id" => self.store_id, "error" => ?e, "addr" => %self.addr);
                            self.clean_up();
                            return Err(());
                        }
                    }
                }
            };

            match self.sender.start_send((
                msg,
                WriteFlags::default().buffer_hint(!self.last_messages.is_empty()),
            )) {
                Ok(AsyncSink::NotReady((msg, _))) => {
                    self.last_messages.push_front(msg);
                    return Ok(Async::NotReady);
                }
                Ok(AsyncSink::Ready) => (),
                Err(e) => {
                    error!("failed to send message"; "store_id" => self.store_id, "error" => ?e, "addr" => %self.addr);
                    self.clean_up();
                    return Err(());
                }
            }
        }
    }
}

struct StreamBackEnd<S, R> {
    store_id: u64,
    core: Arc<StreamCore>,
    env: Arc<Environment>,
    cfg: Arc<Config>,
    security_mgr: Arc<SecurityManager>,
    resolver: S,
    router: R,
    snap_scheduler: Scheduler<SnapTask>,
    backoff: Option<Delay>,
    addr: Option<Either<String, oneshot::Receiver<server::Result<String>>>>,
    client: Option<(TikvClient, oneshot::Receiver<()>)>,
}

impl<S: StoreAddrResolver, R: RaftStoreRouter + 'static> StreamBackEnd<S, R> {
    fn resolve(&mut self) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let store_id = self.store_id;
        let res = self.resolver.resolve(
            store_id,
            Box::new(move |addr| {
                let _ = tx.send(addr);
            }),
        );
        if let Err(e) = res {
            return Err(format!("failed to resolve store {:?}", e));
        }
        self.addr = Some(Either::Right(rx));
        Ok(())
    }

    fn clear_pending_message(&mut self) {
        let len = self.core.buf.len();
        for _ in 0..len {
            let msg = self.core.buf.pop().unwrap();
            let region_id = msg.get_region_id();
            let peer_id = msg.get_to_peer().get_id();
            if msg.get_message().has_snapshot() {
                let res =
                    self.router
                        .report_snapshot_status(region_id, peer_id, SnapshotStatus::Failure);
                if let Err(e) = res {
                    error!(
                        "report snapshot to peer failes";
                        "to_peer_id" => peer_id,
                        "to_store_id" => self.store_id,
                        "region_id" => region_id,
                        "err" => ?e
                    );
                }
            } else {
                let _ = self.router.report_unreachable(region_id, peer_id);
            }
        }
    }

    fn poll_addr(&mut self) -> Poll<(), String> {
        if self.addr.is_none() {
            if let Err(e) = self.resolve() {
                RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
                self.clear_pending_message();
                return Err(e);
            }
        }
        let addr = match self.addr {
            Some(Either::Left(_)) => return Ok(Async::Ready(())),
            Some(Either::Right(ref mut rx)) => match rx.poll() {
                Ok(Async::Ready(s)) => s,
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => Err(server::errors::Error::Other(format!("{:?}", e).into())),
            },
            None => unreachable!(),
        };

        if let Err(e) = addr {
            RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
            self.clear_pending_message();
            return Err(format!("resolve failed: {:?}", e));
        }

        RESOLVE_STORE_COUNTER.with_label_values(&["success"]).inc();
        let addr = addr.unwrap();
        info!("resolve store address ok"; "store_id" => self.store_id, "addr" => %addr);

        self.addr = Some(Either::Left(addr));
        Ok(Async::Ready(()))
    }

    fn connect(&mut self) -> Poll<(), String> {
        try_ready!(self.poll_addr());
        let addr = self.addr.as_ref().unwrap().as_ref().left().unwrap();
        info!("server: new connection with tikv endpoint"; "addr" => addr, "store_id" => self.store_id);

        let cb = ChannelBuilder::new(self.env.clone())
            .stream_initial_window_size(self.cfg.grpc_stream_initial_window_size.0 as i32)
            .max_receive_message_len(MAX_GRPC_RECV_MSG_LEN)
            .max_send_message_len(MAX_GRPC_SEND_MSG_LEN)
            .keepalive_time(self.cfg.grpc_keepalive_time.0)
            .keepalive_timeout(self.cfg.grpc_keepalive_timeout.0)
            .default_compression_algorithm(self.cfg.grpc_compression_algorithm())
            // hack: so it's different args, grpc will always create a new connection.
            .raw_cfg_int(
                CString::new("random id").unwrap(),
                CONN_ID.fetch_add(1, Ordering::SeqCst),
            );
        let channel = self.security_mgr.connect(cb, addr);
        let client = TikvClient::new(channel);
        let (batch_sink, batch_receiver) = client.raft().unwrap();
        let (tx, rx) = oneshot::channel();
        let call = RaftCall {
            sender: batch_sink,
            receiver: batch_receiver,
            core: self.core.clone(),
            last_messages: VecDeque::with_capacity(2),
            router: self.router.clone(),
            snap_scheduler: self.snap_scheduler.clone(),
            lifetime: Some(tx),
            store_id: self.store_id,
            addr: addr.clone(),
        };
        client.spawn(call);
        self.client = Some((client, rx));
        Ok(Async::Ready(()))
    }

    fn poll_call(&mut self) -> Poll<(), String> {
        if self.client.is_none() {
            try_ready!(self.connect());
        }
        self.client
            .as_mut()
            .unwrap()
            .1
            .poll()
            .map_err(|e| format!("{:?}", e))
    }

    fn poll_once(&mut self) -> bool {
        match self.poll_call() {
            Ok(Async::NotReady) => return true,
            Err(e) => {
                let addr = self.addr.take();
                let a = addr.and_then(|s| s.left()).unwrap_or_else(|| "".to_owned());
                error!("connection abort"; "store_id" => self.store_id, "addr" => a, "error" => e);
            }
            Ok(Async::Ready(_)) => unreachable!(),
        }
        self.client = None;
        // May hurt raftstore performance.
        self.router.broadcast_unreachable(self.store_id);
        false
    }
}

impl<S: StoreAddrResolver, R: RaftStoreRouter + 'static> Future for StreamBackEnd<S, R> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            if let Some(delay) = self.backoff.as_mut() {
                match delay.poll() {
                    Ok(Async::Ready(_)) => (),
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                    Err(e) => panic!("backoff failed: {:?}", e),
                }
            }
            self.backoff = None;
            for _ in 0..2 {
                if self.poll_once() {
                    return Ok(Async::NotReady);
                }
            }
            self.backoff = Some(GLOBAL_TIMER_HANDLE.delay(Instant::now() + Duration::from_secs(1)));
        }
    }
}

struct RaftStream {
    core: Arc<StreamCore>,
}

impl RaftStream {
    fn new<S, R>(pool: &RaftStreamPool<S, R>, store_id: u64) -> RaftStream
    where
        S: StoreAddrResolver + 'static,
        R: RaftStoreRouter + 'static,
    {
        let core = Arc::new(StreamCore {
            buf: ArrayQueue::new(4096),
            task: Mutex::new(None),
        });
        let backend = StreamBackEnd {
            store_id,
            core: core.clone(),
            env: pool.env.clone(),
            cfg: pool.cfg.clone(),
            security_mgr: pool.security_mgr.clone(),
            resolver: pool.resolver.clone(),
            router: pool.router.clone(),
            snap_scheduler: pool.snap_scheduler.clone(),
            backoff: None,
            addr: None,
            client: None,
        };
        pool.thread_pool.spawn(backend);
        RaftStream { core }
    }

    fn send(&self, msg: RaftMessage) -> bool {
        // Another way is pop the old messages, but it makes things
        // complicated.
        match self.core.buf.push(msg) {
            Ok(()) => true,
            Err(PushError(_)) => false,
        }
    }

    fn notify(&self) -> bool {
        let task = self.core.task.lock().unwrap().take();
        if let Some(t) = task {
            t.notify();
            true
        } else {
            false
        }
    }

    fn flush(&self) {
        if !self.core.buf.is_empty() {
            self.notify();
        }
    }
}

impl Clone for RaftStream {
    fn clone(&self) -> RaftStream {
        RaftStream {
            core: self.core.clone(),
        }
    }
}

struct CachedRaftStream {
    stream: RaftStream,
    dirty: bool,
}

pub struct RaftStreamPool<S, R> {
    streams: Arc<Mutex<HashMap<u64, RaftStream>>>,
    cache: HashMap<u64, CachedRaftStream>,
    need_flush: Vec<u64>,
    thread_pool: Arc<ThreadPool>,
    env: Arc<Environment>,
    cfg: Arc<Config>,
    security_mgr: Arc<SecurityManager>,
    resolver: S,
    router: R,
    snap_scheduler: Scheduler<SnapTask>,
}

impl<S, R> RaftStreamPool<S, R>
where
    S: StoreAddrResolver + 'static,
    R: RaftStoreRouter + 'static,
{
    pub fn new(
        env: Arc<Environment>,
        cfg: Arc<Config>,
        security_mgr: Arc<SecurityManager>,
        resolver: S,
        router: R,
        snap_scheduler: Scheduler<SnapTask>,
    ) -> RaftStreamPool<S, R> {
        let pool = tokio_threadpool::Builder::new()
            .name_prefix(thd_name!("raft-stream"))
            .pool_size(1)
            .build();
        RaftStreamPool {
            streams: Arc::new(Mutex::new(HashMap::default())),
            cache: HashMap::default(),
            need_flush: vec![],
            thread_pool: Arc::new(pool),
            env,
            cfg,
            security_mgr,
            resolver,
            router,
            snap_scheduler,
        }
    }

    fn load_stream(&mut self, store_id: u64) {
        let s = {
            let mut streams = self.streams.lock().unwrap();
            streams
                .entry(store_id)
                .or_insert_with(|| RaftStream::new(self, store_id))
                .clone()
        };
        self.cache.insert(
            store_id,
            CachedRaftStream {
                stream: s,
                dirty: false,
            },
        );
    }

    pub fn send(&mut self, msg: RaftMessage) -> raftstore::Result<()> {
        let store_id = msg.get_to_peer().get_store_id();
        loop {
            if let Some(s) = self.cache.get_mut(&store_id) {
                if !s.stream.send(msg) {
                    s.stream.flush();
                    s.dirty = false;
                    return Err(raftstore::errors::Error::Transport(DiscardReason::Full));
                }
                if !s.dirty {
                    s.dirty = true;
                    self.need_flush.push(store_id);
                }
                return Ok(());
            } else {
                self.load_stream(store_id);
            }
        }
    }

    pub fn flush(&mut self) {
        if self.need_flush.is_empty() {
            return;
        }
        if self.need_flush.len() < self.cache.len() / 2 {
            for store_id in &self.need_flush {
                let mut s = self.cache.get_mut(&store_id).unwrap();
                if s.dirty {
                    s.dirty = false;
                    s.stream.flush();
                }
            }
        } else {
            for s in self.cache.values_mut() {
                if s.dirty {
                    s.stream.flush();
                    s.dirty = false;
                }
            }
        }
        self.need_flush.clear();
    }
}

impl<S, R> Clone for RaftStreamPool<S, R>
where
    S: Clone,
    R: Clone,
{
    fn clone(&self) -> RaftStreamPool<S, R> {
        RaftStreamPool {
            streams: self.streams.clone(),
            cache: HashMap::default(),
            need_flush: vec![],
            thread_pool: self.thread_pool.clone(),
            env: self.env.clone(),
            cfg: self.cfg.clone(),
            security_mgr: self.security_mgr.clone(),
            resolver: self.resolver.clone(),
            router: self.router.clone(),
            snap_scheduler: self.snap_scheduler.clone(),
        }
    }
}
