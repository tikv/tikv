// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server::metrics::*;
use crate::server::snap::Task as SnapTask;
use crate::server::{self, Config, StoreAddrResolver};
use collections::{HashMap, HashSet};
use crossbeam::queue::{ArrayQueue, PushError};
use engine_rocks::RocksEngine;
use futures::channel::oneshot;
use futures::compat::Future01CompatExt;
use futures::task::{Context, Poll, Waker};
use futures::{Future, Sink};
use grpcio::{
    ChannelBuilder, ClientCStreamReceiver, ClientCStreamSender, Environment, RpcStatus,
    RpcStatusCode, WriteFlags,
};
use kvproto::raft_serverpb::{Done, RaftMessage};
use kvproto::tikvpb::{BatchRaftMessage, TikvClient};
use raft::SnapshotStatus;
use raftstore::errors::DiscardReason;
use raftstore::router::RaftStoreRouter;
use security::SecurityManager;
use std::collections::VecDeque;
use std::ffi::CString;
use std::marker::Unpin;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{cmp, mem, result};
use tikv_util::lru::LruCache;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::worker::Scheduler;
use yatp::task::future::TaskCell;
use yatp::ThreadPool;

// When merge raft messages into a batch message, leave a buffer.
const GRPC_SEND_MSG_BUF: usize = 64 * 1024;
const QUEUE_CAPACITY: usize = 4096;

const RAFT_MSG_MAX_BATCH_SIZE: usize = 128;

static CONN_ID: AtomicI32 = AtomicI32::new(0);

const _ON_RESOLVE_FP: &str = "transport_snapshot_on_resolve";

/// A quick queue for sending raft messages.
struct Queue {
    buf: ArrayQueue<RaftMessage>,
    /// A flag indicates whether the queue can still accept messages.
    connected: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Queue {
    /// Creates a Queue that can store at lease `cap` messages.
    fn with_capacity(cap: usize) -> Queue {
        Queue {
            buf: ArrayQueue::new(cap),
            connected: AtomicBool::new(true),
            waker: Mutex::new(None),
        }
    }

    /// Pushes message into the tail of the Queue.
    ///
    /// You are supposed to call `notify` to make sure the message will be sent
    /// finally.
    ///
    /// True when the message is pushed into queue otherwise false.
    fn push(&self, msg: RaftMessage) -> Result<(), DiscardReason> {
        // Another way is pop the old messages, but it makes things
        // complicated.
        if self.connected.load(Ordering::Relaxed) {
            match self.buf.push(msg) {
                Ok(()) => (),
                Err(PushError(_)) => return Err(DiscardReason::Full),
            }
        } else {
            return Err(DiscardReason::Disconnected);
        }
        if self.connected.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(DiscardReason::Disconnected)
        }
    }

    fn disconnect(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    /// Wakes up consumer to retrive message.
    fn notify(&self) {
        if !self.buf.is_empty() {
            let t = self.waker.lock().unwrap().take();
            if let Some(t) = t {
                t.wake();
            }
        }
    }

    /// Gets the buffer len.
    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }

    /// Gets message from the head of the queue.
    fn try_pop(&self) -> Option<RaftMessage> {
        self.buf.pop().ok()
    }

    /// Same as `try_pop` but register interest on readiness when `None` is returned.
    ///
    /// The method should be called in polling context. If the queue is empty,
    /// it will register current polling task for notifications.
    #[inline]
    fn pop(&self, ctx: &Context) -> Option<RaftMessage> {
        match self.buf.pop() {
            Ok(msg) => Some(msg),
            Err(_) => {
                {
                    let mut waker = self.waker.lock().unwrap();
                    *waker = Some(ctx.waker().clone());
                }
                match self.buf.pop() {
                    Ok(msg) => Some(msg),
                    Err(_) => None,
                }
            }
        }
    }
}

trait Buffer {
    type OutputMessage;

    /// Tests if it is full.
    ///
    /// A full buffer should be flushed successfully before calling `push`.
    fn full(&self) -> bool;
    /// Pushes the message into buffer.
    fn push(&mut self, msg: RaftMessage);
    /// Checks if the batch is empty.
    fn empty(&self) -> bool;
    /// Flushes the message to grpc.
    ///
    /// `sender` should be able to accept messages.
    fn flush(
        &mut self,
        sender: &mut ClientCStreamSender<Self::OutputMessage>,
    ) -> grpcio::Result<()>;
    /// Clears all messages and invoke hook before dropping.
    fn clear(&mut self, hook: impl FnMut(&RaftMessage));
}

/// A buffer for BatchRaftMessage.
struct BatchMessageBuffer {
    batch: BatchRaftMessage,
    overflowing: Option<RaftMessage>,
    size: usize,
    cfg: Arc<Config>,
}

impl BatchMessageBuffer {
    fn new(cfg: Arc<Config>) -> BatchMessageBuffer {
        BatchMessageBuffer {
            batch: BatchRaftMessage::default(),
            overflowing: None,
            size: 0,
            cfg,
        }
    }
}

impl Buffer for BatchMessageBuffer {
    type OutputMessage = BatchRaftMessage;

    #[inline]
    fn full(&self) -> bool {
        self.overflowing.is_some()
    }

    #[inline]
    fn push(&mut self, msg: RaftMessage) {
        let mut msg_size = msg.start_key.len() + msg.end_key.len();
        for entry in msg.get_message().get_entries() {
            msg_size += entry.data.len();
        }
        // To avoid building too large batch, we limit each batch's size. Since `msg_size`
        // is estimated, `GRPC_SEND_MSG_BUF` is reserved for errors.
        if self.size > 0
            && (self.size + msg_size + GRPC_SEND_MSG_BUF >= self.cfg.max_grpc_send_msg_len as usize
                || self.batch.get_msgs().len() >= RAFT_MSG_MAX_BATCH_SIZE)
        {
            self.overflowing = Some(msg);
            return;
        }
        self.size += msg_size;
        self.batch.mut_msgs().push(msg);
    }

    #[inline]
    fn empty(&self) -> bool {
        self.batch.get_msgs().is_empty()
    }

    #[inline]
    fn flush(&mut self, sender: &mut ClientCStreamSender<BatchRaftMessage>) -> grpcio::Result<()> {
        let batch = mem::take(&mut self.batch);
        let res = Pin::new(sender).start_send((
            batch,
            WriteFlags::default().buffer_hint(self.overflowing.is_some()),
        ));

        self.size = 0;
        if let Some(more) = self.overflowing.take() {
            self.push(more);
        }
        res
    }

    #[inline]
    fn clear(&mut self, mut hook: impl FnMut(&RaftMessage)) {
        for msg in self.batch.get_msgs() {
            hook(msg);
        }
        self.batch.mut_msgs().clear();

        if let Some(ref msg) = self.overflowing {
            hook(msg);
        }
        self.overflowing.take();
    }
}

/// A buffer for non-batch RaftMessage.
struct MessageBuffer {
    batch: VecDeque<RaftMessage>,
}

impl MessageBuffer {
    fn new() -> MessageBuffer {
        MessageBuffer {
            batch: VecDeque::with_capacity(2),
        }
    }
}

impl Buffer for MessageBuffer {
    type OutputMessage = RaftMessage;

    #[inline]
    fn full(&self) -> bool {
        self.batch.len() >= 2
    }

    #[inline]
    fn push(&mut self, msg: RaftMessage) {
        self.batch.push_back(msg);
    }

    #[inline]
    fn empty(&self) -> bool {
        self.batch.is_empty()
    }

    #[inline]
    fn flush(&mut self, sender: &mut ClientCStreamSender<RaftMessage>) -> grpcio::Result<()> {
        if let Some(msg) = self.batch.pop_front() {
            Pin::new(sender).start_send((
                msg,
                WriteFlags::default().buffer_hint(!self.batch.is_empty()),
            ))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn clear(&mut self, mut hook: impl FnMut(&RaftMessage)) {
        for msg in &self.batch {
            hook(msg);
        }
        self.batch.clear();
    }
}

/// Reporter reports whether a snapshot is sent successfully.
struct SnapshotReporter<T> {
    raft_router: T,
    region_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl<T: RaftStoreRouter<RocksEngine> + 'static> SnapshotReporter<T> {
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
        }

        if let Err(e) =
            self.raft_router
                .report_snapshot_status(self.region_id, self.to_peer_id, status)
        {
            error!(?e;
                "report snapshot to peer failes";
                "to_peer_id" => self.to_peer_id,
                "to_store_id" => self.to_store_id,
                "region_id" => self.region_id,
            );
        }
    }
}

fn report_unreachable<R>(router: &R, msg: &RaftMessage)
where
    R: RaftStoreRouter<RocksEngine>,
{
    let to_peer = msg.get_to_peer();
    if msg.get_message().has_snapshot() {
        let store = to_peer.store_id.to_string();
        REPORT_FAILURE_MSG_COUNTER
            .with_label_values(&["snapshot", &*store])
            .inc();
        let res = router.report_snapshot_status(msg.region_id, to_peer.id, SnapshotStatus::Failure);
        if let Err(e) = res {
            error!(
                ?e;
                "reporting snapshot to peer fails";
                "to_peer_id" => to_peer.id,
                "to_store_id" => to_peer.store_id,
                "region_id" => msg.region_id,
            );
        }
    }
    let _ = router.report_unreachable(msg.region_id, to_peer.id);
}

fn grpc_error_is_unimplemented(e: &grpcio::Error) -> bool {
    if let grpcio::Error::RpcFailure(RpcStatus { ref status, .. }) = e {
        let x = *status == RpcStatusCode::UNIMPLEMENTED;
        return x;
    }
    false
}

/// Struct tracks the lifetime of a `raft` or `batch_raft` RPC.
struct RaftCall<R, M, B> {
    sender: ClientCStreamSender<M>,
    receiver: ClientCStreamReceiver<Done>,
    queue: Arc<Queue>,
    buffer: B,
    router: R,
    snap_scheduler: Scheduler<SnapTask>,
    lifetime: Option<oneshot::Sender<()>>,
    store_id: u64,
    addr: String,
}

impl<R, M, B> RaftCall<R, M, B>
where
    R: RaftStoreRouter<RocksEngine> + 'static,
    B: Buffer<OutputMessage = M>,
{
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

    fn fill_msg(&mut self, ctx: &Context) {
        while !self.buffer.full() {
            let msg = match self.queue.pop(ctx) {
                Some(msg) => msg,
                None => return,
            };
            if msg.get_message().has_snapshot() {
                self.send_snapshot_sock(msg);
                continue;
            } else {
                self.buffer.push(msg);
            }
        }
    }

    fn clean_up(&mut self, sink_err: &Option<grpcio::Error>, recv_err: &Option<grpcio::Error>) {
        error!("connection aborted"; "store_id" => self.store_id, "sink_error" => ?sink_err, "receiver_err" => ?recv_err, "addr" => %self.addr);

        if let Some(tx) = self.lifetime.take() {
            let should_fallback = [sink_err, recv_err]
                .iter()
                .any(|e| e.as_ref().map_or(false, grpc_error_is_unimplemented));
            if should_fallback {
                // Asks backend to fallback.
                let _ = tx.send(());
                return;
            }
        }
        let router = &self.router;
        router.broadcast_unreachable(self.store_id);
    }
}

impl<R, M, B> Future for RaftCall<R, M, B>
where
    R: RaftStoreRouter<RocksEngine> + Unpin + 'static,
    B: Buffer<OutputMessage = M> + Unpin,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<()> {
        let s = &mut *self;
        loop {
            s.fill_msg(ctx);
            if !s.buffer.empty() {
                let mut res = Pin::new(&mut s.sender).poll_ready(ctx);
                if let Poll::Ready(Ok(())) = res {
                    res = Poll::Ready(s.buffer.flush(&mut s.sender));
                }
                match res {
                    Poll::Ready(Ok(())) => continue,
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => {
                        let re = match Pin::new(&mut s.receiver).poll(ctx) {
                            Poll::Ready(Err(e)) => Some(e),
                            _ => None,
                        };
                        s.clean_up(&Some(e), &re);
                        return Poll::Ready(());
                    }
                }
            }

            if let Poll::Ready(Err(e)) = Pin::new(&mut s.sender).poll_flush(ctx) {
                let re = match Pin::new(&mut s.receiver).poll(ctx) {
                    Poll::Ready(Err(e)) => Some(e),
                    _ => None,
                };
                s.clean_up(&Some(e), &re);
                return Poll::Ready(());
            }
            match Pin::new(&mut s.receiver).poll(ctx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(_)) => {
                    info!("connection close"; "store_id" => s.store_id, "addr" => %s.addr);
                    return Poll::Ready(());
                }
                Poll::Ready(Err(e)) => {
                    s.clean_up(&None, &Some(e));
                    return Poll::Ready(());
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ConnectionBuilder<S, R> {
    env: Arc<Environment>,
    cfg: Arc<Config>,
    security_mgr: Arc<SecurityManager>,
    resolver: S,
    router: R,
    snap_scheduler: Scheduler<SnapTask>,
}

impl<S, R> ConnectionBuilder<S, R> {
    pub fn new(
        env: Arc<Environment>,
        cfg: Arc<Config>,
        security_mgr: Arc<SecurityManager>,
        resolver: S,
        router: R,
        snap_scheduler: Scheduler<SnapTask>,
    ) -> ConnectionBuilder<S, R> {
        ConnectionBuilder {
            env,
            cfg,
            security_mgr,
            resolver,
            router,
            snap_scheduler,
        }
    }
}

/// StreamBackEnd watches lifetime of a connection and handles reconnecting,
/// spawn new RPC.
struct StreamBackEnd<S, R> {
    store_id: u64,
    queue: Arc<Queue>,
    builder: ConnectionBuilder<S, R>,
}

impl<S, R> StreamBackEnd<S, R>
where
    S: StoreAddrResolver,
    R: RaftStoreRouter<RocksEngine> + Unpin + 'static,
{
    fn resolve(&self) -> impl Future<Output = server::Result<String>> {
        let (tx, rx) = oneshot::channel();
        let store_id = self.store_id;
        let res = self.builder.resolver.resolve(
            store_id,
            #[allow(unused_mut)]
            Box::new(move |mut addr| {
                {
                    // Wrapping the fail point in a closure, so we can modify
                    // local variables without return.
                    let mut transport_on_resolve_fp = || {
                        fail_point!(_ON_RESOLVE_FP, |sid| if let Some(sid) = sid {
                            use std::mem;
                            let sid: u64 = sid.parse().unwrap();
                            if sid == store_id {
                                mem::swap(&mut addr, &mut Err(box_err!("injected failure")));
                            }
                        })
                    };
                    transport_on_resolve_fp();
                }
                let _ = tx.send(addr);
            }),
        );
        async move {
            res?;
            match rx.await {
                Ok(a) => a,
                Err(_) => Err(server::Error::Other(
                    "failed to receive resolve result".into(),
                )),
            }
        }
    }

    fn clear_pending_message(&self) {
        let len = self.queue.len();
        for _ in 0..len {
            let msg = self.queue.try_pop().unwrap();
            report_unreachable(&self.builder.router, &msg)
        }
    }

    fn connect(&self, addr: &str) -> TikvClient {
        info!("server: new connection with tikv endpoint"; "addr" => addr, "store_id" => self.store_id);

        let cb = ChannelBuilder::new(self.builder.env.clone())
            .stream_initial_window_size(self.builder.cfg.grpc_stream_initial_window_size.0 as i32)
            .max_send_message_len(self.builder.cfg.max_grpc_send_msg_len)
            .keepalive_time(self.builder.cfg.grpc_keepalive_time.0)
            .keepalive_timeout(self.builder.cfg.grpc_keepalive_timeout.0)
            .default_compression_algorithm(self.builder.cfg.grpc_compression_algorithm())
            // hack: so it's different args, grpc will always create a new connection.
            .raw_cfg_int(
                CString::new("random id").unwrap(),
                CONN_ID.fetch_add(1, Ordering::SeqCst),
            );
        let channel = self.builder.security_mgr.connect(cb, addr);
        TikvClient::new(channel)
    }

    fn batch_call(&self, client: &TikvClient, addr: String) -> oneshot::Receiver<()> {
        let (batch_sink, batch_stream) = client.batch_raft().unwrap();
        let (tx, rx) = oneshot::channel();
        let call = RaftCall {
            sender: batch_sink,
            receiver: batch_stream,
            queue: self.queue.clone(),
            buffer: BatchMessageBuffer::new(self.builder.cfg.clone()),
            router: self.builder.router.clone(),
            snap_scheduler: self.builder.snap_scheduler.clone(),
            lifetime: Some(tx),
            store_id: self.store_id,
            addr,
        };
        // TODO: verify it will be notified if client is dropped while env still alive.
        client.spawn(call);
        rx
    }

    fn call(&self, client: &TikvClient, addr: String) -> oneshot::Receiver<()> {
        let (sink, stream) = client.raft().unwrap();
        let (tx, rx) = oneshot::channel();
        let call = RaftCall {
            sender: sink,
            receiver: stream,
            queue: self.queue.clone(),
            buffer: MessageBuffer::new(),
            router: self.builder.router.clone(),
            snap_scheduler: self.builder.snap_scheduler.clone(),
            lifetime: Some(tx),
            store_id: self.store_id,
            addr,
        };
        client.spawn(call);
        rx
    }
}

async fn maybe_backoff(cfg: &Config, last_wake_time: &mut Instant, retry_times: &mut u32) {
    if *retry_times == 0 {
        return;
    }
    let timeout = cfg.raft_client_backoff_step.0 * cmp::min(*retry_times, 5);
    let now = Instant::now();
    if *last_wake_time + timeout < now {
        // We have spent long enough time in last retry, no need to backoff again.
        *last_wake_time = now;
        *retry_times = 0;
        return;
    }
    if let Err(e) = GLOBAL_TIMER_HANDLE.delay(now + timeout).compat().await {
        error_unknown!(?e; "failed to backoff");
    }
    *last_wake_time = Instant::now();
}

/// A future that drives the life cycle of a connection.
///
/// The general progress of connection is:
///
/// 1. resolve address
/// 2. connect
/// 3. make batch call
/// 4. fallback to legacy API if incompatible
///
/// Every failure during the process should trigger retry automatically.
async fn start<S, R>(
    back_end: StreamBackEnd<S, R>,
    conn_id: usize,
    pool: Arc<Mutex<ConnectionPool>>,
) where
    S: StoreAddrResolver + Send,
    R: RaftStoreRouter<RocksEngine> + Unpin + Send + 'static,
{
    let mut last_wake_time = Instant::now();
    let mut retry_times = 0;
    loop {
        maybe_backoff(&back_end.builder.cfg, &mut last_wake_time, &mut retry_times).await;
        retry_times += 1;
        let f = back_end.resolve();
        let addr = match f.await {
            Ok(addr) => {
                RESOLVE_STORE_COUNTER.with_label_values(&["success"]).inc();
                info!("resolve store address ok"; "store_id" => back_end.store_id, "addr" => %addr);
                addr
            }
            Err(e) => {
                RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
                back_end.clear_pending_message();
                error_unknown!(?e; "resolve store address failed"; "store_id" => back_end.store_id,);
                // TOMBSTONE
                if format!("{}", e).contains("has been removed") {
                    let mut pool = pool.lock().unwrap();
                    if let Some(s) = pool.connections.remove(&(back_end.store_id, conn_id)) {
                        s.disconnect();
                    }
                    pool.tombstone_stores.insert(back_end.store_id);
                    return;
                }
                continue;
            }
        };
        let client = back_end.connect(&addr);
        let f = back_end.batch_call(&client, addr.clone());
        let mut res = f.await;
        if res == Ok(()) {
            // If the call is setup successfully, it will never finish. Returning `Ok(())` means the
            // batch_call is not supported, we are probably connect to an old version of TiKV. So we
            // need to fallback to use legacy API.
            let f = back_end.call(&client, addr.clone());
            res = f.await;
        }
        match res {
            Ok(()) => {
                error!("connection fail"; "store_id" => back_end.store_id, "addr" => addr, "err" => "require fallback even with legacy API");
            }
            Err(_) => {
                error!("connection abort"; "store_id" => back_end.store_id, "addr" => addr);
                if retry_times > 1 {
                    // Clears pending messages to avoid consuming high memory when one node is shutdown.
                    back_end.clear_pending_message();
                }
                back_end
                    .builder
                    .router
                    .broadcast_unreachable(back_end.store_id);
            }
        }
    }
}

/// A global connection pool.
///
/// All valid connections should be stored as a record. Once it's removed
/// from the struct, all cache clone should also remove it at some time.
#[derive(Default)]
struct ConnectionPool {
    connections: HashMap<(u64, usize), Arc<Queue>>,
    tombstone_stores: HashSet<u64>,
}

/// Queue in cache.
struct CachedQueue {
    queue: Arc<Queue>,
    /// If a msg is enqueued, but the queue has not been notified for polling,
    /// it will be marked to true. And all dirty queues are expected to be
    /// notified during flushing.
    dirty: bool,
}

/// A raft client that can manages connections correctly.
///
/// A correct usage of raft client is:
///
/// ```text
/// for m in msgs {
///     if !raft_client.send(m) {
///         // handle error.   
///     }
/// }
/// raft_client.flush();
/// ```
pub struct RaftClient<S, R> {
    pool: Arc<Mutex<ConnectionPool>>,
    cache: LruCache<(u64, usize), CachedQueue>,
    need_flush: Vec<(u64, usize)>,
    future_pool: Arc<ThreadPool<TaskCell>>,
    builder: ConnectionBuilder<S, R>,
}

impl<S, R> RaftClient<S, R>
where
    S: StoreAddrResolver + Send + 'static,
    R: RaftStoreRouter<RocksEngine> + Unpin + Send + 'static,
{
    pub fn new(builder: ConnectionBuilder<S, R>) -> RaftClient<S, R> {
        let future_pool = Arc::new(
            yatp::Builder::new(thd_name!("raft-stream"))
                .max_thread_count(1)
                .build_future_pool(),
        );
        RaftClient {
            pool: Arc::default(),
            cache: LruCache::with_capacity_and_sample(0, 7),
            need_flush: vec![],
            future_pool,
            builder,
        }
    }

    /// Loads connection from pool.
    ///
    /// Creates it if it doesn't exist. `false` is returned if such connection
    /// can't be established.
    fn load_stream(&mut self, store_id: u64, conn_id: usize) -> bool {
        let (s, pool_len) = {
            let mut pool = self.pool.lock().unwrap();
            if pool.tombstone_stores.contains(&store_id) {
                let pool_len = pool.connections.len();
                drop(pool);
                self.cache.resize(pool_len);
                return false;
            }
            let conn = pool
                .connections
                .entry((store_id, conn_id))
                .or_insert_with(|| {
                    let queue = Arc::new(Queue::with_capacity(QUEUE_CAPACITY));
                    let back_end = StreamBackEnd {
                        store_id,
                        queue: queue.clone(),
                        builder: self.builder.clone(),
                    };
                    self.future_pool
                        .spawn(start(back_end, conn_id, self.pool.clone()));
                    queue
                })
                .clone();
            (conn, pool.connections.len())
        };
        self.cache.resize(pool_len);
        self.cache.insert(
            (store_id, conn_id),
            CachedQueue {
                queue: s,
                dirty: false,
            },
        );
        true
    }

    /// Sends a message.
    ///
    /// If the message fails to be sent, false is returned. Returning true means the message is
    /// enqueued to buffer. Caller is expected to call `flush` to ensure all buffered messages
    /// are sent out.
    pub fn send(&mut self, msg: RaftMessage) -> result::Result<(), DiscardReason> {
        let store_id = msg.get_to_peer().store_id;
        let conn_id = (msg.region_id % self.builder.cfg.grpc_raft_conn_num as u64) as usize;
        #[allow(unused_mut)]
        let mut transport_on_send_store_fp = || {
            fail_point!(
                "transport_on_send_snapshot",
                msg.get_message().get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot,
                |sid| if let Some(sid) = sid {
                    let sid: u64 = sid.parse().unwrap();
                    if sid == store_id {
                        // Forbid building new connections.
                        fail::cfg(_ON_RESOLVE_FP, &format!("1*return({})", sid)).unwrap();
                        self.cache.remove(&(store_id, conn_id));
                        self.pool
                            .lock()
                            .unwrap()
                            .connections
                            .remove(&(store_id, conn_id));
                    }
                }
            )
        };
        transport_on_send_store_fp();
        loop {
            if let Some(s) = self.cache.get_mut(&(store_id, conn_id)) {
                match s.queue.push(msg) {
                    Ok(_) => {
                        if !s.dirty {
                            s.dirty = true;
                            self.need_flush.push((store_id, conn_id));
                        }
                        return Ok(());
                    }
                    Err(DiscardReason::Full) => {
                        s.queue.notify();
                        s.dirty = false;
                        return Err(DiscardReason::Full);
                    }
                    Err(DiscardReason::Disconnected) => break,
                    Err(DiscardReason::Filtered) => return Err(DiscardReason::Filtered),
                }
            }
            if !self.load_stream(store_id, conn_id) {
                return Err(DiscardReason::Disconnected);
            }
        }
        self.cache.remove(&(store_id, conn_id));
        Err(DiscardReason::Disconnected)
    }

    pub fn need_flush(&self) -> bool {
        !self.need_flush.is_empty()
    }

    /// Flushes all buffered messages.
    pub fn flush(&mut self) {
        if self.need_flush.is_empty() {
            return;
        }
        for id in &self.need_flush {
            if let Some(s) = self.cache.get_mut(id) {
                if s.dirty {
                    s.dirty = false;
                    s.queue.notify();
                }
                continue;
            }
            let l = self.pool.lock().unwrap();
            if let Some(q) = l.connections.get(id) {
                q.notify();
            }
        }
        self.need_flush.clear();
        if self.need_flush.capacity() > 2048 {
            self.need_flush.shrink_to(512);
        }
    }
}

impl<S, R> Clone for RaftClient<S, R>
where
    S: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        RaftClient {
            pool: self.pool.clone(),
            cache: LruCache::with_capacity_and_sample(0, 7),
            need_flush: vec![],
            future_pool: self.future_pool.clone(),
            builder: self.builder.clone(),
        }
    }
}
