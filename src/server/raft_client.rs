// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    ffi::CString,
    marker::Unpin,
    pin::Pin,
    result,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU8, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use collections::{HashMap, HashSet};
use crossbeam::queue::ArrayQueue;
use futures::{
    Future, Sink,
    channel::oneshot,
    compat::Future01CompatExt,
    ready,
    task::{Context, Poll, Waker},
};
use futures_timer::Delay;
use grpcio::{
    CallOption, Channel, ChannelBuilder, ClientCStreamReceiver, ClientCStreamSender, Environment,
    MetadataBuilder, RpcStatusCode, WriteFlags,
};
use grpcio_health::proto::HealthClient;
use kvproto::{
    raft_serverpb::{Done, RaftMessage, RaftSnapshotData},
    tikvpb::{BatchRaftMessage, TikvClient},
};
use protobuf::Message;
use raft::SnapshotStatus;
use raftstore::errors::DiscardReason;
use security::SecurityManager;
use tikv_kv::RaftExtension;
use tikv_util::{
    config::{Tracker, VersionTrack},
    lru::LruCache,
    time::{InstantExt, duration_to_sec},
    timer::GLOBAL_TIMER_HANDLE,
    worker::{Scheduler, Worker},
};
use yatp::{ThreadPool, task::future::TaskCell};

use crate::server::{
    Config, StoreAddrResolver,
    load_statistics::ThreadLoadPool,
    metrics::*,
    resolve::{Error as ResolveError, Result as ResolveResult},
    snap::Task as SnapTask,
};

// Implement health_controller::HealthChecker for HealthChecker to bridge with
// health_controller
impl health_controller::HealthChecker for HealthChecker {
    fn get_all_max_latencies(&self) -> HashMap<u64, f64> {
        self.get_all_max_latencies()
    }
}

pub struct MetadataSourceStoreId {}

impl MetadataSourceStoreId {
    pub const KEY: &'static str = "source_store_id";

    pub fn parse(value: &[u8]) -> u64 {
        let value = std::str::from_utf8(value).unwrap();
        value.parse::<u64>().unwrap()
    }

    pub fn format(id: u64) -> String {
        format!("{}", id)
    }
}

static CONN_ID: AtomicI32 = AtomicI32::new(0);

const _ON_RESOLVE_FP: &str = "transport_snapshot_on_resolve";

#[repr(u8)]
enum ConnState {
    Established = 0,
    /// The connection is paused and may be resumed later.
    Paused = 1,
    /// The connection is closed and removed from the connection pool.
    Disconnected = 2,
}

impl From<u8> for ConnState {
    fn from(state: u8) -> ConnState {
        match state {
            0 => ConnState::Established,
            1 => ConnState::Paused,
            2 => ConnState::Disconnected,
            _ => unreachable!(),
        }
    }
}

/// A quick queue for sending raft messages.
struct Queue {
    buf: ArrayQueue<(RaftMessage, Instant)>,
    conn_state: AtomicU8,
    waker: Mutex<Option<Waker>>,
}

impl Queue {
    /// Creates a Queue that can store at lease `cap` messages.
    fn with_capacity(cap: usize) -> Queue {
        Queue {
            buf: ArrayQueue::new(cap),
            conn_state: AtomicU8::new(ConnState::Established as u8),
            waker: Mutex::new(None),
        }
    }

    /// Pushes message into the tail of the Queue.
    ///
    /// You are supposed to call `notify` to make sure the message will be sent
    /// finally.
    ///
    /// True when the message is pushed into queue otherwise false.
    fn push(&self, msg_with_time: (RaftMessage, Instant)) -> Result<(), DiscardReason> {
        match self.conn_state.load(Ordering::SeqCst).into() {
            ConnState::Established => match self.buf.push(msg_with_time) {
                Ok(()) => Ok(()),
                Err(_) => Err(DiscardReason::Full),
            },
            ConnState::Paused => Err(DiscardReason::Paused),
            ConnState::Disconnected => Err(DiscardReason::Disconnected),
        }
    }

    fn set_conn_state(&self, s: ConnState) {
        self.conn_state.store(s as u8, Ordering::SeqCst);
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
    fn try_pop(&self) -> Option<(RaftMessage, Instant)> {
        self.buf.pop()
    }

    /// Same as `try_pop` but register interest on readiness when `None` is
    /// returned.
    ///
    /// The method should be called in polling context. If the queue is empty,
    /// it will register current polling task for notifications.
    #[inline]
    fn pop(&self, ctx: &Context<'_>) -> Option<(RaftMessage, Instant)> {
        self.buf.pop().or_else(|| {
            {
                let mut waker = self.waker.lock().unwrap();
                *waker = Some(ctx.waker().clone());
            }
            self.buf.pop()
        })
    }
}

trait Buffer {
    type OutputMessage;

    /// Tests if it is full.
    ///
    /// A full buffer should be flushed successfully before calling `push`.
    fn full(&self) -> bool;
    /// Pushes the message into buffer.
    fn push(&mut self, msg_with_time: (RaftMessage, Instant));
    /// Checks if the batch is empty.
    fn empty(&self) -> bool;
    /// Flushes the message to grpc.
    ///
    /// `sender` should be able to accept messages.
    fn flush(
        &mut self,
        sender: &mut ClientCStreamSender<Self::OutputMessage>,
    ) -> grpcio::Result<()>;

    /// If the buffer is not full, suggest whether sender should wait
    /// for next message.
    fn wait_hint(&mut self) -> Option<Duration> {
        None
    }
}

/// A buffer for BatchRaftMessage.
struct BatchMessageBuffer {
    batch: Vec<(RaftMessage, Instant)>,
    overflowing: Option<(RaftMessage, Instant)>,
    size: usize,
    cfg: Config,
    cfg_tracker: Tracker<Config>,
    loads: Arc<ThreadLoadPool>,
}

impl BatchMessageBuffer {
    fn new(
        global_cfg_track: &Arc<VersionTrack<Config>>,
        loads: Arc<ThreadLoadPool>,
    ) -> BatchMessageBuffer {
        let cfg_tracker = Arc::clone(global_cfg_track).tracker("raft-client-buffer".into());
        let cfg = global_cfg_track.value().clone();
        BatchMessageBuffer {
            batch: Vec::with_capacity(cfg.raft_msg_max_batch_size),
            overflowing: None,
            size: 0,
            cfg,
            cfg_tracker,
            loads,
        }
    }

    #[inline]
    fn message_size(msg: &RaftMessage) -> usize {
        let mut msg_size = msg.start_key.len()
            + msg.end_key.len()
            + msg.get_message().context.len()
            + msg.extra_ctx.len()
            // index: 3, term: 2, data tag and size: 3, entry tag and size: 3
            + 11 * msg.get_message().get_entries().len();
        for entry in msg.get_message().get_entries() {
            msg_size += entry.data.len();
        }
        msg_size
    }

    #[inline]
    fn maybe_refresh_config(&mut self) {
        if let Some(new_cfg) = self.cfg_tracker.any_new() {
            self.cfg = new_cfg.clone();
        }
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.batch.clear();
        self.size = 0;
        self.overflowing = None;
        // try refresh config
        self.maybe_refresh_config();
    }
}

impl Buffer for BatchMessageBuffer {
    type OutputMessage = BatchRaftMessage;

    #[inline]
    fn full(&self) -> bool {
        self.overflowing.is_some()
    }

    #[inline]
    fn push(&mut self, msg_with_time: (RaftMessage, Instant)) {
        let msg_size = Self::message_size(&msg_with_time.0);
        // To avoid building too large batch, we limit each batch's size. Since
        // `msg_size` is estimated, `GRPC_SEND_MSG_BUF` is reserved for errors.
        if self.size > 0
            && (self.size + msg_size + self.cfg.raft_client_grpc_send_msg_buffer
                >= self.cfg.max_grpc_send_msg_len as usize
                || self.batch.len() >= self.cfg.raft_msg_max_batch_size)
        {
            self.overflowing = Some(msg_with_time);
            return;
        }
        self.size += msg_size;
        self.batch.push(msg_with_time);
    }

    #[inline]
    fn empty(&self) -> bool {
        self.batch.is_empty()
    }

    #[inline]
    fn flush(&mut self, sender: &mut ClientCStreamSender<BatchRaftMessage>) -> grpcio::Result<()> {
        let mut batch_msgs = BatchRaftMessage::default();
        self.batch.drain(..).for_each(|(msg, time)| {
            RAFT_MESSAGE_DURATION
                .send_wait
                .observe(time.saturating_elapsed().as_secs_f64());
            batch_msgs.msgs.push(msg);
        });
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        batch_msgs.last_observed_time = now;

        let res = Pin::new(sender).start_send((
            batch_msgs,
            WriteFlags::default().buffer_hint(self.overflowing.is_some()),
        ));

        self.size = 0;
        if let Some(more) = self.overflowing.take() {
            self.push(more);
        }

        // try refresh config after flush. `max_grpc_send_msg_len` and
        // `raft_msg_max_batch_size` can impact the buffer push logic, but since
        // they are soft restriction, we check config change at here to avoid
        // affact performance since `push` is a hot path.
        self.maybe_refresh_config();

        res
    }

    #[inline]
    fn wait_hint(&mut self) -> Option<Duration> {
        let wait_dur = self.cfg.heavy_load_wait_duration();
        if !wait_dur.is_zero() {
            if self.loads.current_thread_in_heavy_load() {
                Some(wait_dur)
            } else {
                None
            }
        } else {
            None
        }
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
    fn push(&mut self, msg_with_time: (RaftMessage, Instant)) {
        self.batch.push_back(msg_with_time.0);
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
}

/// Reporter reports whether a snapshot is sent successfully.
struct SnapshotReporter<R> {
    raft_router: R,
    region_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl<R> SnapshotReporter<R>
where
    R: RaftExtension + 'static,
{
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

        self.raft_router
            .report_snapshot_status(self.region_id, self.to_peer_id, status);
    }
}

fn report_unreachable(router: &impl RaftExtension, msg: &RaftMessage) {
    let to_peer = msg.get_to_peer();
    if msg.get_message().has_snapshot() {
        let store = to_peer.store_id.to_string();
        REPORT_FAILURE_MSG_COUNTER
            .with_label_values(&["snapshot", &*store])
            .inc();
        router.report_snapshot_status(msg.region_id, to_peer.id, SnapshotStatus::Failure);
    }
    router.report_peer_unreachable(msg.region_id, to_peer.id);
}

fn grpc_error_is_unimplemented(e: &grpcio::Error) -> bool {
    if let grpcio::Error::RpcFailure(ref status) = e {
        status.code() == RpcStatusCode::UNIMPLEMENTED
    } else {
        false
    }
}

/// Struct tracks the lifetime of a `raft` or `batch_raft` RPC.
struct AsyncRaftSender<R, M, B> {
    sender: ClientCStreamSender<M>,
    queue: Arc<Queue>,
    buffer: B,
    router: R,
    snap_scheduler: Scheduler<SnapTask>,
    addr: String,
    flush_timeout: Option<Delay>,
}

impl<R, M, B> AsyncRaftSender<R, M, B>
where
    R: RaftExtension + 'static,
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

    fn fill_msg(&mut self, ctx: &Context<'_>) {
        while !self.buffer.full() {
            let msg_with_time = match self.queue.pop(ctx) {
                Some(msg_with_time) => msg_with_time,
                None => return,
            };
            if msg_with_time.0.get_message().has_snapshot() {
                let mut snapshot = RaftSnapshotData::default();
                snapshot
                    .merge_from_bytes(msg_with_time.0.get_message().get_snapshot().get_data())
                    .unwrap();
                // Witness's snapshot must be empty, no need to send snapshot files, report
                // immediately
                if !snapshot.get_meta().get_for_witness() {
                    self.send_snapshot_sock(msg_with_time.0);
                    continue;
                } else {
                    let rep = self.new_snapshot_reporter(&msg_with_time.0);
                    rep.report(SnapshotStatus::Finish);
                }
            }
            self.buffer.push(msg_with_time);
        }
    }
}

impl<R, M, B> Future for AsyncRaftSender<R, M, B>
where
    R: RaftExtension + Unpin + 'static,
    B: Buffer<OutputMessage = M> + Unpin,
{
    type Output = grpcio::Result<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<grpcio::Result<()>> {
        let s = &mut *self;
        loop {
            s.fill_msg(ctx);
            if !s.buffer.empty() {
                // Then it's the first time visit this block since last flush.
                if s.flush_timeout.is_none() {
                    ready!(Pin::new(&mut s.sender).poll_ready(ctx))?;
                }
                // Only set up a timer if buffer is not full.
                if !s.buffer.full() {
                    if s.flush_timeout.is_none() {
                        // Only set up a timer if necessary.
                        if let Some(wait_time) = s.buffer.wait_hint() {
                            s.flush_timeout = Some(Delay::new(wait_time));
                        }
                    }

                    // It will be woken up again when the timer fires or new messages are enqueued.
                    if let Some(timeout) = &mut s.flush_timeout {
                        if Pin::new(timeout).poll(ctx).is_pending() {
                            return Poll::Pending;
                        } else {
                            RAFT_MESSAGE_FLUSH_COUNTER.delay.inc_by(1);
                        }
                    } else {
                        RAFT_MESSAGE_FLUSH_COUNTER.eof.inc_by(1);
                    }
                } else if s.flush_timeout.is_some() {
                    RAFT_MESSAGE_FLUSH_COUNTER.full_after_delay.inc_by(1);
                } else {
                    RAFT_MESSAGE_FLUSH_COUNTER.full.inc_by(1);
                }

                // So either enough messages are batched up or don't need to wait or wait
                // timeouts.
                s.flush_timeout.take();
                ready!(Poll::Ready(s.buffer.flush(&mut s.sender)))?;
                continue;
            }

            if let Poll::Ready(Err(e)) = Pin::new(&mut s.sender).poll_flush(ctx) {
                return Poll::Ready(Err(e));
            }
            return Poll::Pending;
        }
    }
}

#[derive(PartialEq)]
enum RaftCallRes {
    // the call is not supported, probably due to visiting to older version TiKV
    Fallback,
    // the connection is aborted or closed
    Disconnected,
}

struct RaftCall<R, M, B> {
    sender: AsyncRaftSender<R, M, B>,
    receiver: ClientCStreamReceiver<Done>,
    lifetime: Option<oneshot::Sender<RaftCallRes>>,
    store_id: u64,
}

impl<R, M, B> RaftCall<R, M, B>
where
    R: RaftExtension + Unpin + 'static,
    B: Buffer<OutputMessage = M> + Unpin,
{
    async fn poll(&mut self) {
        let res = futures::join!(&mut self.sender, &mut self.receiver);
        if let (Ok(()), Ok(Done { .. })) = res {
            info!("connection close"; "store_id" => self.store_id, "addr" => %self.sender.addr);
            if let Some(tx) = self.lifetime.take() {
                let _ = tx.send(RaftCallRes::Disconnected);
            }
            return;
        }

        let (sink_err, recv_err) = (res.0.err(), res.1.err());
        warn!(
            "connection aborted";
            "store_id" => self.store_id,
            "sink_error" => ?sink_err,
            "receiver_err" => ?recv_err,
            "addr" => %self.sender.addr,
        );
        if let Some(tx) = self.lifetime.take() {
            let should_fallback = [sink_err, recv_err]
                .iter()
                .any(|e| e.as_ref().is_some_and(grpc_error_is_unimplemented));

            let res = if should_fallback {
                // Asks backend to fallback.
                RaftCallRes::Fallback
            } else {
                RaftCallRes::Disconnected
            };
            let _ = tx.send(res);
        }
    }
}

#[derive(Clone)]
pub struct ConnectionBuilder<S, R> {
    env: Arc<Environment>,
    cfg: Arc<VersionTrack<Config>>,
    security_mgr: Arc<SecurityManager>,
    resolver: S,
    router: R,
    snap_scheduler: Scheduler<SnapTask>,
    loads: Arc<ThreadLoadPool>,
}

impl<S, R> ConnectionBuilder<S, R> {
    pub fn new(
        env: Arc<Environment>,
        cfg: Arc<VersionTrack<Config>>,
        security_mgr: Arc<SecurityManager>,
        resolver: S,
        router: R,
        snap_scheduler: Scheduler<SnapTask>,
        loads: Arc<ThreadLoadPool>,
    ) -> ConnectionBuilder<S, R> {
        ConnectionBuilder {
            env,
            cfg,
            security_mgr,
            resolver,
            router,
            snap_scheduler,
            loads,
        }
    }
}

/// StreamBackEnd watches lifetime of a connection and handles reconnecting,
/// spawn new RPC.
struct StreamBackEnd<S, R> {
    self_store_id: u64,
    store_id: u64,
    queue: Arc<Queue>,
    builder: ConnectionBuilder<S, R>,
}

impl<S, R> StreamBackEnd<S, R>
where
    S: StoreAddrResolver,
    R: RaftExtension + Unpin + 'static,
{
    fn resolve(&self) -> impl Future<Output = ResolveResult<String>> {
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
                                // Sleep some time to avoid race between enqueuing message and
                                // resolving address.
                                std::thread::sleep(std::time::Duration::from_millis(10));
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
                Err(_) => Err(ResolveError::Other(
                    "failed to receive resolve result".into(),
                )),
            }
        }
    }

    fn clear_pending_message(&self, reason: &str) {
        let len = self.queue.len();
        for _ in 0..len {
            let msg_with_time = self.queue.try_pop().unwrap();
            report_unreachable(&self.builder.router, &msg_with_time.0)
        }
        REPORT_FAILURE_MSG_COUNTER
            .with_label_values(&[reason, &self.store_id.to_string()])
            .inc_by(len as u64);
    }

    fn connect(&self, addr: &str) -> Channel {
        info!("server: new connection with tikv endpoint"; "addr" => addr, "store_id" => self.store_id);

        let cfg = self.builder.cfg.value();
        let cb = ChannelBuilder::new(self.builder.env.clone())
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
            .keepalive_time(cfg.grpc_keepalive_time.0)
            .keepalive_timeout(cfg.grpc_keepalive_timeout.0)
            .default_compression_algorithm(cfg.grpc_compression_algorithm())
            .default_gzip_compression_level(cfg.grpc_gzip_compression_level)
            .default_grpc_min_message_size_to_compress(cfg.grpc_min_message_size_to_compress)
            .max_reconnect_backoff(cfg.raft_client_max_backoff.0)
            .initial_reconnect_backoff(cfg.raft_client_initial_reconnect_backoff.0)
            // hack: so it's different args, grpc will always create a new connection.
            .raw_cfg_int(
                CString::new("random id").unwrap(),
                CONN_ID.fetch_add(1, Ordering::SeqCst),
            );
        self.builder.security_mgr.connect(cb, addr)
    }

    fn batch_call(&self, client: &TikvClient, addr: String) -> oneshot::Receiver<RaftCallRes> {
        let (batch_sink, batch_stream) = client.batch_raft_opt(self.get_call_option()).unwrap();

        let (tx, rx) = oneshot::channel();
        let mut call = RaftCall {
            sender: AsyncRaftSender {
                sender: batch_sink,
                queue: self.queue.clone(),
                buffer: BatchMessageBuffer::new(&self.builder.cfg, self.builder.loads.clone()),
                router: self.builder.router.clone(),
                snap_scheduler: self.builder.snap_scheduler.clone(),
                addr,
                flush_timeout: None,
            },
            receiver: batch_stream,
            lifetime: Some(tx),
            store_id: self.store_id,
        };
        // TODO: verify it will be notified if client is dropped while env still alive.
        client.spawn(async move {
            call.poll().await;
        });
        rx
    }

    fn call(&self, client: &TikvClient, addr: String) -> oneshot::Receiver<RaftCallRes> {
        let (sink, stream) = client.raft_opt(self.get_call_option()).unwrap();

        let (tx, rx) = oneshot::channel();
        let mut call = RaftCall {
            sender: AsyncRaftSender {
                sender: sink,
                queue: self.queue.clone(),
                buffer: MessageBuffer::new(),
                router: self.builder.router.clone(),
                snap_scheduler: self.builder.snap_scheduler.clone(),
                addr,
                flush_timeout: None,
            },
            receiver: stream,
            lifetime: Some(tx),
            store_id: self.store_id,
        };
        client.spawn(async move {
            call.poll().await;
        });
        rx
    }

    fn get_call_option(&self) -> CallOption {
        let mut metadata = MetadataBuilder::with_capacity(1);
        let value = MetadataSourceStoreId::format(self.self_store_id);
        metadata
            .add_str(MetadataSourceStoreId::KEY, &value)
            .unwrap();
        CallOption::default().headers(metadata.build())
    }
}

async fn maybe_backoff(backoff: Duration, last_wake_time: &mut Option<Instant>) {
    let now = Instant::now();
    if let Some(last) = *last_wake_time {
        if last + backoff < now {
            // We have spent long enough time in last retry, no need to backoff again.
            *last_wake_time = Some(now);
            return;
        }
    } else {
        *last_wake_time = Some(now);
        return;
    }

    if let Err(e) = GLOBAL_TIMER_HANDLE.delay(now + backoff).compat().await {
        error_unknown!(?e; "failed to backoff");
    }
    *last_wake_time = Some(Instant::now());
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
    R: RaftExtension + Unpin + Send + 'static,
{
    let mut last_wake_time = None;
    let backoff_duration = back_end.builder.cfg.value().raft_client_max_backoff.0;
    let mut addr_channel = None;
    let mut begin = None;
    let mut try_count = 0;
    loop {
        if begin.is_none() {
            begin = Some(Instant::now());
        }
        try_count += 1;
        maybe_backoff(backoff_duration, &mut last_wake_time).await;
        let f = back_end.resolve();
        let addr = match f.await {
            Ok(addr) => {
                RESOLVE_STORE_COUNTER.with_label_values(&["success"]).inc();
                info!("resolve store address ok"; "store_id" => back_end.store_id, "addr" => %addr);
                addr
            }
            Err(e) => {
                RESOLVE_STORE_COUNTER.with_label_values(&["failed"]).inc();
                back_end.clear_pending_message("resolve");
                error_unknown!(?e; "resolve store address failed"; "store_id" => back_end.store_id,);
                if let ResolveError::StoreTombstone(_) = e {
                    let mut pool = pool.lock().unwrap();
                    if let Some(conn_info) = pool.connections.remove(&(back_end.store_id, conn_id))
                    {
                        conn_info.queue.set_conn_state(ConnState::Disconnected);
                    }
                    pool.tombstone_stores.insert(back_end.store_id);
                    return;
                }
                continue;
            }
        };

        // reuse channel if the address is the same.
        if addr_channel
            .as_ref()
            .is_none_or(|(_, prev_addr)| prev_addr != &addr)
        {
            addr_channel = Some((back_end.connect(&addr), addr.clone()));
        }
        let channel = addr_channel.as_ref().unwrap().0.clone();

        debug!("connecting to store"; "store_id" => back_end.store_id, "addr" => %addr);
        if !channel.wait_for_connected(backoff_duration).await {
            warn!("wait connect timeout"; "store_id" => back_end.store_id, "addr" => addr);

            // Clears pending messages to avoid consuming high memory when one node is
            // shutdown.
            back_end.clear_pending_message("unreachable");

            back_end
                .builder
                .router
                .report_store_unreachable(back_end.store_id);
            continue;
        } else {
            let wait_conn_duration = begin.unwrap_or_else(Instant::now).elapsed();
            info!("connection established";
                "store_id" => back_end.store_id,
                "addr" => %addr,
                "cost" => ?wait_conn_duration,
                "msg_count" => ?back_end.queue.len(),
                "try_count" => try_count,
            );
            RAFT_CLIENT_WAIT_CONN_READY_DURATION_HISTOGRAM_VEC
                .with_label_values(&[addr.as_str()])
                .observe(duration_to_sec(wait_conn_duration));
            begin = None;
            try_count = 0;

            // Update the channel in ConnectionPool for health check
            {
                let mut pool_guard = pool.lock().unwrap();
                if let Some(conn_info) = pool_guard
                    .connections
                    .get_mut(&(back_end.store_id, conn_id))
                {
                    conn_info.channel = Some(channel.clone());
                }
            }
        }

        let client = TikvClient::new(channel);
        let f = back_end.batch_call(&client, addr.clone());
        let mut res = f.await; // block here until the stream call is closed or aborted.
        if res == Ok(RaftCallRes::Fallback) {
            // If the call is setup successfully, it will never finish. Returning
            // `UnImplemented` means the batch_call is not supported, we are probably
            // connect to an old version of TiKV. So we need to fallback to use
            // legacy API.
            let f = back_end.call(&client, addr.clone());
            res = f.await;
        }
        match res {
            Ok(RaftCallRes::Fallback) => {
                error!("connection fail"; "store_id" => back_end.store_id, "addr" => addr, "err" => "require fallback even with legacy API");
            }
            // Err(_) should be tx is dropped
            Ok(RaftCallRes::Disconnected) | Err(_) => {
                warn!("connection abort"; "store_id" => back_end.store_id, "addr" => addr);
                REPORT_FAILURE_MSG_COUNTER
                    .with_label_values(&["unreachable", &back_end.store_id.to_string()])
                    .inc_by(1);
                back_end
                    .builder
                    .router
                    .report_store_unreachable(back_end.store_id);
                addr_channel = None;

                // Clear the channel when connection is lost
                {
                    let mut pool_guard = pool.lock().unwrap();
                    if let Some(conn_info) = pool_guard
                        .connections
                        .get_mut(&(back_end.store_id, conn_id))
                    {
                        conn_info.channel = None;
                    }
                }
            }
        }
    }
}

/// Connection information for a specific store connection.
///
/// This struct contains the essential components needed to manage a connection
/// to a remote TiKV store, including message queuing and gRPC channel
/// management.
struct ConnectionInfo {
    /// Message queue for buffering raft messages before sending.
    ///
    /// The queue is shared between the sender (RaftClient) and the background
    /// connection task. It handles connection states
    /// (established/paused/disconnected) and provides backpressure when
    /// full.
    queue: Arc<Queue>,

    /// Optional gRPC channel for the connection.
    ///
    /// This is `None` when the connection is not established or has been lost.
    /// When `Some`, it contains an active gRPC channel that can be used for
    /// health checks and other communication with the remote store.
    /// The channel is updated by the background connection task when the
    /// connection state changes.
    channel: Option<Channel>,
}

/// A global connection pool.
///
/// All valid connections should be stored as a record. Once it's removed
/// from the struct, all cache clone should also remove it at some time.
#[derive(Default)]
struct ConnectionPool {
    connections: HashMap<(u64, usize), ConnectionInfo>,
    tombstone_stores: HashSet<u64>,
    store_allowlist: Vec<u64>,
}

impl ConnectionPool {
    fn set_store_allowlist(&mut self, stores: Vec<u64>) {
        self.store_allowlist = stores;
        let need_pause_check = !self.store_allowlist.is_empty();
        let allowlist = &self.store_allowlist;

        for (&(store_id, _), conn_info) in self.connections.iter_mut() {
            let mut state = ConnState::Established;
            if need_pause_check && !allowlist.contains(&store_id) {
                state = ConnState::Paused;
            }
            conn_info.queue.set_conn_state(state);
        }
    }

    fn need_pause(&self, store_id: u64) -> bool {
        !self.store_allowlist.is_empty() && !self.store_allowlist.contains(&store_id)
    }
}

/// Queue in cache.
struct CachedQueue {
    queue: Arc<Queue>,
    /// If a msg is enqueued, but the queue has not been notified for polling,
    /// it will be marked to true. And all dirty queues are expected to be
    /// notified during flushing.
    dirty: bool,
    /// Mark if the connection is full.
    full: bool,
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
    self_store_id: u64,
    pool: Arc<Mutex<ConnectionPool>>,
    cache: LruCache<(u64, usize), CachedQueue>,
    need_flush: Vec<(u64, usize)>,
    full_stores: Vec<(u64, usize)>,
    future_pool: Arc<ThreadPool<TaskCell>>,
    builder: ConnectionBuilder<S, R>,
    last_hash: (u64, u64),

    health_checker: HealthChecker,
}

impl<S, R> RaftClient<S, R>
where
    S: StoreAddrResolver + Send + 'static,
    R: RaftExtension + Unpin + Send + 'static,
{
    pub fn new(
        self_store_id: u64,
        builder: ConnectionBuilder<S, R>,
        inspect_interval: Duration,
        background_worker: Worker,
    ) -> Self {
        let future_pool = Arc::new(
            yatp::Builder::new(thd_name!("raft-stream"))
                .max_thread_count(1)
                .build_future_pool(),
        );
        let pool: Arc<Mutex<ConnectionPool>> = Arc::default();
        let health_checker = HealthChecker::new(
            self_store_id,
            pool.clone(),
            inspect_interval,
            background_worker.clone(),
        );
        RaftClient {
            self_store_id,
            pool,
            cache: LruCache::with_capacity_and_sample(0, 7),
            need_flush: vec![],
            full_stores: vec![],
            future_pool,
            builder,
            last_hash: (0, 0),
            health_checker,
        }
    }

    // Start the network inspection
    pub fn start_network_inspection(&self) {
        let health_checker = self.health_checker.clone();
        self.future_pool.spawn(async move {
            health_checker.run().await;
        });
    }

    /// Get the maximum latency for a specific store. Just for test
    /// Returns the latency in milliseconds, or None if no latency recorded for
    /// this store
    pub fn get_max_latency(&self, store_id: u64) -> Option<f64> {
        self.health_checker.get_max_latency(store_id)
    }

    /// Get all maximum latencies. Just for test
    /// Returns a HashMap of store_id -> max_latency_ms
    pub fn get_all_max_latencies(&self) -> HashMap<u64, f64> {
        self.health_checker.get_all_max_latencies()
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
            let need_pause = pool.need_pause(store_id);
            let conn_info = pool
                .connections
                .entry((store_id, conn_id))
                .or_insert_with(|| {
                    let queue = Arc::new(Queue::with_capacity(
                        self.builder.cfg.value().raft_client_queue_size,
                    ));
                    if need_pause {
                        queue.set_conn_state(ConnState::Paused);
                    }
                    let back_end = StreamBackEnd {
                        self_store_id: self.self_store_id,
                        store_id,
                        queue: queue.clone(),
                        builder: self.builder.clone(),
                    };
                    self.future_pool
                        .spawn(start(back_end, conn_id, self.pool.clone()));
                    ConnectionInfo {
                        queue: queue.clone(),
                        channel: None,
                    }
                });
            let queue = conn_info.queue.clone();
            (queue, pool.connections.len())
        };
        self.cache.resize(pool_len);
        self.cache.insert(
            (store_id, conn_id),
            CachedQueue {
                queue: s,
                dirty: false,
                full: false,
            },
        );
        true
    }

    /// Sends a message.
    ///
    /// If the message fails to be sent, false is returned. Returning true means
    /// the message is enqueued to buffer. Caller is expected to call `flush` to
    /// ensure all buffered messages are sent out.
    pub fn send(&mut self, msg: RaftMessage) -> result::Result<(), DiscardReason> {
        let wait_send_start = Instant::now();
        let store_id = msg.get_to_peer().store_id;
        let grpc_raft_conn_num = self.builder.cfg.value().grpc_raft_conn_num as u64;
        let conn_id = if grpc_raft_conn_num == 1 {
            0
        } else {
            if self.last_hash.0 == 0 || msg.region_id != self.last_hash.0 {
                self.last_hash = (
                    msg.region_id,
                    seahash::hash(&msg.region_id.to_ne_bytes()) % grpc_raft_conn_num,
                );
            };
            self.last_hash.1 as usize
        };

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
                match s.queue.push((msg, wait_send_start)) {
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
                        if !s.full {
                            s.full = true;
                            self.full_stores.push((store_id, conn_id));
                        }
                        return Err(DiscardReason::Full);
                    }
                    Err(DiscardReason::Disconnected) => break,
                    Err(DiscardReason::Paused) => return Err(DiscardReason::Paused),
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
        !self.need_flush.is_empty() || !self.full_stores.is_empty()
    }

    fn flush_full_metrics(&mut self) {
        if self.full_stores.is_empty() {
            return;
        }

        for id in &self.full_stores {
            if let Some(s) = self.cache.get_mut(id) {
                s.full = false;
            }
            REPORT_FAILURE_MSG_COUNTER
                .with_label_values(&["full", &id.0.to_string()])
                .inc();
        }
        self.full_stores.clear();
        if self.full_stores.capacity() > 2048 {
            self.full_stores.shrink_to(512);
        }
    }

    /// Flushes all buffered messages.
    pub fn flush(&mut self) {
        self.flush_full_metrics();
        if self.need_flush.is_empty() {
            return;
        }
        let mut counter = 0;
        for id in &self.need_flush {
            if let Some(s) = self.cache.get_mut(id) {
                if s.dirty {
                    s.dirty = false;
                    counter += 1;
                    s.queue.notify();
                }
                continue;
            }
            let l = self.pool.lock().unwrap();
            if let Some(conn_info) = l.connections.get(id) {
                counter += 1;
                conn_info.queue.notify();
            }
        }
        self.need_flush.clear();
        if self.need_flush.capacity() > 2048 {
            self.need_flush.shrink_to(512);
        }
        RAFT_MESSAGE_FLUSH_COUNTER.wake.inc_by(counter);
    }

    pub fn set_store_allowlist(&mut self, stores: Vec<u64>) {
        let mut p = self.pool.lock().unwrap();
        p.set_store_allowlist(stores);
    }

    pub fn get_health_checker(&self) -> HealthChecker {
        self.health_checker.clone()
    }
}

impl<S, R> Clone for RaftClient<S, R>
where
    S: Clone,
    R: Clone,
{
    fn clone(&self) -> Self {
        RaftClient {
            self_store_id: self.self_store_id,
            pool: self.pool.clone(),
            cache: LruCache::with_capacity_and_sample(0, 7),
            need_flush: vec![],
            full_stores: vec![],
            future_pool: self.future_pool.clone(),
            builder: self.builder.clone(),
            last_hash: (0, 0),
            health_checker: self.health_checker.clone(),
        }
    }
}

#[derive(Clone)]
pub struct HealthChecker {
    self_store_id: u64,
    pool: Arc<Mutex<ConnectionPool>>,
    inspect_interval: Duration,
    // Store shutdown senders for each store inspection task
    task_handles: Arc<Mutex<HashMap<u64, oneshot::Sender<()>>>>,
    background_worker: Worker,
    // Store the maximum latency and its timestamp that began to check for each store (in
    // milliseconds, Instant)
    max_latencies: Arc<Mutex<HashMap<u64, (f64, Instant)>>>,
}

impl HealthChecker {
    fn new(
        self_store_id: u64,
        pool: Arc<Mutex<ConnectionPool>>,
        inspect_interval: Duration,
        background_worker: Worker,
    ) -> Self {
        HealthChecker {
            self_store_id,
            pool,
            inspect_interval,
            task_handles: Arc::new(Mutex::new(HashMap::default())),
            background_worker,
            max_latencies: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    /// Main control loop that manages inspection tasks for all stores
    pub async fn run(&self) {
        if self.inspect_interval.is_zero() {
            info!("Network inspection is disabled.");
            return;
        }
        let mut watched_stores = HashSet::default();
        let check_interval = (|| {
            fail_point!("network_inspection_interval", |_| {
                Duration::from_millis(10)
            });
            Duration::from_secs(30)
        })();

        loop {
            // Check current stores in the pool
            let current_stores: HashSet<u64> = {
                let pool_guard = self.pool.lock().unwrap();
                pool_guard
                    .connections
                    .keys()
                    .map(|(store_id, _)| *store_id)
                    .collect()
            };

            // Find stores that need to be added
            for store_id in current_stores.difference(&watched_stores) {
                self.start_store_inspection(*store_id);
            }

            // Find stores that need to be removed
            for store_id in watched_stores.difference(&current_stores) {
                self.stop_store_inspection(*store_id);
            }

            watched_stores = current_stores;
            if let Err(e) = GLOBAL_TIMER_HANDLE
                .delay(Instant::now() + check_interval)
                .compat()
                .await
            {
                error!("failed to delay in store inspection management"; "error" => ?e);
            }
        }
    }

    /// Start inspection task for a specific store
    fn start_store_inspection(&self, store_id: u64) {
        info!("Starting health check inspection for store"; "store_id" => store_id);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Store the shutdown sender
        {
            let mut handles = self.task_handles.lock().unwrap();
            handles.insert(store_id, shutdown_tx);
        }

        // Spawn the inspection task using the background worker
        let pool = self.pool.clone();
        let inspect_interval = self.inspect_interval;
        let max_latencies = self.max_latencies.clone();
        let self_store_id = self.self_store_id;

        self.background_worker.spawn_async_task(async move {
            Self::store_inspection_loop(
                self_store_id,
                store_id,
                pool,
                inspect_interval,
                max_latencies,
                shutdown_rx,
            )
            .await;
        });
    }

    /// Stop inspection task for a specific store
    fn stop_store_inspection(&self, store_id: u64) {
        info!("Stopping health check inspection for store"; "store_id" => store_id);

        let shutdown_tx = {
            let mut handles = self.task_handles.lock().unwrap();
            handles.remove(&store_id)
        };

        if let Some(tx) = shutdown_tx {
            let _ = tx.send(());
        }
    }

    /// Main inspection loop for a single store
    async fn store_inspection_loop(
        self_store_id: u64,
        store_id: u64,
        pool: Arc<Mutex<ConnectionPool>>,
        inspect_interval: Duration,
        max_latencies: Arc<Mutex<HashMap<u64, (f64, Instant)>>>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        let mut last_check_time = Instant::now();

        loop {
            // Check if we should shutdown
            if let Ok(Some(())) = shutdown_rx.try_recv() {
                info!("Shutting down health check inspection for store"; "store_id" => store_id);
                // Reset max latency for this store
                {
                    let mut max_latencies = max_latencies.lock().unwrap();
                    max_latencies.remove(&store_id);
                }
                break;
            }

            // Wait for the specified interval
            if let Err(e) = GLOBAL_TIMER_HANDLE
                .delay(last_check_time + inspect_interval)
                .compat()
                .await
            {
                error!("failed to latency in network inspection"; "store_id" => store_id, "error" => ?e);
                continue;
            }

            last_check_time = Instant::now();

            // Get connections for this specific store
            let store_connections: Vec<(usize, Arc<Queue>, Option<Channel>)> = {
                let pool_guard = pool.lock().unwrap();
                pool_guard
                    .connections
                    .iter()
                    .filter_map(|((sid, conn_id), conn_info)| {
                        if *sid == store_id {
                            Some((*conn_id, conn_info.queue.clone(), conn_info.channel.clone()))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            // If no connections for this store, continue
            if store_connections.is_empty() {
                continue;
            }

            // Perform health check for each connection of this store
            for (conn_id, queue, channel_opt) in store_connections {
                // Skip if the connection is not established
                if queue.conn_state.load(Ordering::SeqCst) != ConnState::Established as u8 {
                    continue;
                }

                // Get the channel if available
                if let Some(channel) = channel_opt {
                    Self::perform_health_check(
                        self_store_id,
                        store_id,
                        conn_id,
                        channel,
                        max_latencies.clone(),
                    )
                    .await;

                    break;
                }
            }
        }
    }

    async fn perform_health_check(
        self_store_id: u64,
        store_id: u64,
        conn_id: usize,
        channel: Channel,
        max_latencies: Arc<Mutex<HashMap<u64, (f64, Instant)>>>,
    ) {
        let start_time = Instant::now();

        let health_client = HealthClient::new(channel);

        let mut req = grpcio_health::proto::HealthCheckRequest::new();
        req.set_service("".to_string()); // Empty service name for overall health

        // Slow score calculation algorithm
        //   1. By default, the latency is sampled every 100ms, and latencies exceeding
        //      1s are considered timeouts.
        //   2. A slow score is calculated every 3 samples. If there are n timeouts in
        //      the 3 samples, the slow score is multiplied by 2^(n/3).
        // If a request does not return within 5s, it will resulting in 40(4s) timeout
        // samples. Thus, the slow score will reach 2^(40/3) > 100. Since the upper
        // limit of the slow score (100) has already been reached, the probing
        // can be stopped.
        let call_opt = CallOption::default().timeout(Duration::from_secs(5));

        let now = Instant::now();
        {
            let mut latencies = max_latencies.lock().unwrap();
            latencies.entry(store_id).insert_entry((0.0, now));
        }
        match health_client.check_async_opt(&req, call_opt) {
            Ok(resp_future) => {
                let _ = resp_future.await;
                let elapsed = start_time.elapsed();

                let latency_ms = elapsed.as_secs_f64() * 1000.0;
                {
                    let mut latencies = max_latencies.lock().unwrap();
                    let entry = latencies.entry(store_id).or_insert((0.0, now));
                    entry.0 = latency_ms;
                }
            }
            Err(e) => {
                warn!(
                    "Failed to create health check request";
                    "self_store_id" => self_store_id,
                    "store_id" => store_id,
                    "conn_id" => conn_id,
                    "error" => ?e
                );
            }
        }
    }

    /// Get the maximum latency for a specific store
    /// Returns the latency in milliseconds, or None if no latency recorded for
    /// this store
    /// This method just for test
    pub fn get_max_latency(&self, store_id: u64) -> Option<f64> {
        let latencies = self.max_latencies.lock().unwrap();
        let now = Instant::now();
        latencies.get(&store_id).map(|(lat, ts)| {
            if *lat == 0.0 {
                now.duration_since(*ts).as_secs_f64() * 1000.
            } else {
                *lat
            }
        })
    }

    /// Get all maximum latencies
    /// Returns a HashMap of store_id -> max_latency_ms
    pub fn get_all_max_latencies(&self) -> HashMap<u64, f64> {
        let latencies = self.max_latencies.lock().unwrap();
        let now = Instant::now();
        latencies
            .iter()
            .map(|(k, (lat, ts))| {
                let ret = if *lat == 0.0 {
                    now.duration_since(*ts).as_secs_f64() * 1000.
                } else {
                    *lat
                };
                (*k, ret)
            })
            .collect()
    }
}

impl Drop for HealthChecker {
    fn drop(&mut self) {
        info!(
            "Shutting down HealthChecker, sending shutdown signals to all running inspection tasks"
        );

        // Send shutdown signals to all running inspection tasks
        let mut handles = self.task_handles.lock().unwrap();
        for (store_id, shutdown_tx) in handles.drain() {
            info!("Sending shutdown signal to inspection task for store"; "store_id" => store_id);
            if shutdown_tx.send(()).is_err() {
                warn!("Failed to send shutdown signal to inspection task for store"; "store_id" => store_id);
            }
        }

        info!("HealthChecker shutdown complete, all inspection tasks signaled to stop");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kvproto::{metapb::RegionEpoch, raft_serverpb::RaftMessage};
    use raft::eraftpb::Snapshot;

    use super::*;
    use crate::server::load_statistics::ThreadLoadPool;

    #[test]
    fn test_push_raft_message_with_context() {
        let mut msg_buf = BatchMessageBuffer::new(
            &Arc::new(VersionTrack::new(Config::default())),
            Arc::new(ThreadLoadPool::with_threshold(100)),
        );
        for i in 0..2 {
            let context_len = msg_buf.cfg.max_grpc_send_msg_len as usize;
            let context = vec![0; context_len];
            let mut msg = RaftMessage::default();
            msg.set_region_id(1);
            let mut region_epoch = RegionEpoch::default();
            region_epoch.conf_ver = 1;
            region_epoch.version = 0x123456;
            msg.set_region_epoch(region_epoch);
            msg.set_start_key(b"12345".to_vec());
            msg.set_end_key(b"67890".to_vec());
            msg.mut_message().set_snapshot(Snapshot::default());
            msg.mut_message().set_commit(0);
            if i != 0 {
                msg.mut_message().set_context(context.into());
            }
            msg_buf.push((msg, Instant::now()));
        }
        assert!(msg_buf.full());
    }

    #[test]
    fn test_push_raft_message_with_extra_ctx() {
        let mut msg_buf = BatchMessageBuffer::new(
            &Arc::new(VersionTrack::new(Config::default())),
            Arc::new(ThreadLoadPool::with_threshold(100)),
        );

        for i in 0..2 {
            let ctx_len = msg_buf.cfg.max_grpc_send_msg_len as usize;
            let ctx = vec![0; ctx_len];
            let mut msg = RaftMessage::default();
            msg.set_region_id(1);
            let mut region_epoch = RegionEpoch::default();
            region_epoch.conf_ver = 1;
            region_epoch.version = 0x123456;
            msg.set_region_epoch(region_epoch);
            msg.set_start_key(b"12345".to_vec());
            msg.set_end_key(b"67890".to_vec());
            msg.mut_message().set_snapshot(Snapshot::default());
            msg.mut_message().set_commit(0);
            if i != 0 {
                msg.set_extra_ctx(ctx);
            }
            msg_buf.push((msg, Instant::now()));
        }
        assert!(msg_buf.full());
    }

    fn new_test_msg(size: usize) -> RaftMessage {
        let mut msg = RaftMessage::default();
        msg.set_region_id(1);
        let mut region_epoch = RegionEpoch::default();
        region_epoch.conf_ver = 1;
        region_epoch.version = 0x123456;
        msg.set_region_epoch(region_epoch);
        msg.set_start_key(vec![0; size]);
        msg.set_end_key(vec![]);
        msg.mut_message().set_snapshot(Snapshot::default());
        msg.mut_message().set_commit(0);
        assert_eq!(BatchMessageBuffer::message_size(&msg), size);
        msg
    }

    #[test]
    fn test_push_raft_message_cfg_change() {
        let version_track = Arc::new(VersionTrack::new(Config::default()));
        let mut msg_buf = BatchMessageBuffer::new(
            &version_track,
            Arc::new(ThreadLoadPool::with_threshold(100)),
        );

        let default_grpc_msg_len = msg_buf.cfg.max_grpc_send_msg_len as usize;
        let max_msg_len = default_grpc_msg_len - msg_buf.cfg.raft_client_grpc_send_msg_buffer;
        msg_buf.push((new_test_msg(max_msg_len), Instant::now()));
        assert!(!msg_buf.full());
        msg_buf.push((new_test_msg(1), Instant::now()));
        assert!(msg_buf.full());

        // update config
        let _ = version_track.update(|cfg| -> Result<(), ()> {
            cfg.max_grpc_send_msg_len *= 2;
            Ok(())
        });
        msg_buf.clear();

        let new_max_msg_len =
            default_grpc_msg_len * 2 - msg_buf.cfg.raft_client_grpc_send_msg_buffer;
        for _i in 0..2 {
            msg_buf.push((new_test_msg(new_max_msg_len / 2 - 1), Instant::now()));
            assert!(!msg_buf.full());
        }
        msg_buf.push((new_test_msg(2), Instant::now()));
        assert!(msg_buf.full());
    }

    #[bench]
    fn bench_client_buffer_push(b: &mut test::Bencher) {
        let version_track = Arc::new(VersionTrack::new(Config::default()));
        let mut msg_buf = BatchMessageBuffer::new(
            &version_track,
            Arc::new(ThreadLoadPool::with_threshold(100)),
        );

        b.iter(|| {
            for _i in 0..10 {
                msg_buf.push(test::black_box((new_test_msg(1024), Instant::now())));
            }
            // run clear to mock flush.
            msg_buf.clear();

            test::black_box(&mut msg_buf);
        });
    }

    #[test]
    fn test_health_checker_creation() {
        let self_store_id = 1;
        let pool: Arc<Mutex<ConnectionPool>> = Arc::default();
        let interval = Duration::from_millis(100);
        let background_worker = Worker::new("test-worker");

        let health_checker =
            HealthChecker::new(self_store_id, pool.clone(), interval, background_worker);

        assert_eq!(health_checker.self_store_id, self_store_id);
        assert!(Arc::ptr_eq(&health_checker.pool, &pool));
        assert_eq!(health_checker.inspect_interval, interval);
        assert!(health_checker.task_handles.lock().unwrap().is_empty());
        assert!(health_checker.max_latencies.lock().unwrap().is_empty());
    }

    #[test]
    fn test_health_checker_latency_management() {
        let self_store_id = 1;
        let pool: Arc<Mutex<ConnectionPool>> = Arc::default();
        let interval = Duration::from_millis(100);
        let background_worker = Worker::new("test-worker");

        let health_checker = HealthChecker::new(self_store_id, pool, interval, background_worker);

        // Manually add some latencies for testing
        use std::{thread::sleep, time::Duration as StdDuration};
        let now = Instant::now();
        {
            let mut latencies = health_checker.max_latencies.lock().unwrap();
            latencies.insert(1, (100.5, now));
            latencies.insert(2, (200.8, now));
            latencies.insert(3, (300.2, now));
        }

        // Test get
        assert_eq!(health_checker.get_max_latency(1), Some(100.5));
        assert_eq!(health_checker.get_max_latency(2), Some(200.8));
        assert_eq!(health_checker.get_max_latency(4), None);

        let all_latencies = health_checker.get_all_max_latencies();
        assert_eq!(all_latencies.len(), 3);
        assert_eq!(all_latencies[&1], 100.5);
        assert_eq!(all_latencies[&2], 200.8);
        assert_eq!(all_latencies[&3], 300.2);

        let now = Instant::now();
        {
            let mut latencies = health_checker.max_latencies.lock().unwrap();
            latencies.insert(1, (0.0, now));
        }
        sleep(StdDuration::from_millis(200));
        let v = health_checker.get_max_latency(1).unwrap();
        assert_ge!(v, 200.0);

        let all_latencies = health_checker.get_all_max_latencies();
        assert_eq!(all_latencies.len(), 3);
        assert_ge!(all_latencies[&1], 200.0);
        assert_eq!(all_latencies[&2], 200.8);
        assert_eq!(all_latencies[&3], 300.2);
    }

    #[tokio::test]
    async fn test_health_checker_start_stop_store_inspection() {
        let self_store_id = 1;
        let pool: Arc<Mutex<ConnectionPool>> = Arc::default();
        let interval = Duration::from_millis(50);
        let background_worker = Worker::new("test-worker");

        let health_checker = HealthChecker::new(self_store_id, pool, interval, background_worker);
        let store_id = 2;

        // Start inspection for a store
        health_checker.start_store_inspection(store_id);

        // Verify the task handle was added
        {
            let handles = health_checker.task_handles.lock().unwrap();
            assert!(handles.contains_key(&store_id));
        }

        // Stop inspection for the store
        health_checker.stop_store_inspection(store_id);

        // Verify the task handle was removed
        {
            let handles = health_checker.task_handles.lock().unwrap();
            assert!(!handles.contains_key(&store_id));
        }
    }
}
