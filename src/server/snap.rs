// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    io::{Error as IoError, ErrorKind, Read, Write},
    pin::Pin,
    result::Result as StdResult,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant as StdInstant},
};

use file_system::{IoType, WithIoType};
use futures::{
    channel::oneshot::Sender,
    compat::Future01CompatExt,
    future::{select, Either, Future, TryFutureExt},
    pin_mut,
    sink::SinkExt,
    stream::{Stream, StreamExt, TryStreamExt},
    task::{Context, Poll},
};
use futures_util::FutureExt;
use kvproto::{
    pdpb::SnapshotStat,
    raft_serverpb::{
        RaftMessage, RaftSnapshotData, SnapshotChunk, TabletSnapshotRequest, TabletSnapshotResponse,
    },
    tikvpb_grpc::tikv_client::TikvClient,
};
use protobuf::Message;
use raftstore::store::{SnapEntry, SnapKey, SnapManager, Snapshot};
use security::SecurityManager;
use tikv_kv::RaftExtension;
use tikv_util::{
    box_err,
    config::{Tracker, VersionTrack, MIB},
    time::{Instant, UnixSecs},
    timer::GLOBAL_TIMER_HANDLE,
    worker::Runnable,
    DeferContext,
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tonic::transport::Channel;

use super::{metrics::*, Config, Error, Result};
use crate::{server::tablet_snap::NoSnapshotCache, tikv_util::sys::thread::ThreadBuildWrapper};

pub type Callback = Box<dyn FnOnce(Result<()>) + Send>;

pub const DEFAULT_POOL_SIZE: usize = 4;

// the default duration before a snapshot sending task is canceled.
const SNAP_SEND_TIMEOUT_DURATION: Duration = Duration::from_secs(600);
// the minimum expected send speed for sending snapshot, this is used to avoid
// timeout too early when the snapshot size is too big.
const MIN_SNAP_SEND_SPEED: u64 = MIB;

#[inline]
fn get_snap_timeout(size: u64) -> Duration {
    let timeout = (|| {
        fail_point!("snap_send_duration_timeout", |t| -> Duration {
            let t = t.unwrap().parse::<u64>();
            Duration::from_millis(t.unwrap())
        });
        SNAP_SEND_TIMEOUT_DURATION
    })();
    let max_expected_dur = Duration::from_secs(size / MIN_SNAP_SEND_SPEED);
    std::cmp::max(timeout, max_expected_dur)
}

/// A task for either receiving Snapshot or sending Snapshot
pub enum Task {
    Recv {
        stream: tonic::Streaming<SnapshotChunk>,
        tx: Sender<tonic::Result<()>>,
    },
    RecvTablet {
        stream: tonic::Streaming<TabletSnapshotRequest>,
        tx: futures::channel::mpsc::UnboundedSender<tonic::Result<TabletSnapshotResponse>>,
    },
    Send {
        addr: String,
        msg: RaftMessage,
        cb: Callback,
    },
    RefreshConfigEvent,
    Validate(Box<dyn FnOnce(&Config) + Send>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Recv { .. } => write!(f, "Recv"),
            Task::RecvTablet { .. } => write!(f, "RecvTablet"),
            Task::Send {
                ref addr, ref msg, ..
            } => write!(f, "Send Snap[to: {}, snap: {:?}]", addr, msg),
            Task::RefreshConfigEvent => write!(f, "Refresh configuration"),
            Task::Validate(_) => write!(f, "Validate snap worker config"),
        }
    }
}

struct SnapChunk {
    first: Option<SnapshotChunk>,
    snap: Box<Snapshot>,
    remain_bytes: usize,
}

pub const SNAP_CHUNK_LEN: usize = 1024 * 1024;

impl Stream for SnapChunk {
    type Item = Result<SnapshotChunk>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(t) = self.first.take() {
            return Poll::Ready(Some(Ok(t)));
        }

        let mut buf = match self.remain_bytes {
            0 => return Poll::Ready(None),
            n if n > SNAP_CHUNK_LEN => vec![0; SNAP_CHUNK_LEN],
            n => vec![0; n],
        };
        let result = self.snap.read_exact(buf.as_mut_slice());
        match result {
            Ok(_) => {
                self.remain_bytes -= buf.len();
                let mut chunk = SnapshotChunk::default();
                chunk.set_data(buf);
                Poll::Ready(Some(Ok(chunk)))
            }
            Err(e) => Poll::Ready(Some(Err(box_err!("failed to read snapshot chunk: {}", e)))),
        }
    }
}

struct SnapChunkWrapper {
    chunk: Arc<Mutex<SnapChunk>>,
}

impl SnapChunkWrapper {
    fn new(chunk: Arc<Mutex<SnapChunk>>) -> Self {
        Self { chunk }
    }
}

impl Stream for SnapChunkWrapper {
    type Item = SnapshotChunk;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut chunk = self.chunk.lock().unwrap();
        Pin::new(&mut *chunk)
            .poll_next(cx)
            .map(|i| i.map(|r| r.unwrap()))
    }
}

pub struct SendStat {
    key: SnapKey,
    total_size: u64,
    elapsed: Duration,
}

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the
/// snapshot file.
pub fn send_snap(
    mgr: SnapManager,
    security_mgr: Arc<SecurityManager>,
    cfg: &Config,
    addr: &str,
    msg: RaftMessage,
    handle: tokio::runtime::Handle,
) -> Result<impl Future<Output = Result<SendStat>>> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();

    let send_timer = SEND_SNAP_HISTOGRAM.start_coarse_timer();

    let (key, snap_start, generate_duration_sec) = {
        let snap = msg.get_message().get_snapshot();
        let mut snap_data = RaftSnapshotData::default();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            return Err(Error::Io(IoError::new(ErrorKind::Other, e)));
        }
        let key = SnapKey::from_region_snap(msg.get_region_id(), snap);
        let snap_start = snap_data.get_meta().get_start();
        let generate_duration_sec = snap_data.get_meta().get_generate_duration_sec();
        (key, snap_start, generate_duration_sec)
    };

    mgr.register(key.clone(), SnapEntry::Sending);
    let deregister = {
        let (mgr, key) = (mgr.clone(), key.clone());
        DeferContext::new(move || mgr.deregister(&key, &SnapEntry::Sending))
    };

    let s = box_try!(mgr.get_snapshot_for_sending(&key));
    if !s.exists() {
        return Err(box_err!("missing snap file: {:?}", s.path()));
    }
    let total_size = s.total_size();
    SNAP_LIMIT_TRANSPORT_BYTES_COUNTER_STATIC
        .send
        .inc_by(total_size);
    let mut chunks = {
        let mut first_chunk = SnapshotChunk::default();
        first_chunk.set_message(msg);

        SnapChunk {
            first: Some(first_chunk),
            snap: s,
            remain_bytes: total_size as usize,
        }
    };

    let addr = tikv_util::format_url(addr);
    let channel = Channel::from_shared(addr)
        .unwrap()
        //.tls_config()
        .initial_stream_window_size(cfg.grpc_stream_initial_window_size.0 as u32)
        .http2_keep_alive_interval(cfg.grpc_keepalive_time.0)
        .keep_alive_timeout(cfg.grpc_keepalive_timeout.0)
        .executor(tikv_util::RuntimeExec::new(handle))
        .connect_lazy();

    // let channel = security_mgr.connect(cb, addr);
    let mut client = TikvClient::new(channel);

    let send_task = async move {
        let chunks = Arc::new(Mutex::new(chunks));
        let task = client
            .snapshot(SnapChunkWrapper::new(chunks.clone()))
            .map_err(Error::Grpc);

        let wait_timeout = GLOBAL_TIMER_HANDLE
            .delay(StdInstant::now() + get_snap_timeout(total_size))
            .compat();
        let recv_result = {
            pin_mut!(task, wait_timeout);
            match select(task, wait_timeout).await {
                Either::Left((r, _)) => r,
                Either::Right((..)) => Err(Error::Other(box_err!("send snapshot timeout"))),
            }
        };
        send_timer.observe_duration();
        drop(deregister);
        drop(client);

        fail_point!("snapshot_delete_after_send");
        mgr.delete_snapshot(&key, &chunks.lock().unwrap().snap, true);
        match recv_result {
            Ok(_) => {
                let cost = UnixSecs::now().into_inner().saturating_sub(snap_start);
                let send_duration_sec = timer.saturating_elapsed().as_secs();
                // it should ignore if the duration of snapshot is less than 1s to decrease the
                // grpc data size.
                if cost >= 1 {
                    let mut stat = SnapshotStat::default();
                    stat.set_region_id(key.region_id);
                    stat.set_transport_size(total_size);
                    stat.set_generate_duration_sec(generate_duration_sec);
                    stat.set_send_duration_sec(send_duration_sec);
                    stat.set_total_duration_sec(cost);
                    mgr.collect_stat(stat);
                }
                // TODO: improve it after rustc resolves the bug.
                // Call `info` in the closure directly will cause rustc
                // panic with `Cannot create local mono-item for DefId`.
                Ok(SendStat {
                    key,
                    total_size,
                    elapsed: timer.saturating_elapsed(),
                })
            }
            Err(e) => Err(e),
        }
    };
    Ok(send_task)
}

struct RecvSnapContext {
    key: SnapKey,
    file: Option<Box<Snapshot>>,
    raft_msg: RaftMessage,
    io_type: IoType,
    start: Instant,
}

impl RecvSnapContext {
    fn new(head_chunk: Option<SnapshotChunk>, snap_mgr: &SnapManager) -> Result<Self> {
        // head_chunk is None means the stream is empty.
        let mut head = head_chunk.ok_or_else(|| Error::Other("empty gRPC stream".into()))?;
        if !head.has_message() {
            return Err(box_err!("no raft message in the first chunk"));
        }

        let meta = head.take_message();
        let key = match SnapKey::from_snap(meta.get_message().get_snapshot()) {
            Ok(k) => k,
            Err(e) => return Err(box_err!("failed to create snap key: {:?}", e)),
        };

        let data = meta.get_message().get_snapshot().get_data();
        let mut snapshot = RaftSnapshotData::default();
        snapshot.merge_from_bytes(data)?;
        let io_type = if snapshot.get_meta().get_for_balance() {
            IoType::LoadBalance
        } else {
            IoType::Replication
        };
        let _with_io_type = WithIoType::new(io_type);

        let snap = {
            let s = match snap_mgr.get_snapshot_for_receiving(&key, snapshot.take_meta()) {
                Ok(s) => s,
                Err(e) => return Err(box_err!("{} failed to create snapshot file: {:?}", key, e)),
            };

            if s.exists() {
                let p = s.path();
                info!("snapshot file already exists, skip receiving"; "snap_key" => %key, "file" => p);
                None
            } else {
                Some(s)
            }
        };

        Ok(RecvSnapContext {
            key,
            file: snap,
            raft_msg: meta,
            io_type,
            start: Instant::now(),
        })
    }

    fn finish<R: RaftExtension>(self, raft_router: R) -> Result<()> {
        let _with_io_type = WithIoType::new(self.io_type);
        let key = self.key;
        if let Some(mut file) = self.file {
            info!("saving snapshot file"; "snap_key" => %key, "file" => file.path());
            if let Err(e) = file.save() {
                let path = file.path();
                let e = box_err!("{} failed to save snapshot file {}: {:?}", key, path, e);
                return Err(e);
            }
        }
        raft_router.feed(self.raft_msg, true);
        info!("saving all snapshot files"; "snap_key" => %key, "takes" => ?self.start.saturating_elapsed());
        Ok(())
    }
}

fn recv_snap<R: RaftExtension + 'static>(
    stream: tonic::Streaming<SnapshotChunk>,
    tx: Sender<tonic::Result<()>>,
    snap_mgr: SnapManager,
    raft_router: R,
) -> impl Future<Output = Result<()>> {
    let recv_task = async move {
        let mut stream = stream.map_err(Error::from);
        let head = stream.next().await.transpose()?;
        let mut context = RecvSnapContext::new(head, &snap_mgr)?;
        if context.file.is_none() {
            return context.finish(raft_router);
        }
        let context_key = context.key.clone();
        let total_size = context.file.as_ref().unwrap().total_size();
        SNAP_LIMIT_TRANSPORT_BYTES_COUNTER_STATIC
            .recv
            .inc_by(total_size);
        snap_mgr.register(context.key.clone(), SnapEntry::Receiving);
        defer!(snap_mgr.deregister(&context_key, &SnapEntry::Receiving));
        while let Some(item) = stream.next().await {
            fail_point!("receiving_snapshot_net_error", |_| {
                Err(box_err!("{} failed to receive snapshot", context_key))
            });
            let mut chunk = item?;
            let data = chunk.take_data();
            if data.is_empty() {
                return Err(box_err!("{} receive chunk with empty data", context.key));
            }
            let f = context.file.as_mut().unwrap();
            let _with_io_type = WithIoType::new(context.io_type);
            if let Err(e) = Write::write_all(&mut *f, &data) {
                let key = &context.key;
                let path = context.file.as_mut().unwrap().path();
                let e = box_err!("{} failed to write snapshot file {}: {}", key, path, e);
                return Err(e);
            }
        }
        context.finish(raft_router)
    };
    async move {
        match recv_task.await {
            Ok(()) => tx.send(Ok(())),
            Err(e) => {
                let status = tonic::Status::unknown(format!("{:?}", e));
                tx.send(Err(status))
            }
        }
        .map_err(|e| Error::Grpc(tonic::Status::unknown(format!("{:?}", e))))
    }
}

pub struct Runner<R: RaftExtension> {
    snap_mgr: SnapManager,
    pool: Runtime,
    raft_router: R,
    security_mgr: Arc<SecurityManager>,
    cfg_tracker: Tracker<Config>,
    cfg: Config,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
    grpc_handle: tokio::runtime::Handle,
}

impl<R: RaftExtension + 'static> Runner<R> {
    // `can_receive_tablet_snapshot` being true means we are using tiflash engine
    // within a raft group with raftstore-v2. It is set be true to enable runner
    // to receive tablet snapshot from v2.
    pub fn new(
        snap_mgr: SnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
        cfg: Arc<VersionTrack<Config>>,
        grpc_handle: tokio::runtime::Handle,
    ) -> Self {
        let cfg_tracker = cfg.clone().tracker("snap-sender".to_owned());
        let config = cfg.value().clone();
        let snap_worker = Runner {
            snap_mgr,
            pool: RuntimeBuilder::new_multi_thread()
                .thread_name(thd_name!("snap-sender"))
                .with_sys_hooks()
                .worker_threads(DEFAULT_POOL_SIZE)
                .build()
                .unwrap(),
            raft_router: r,
            security_mgr,
            cfg_tracker,
            cfg: config,
            sending_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
            grpc_handle,
        };
        snap_worker
    }

    fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            let limit = if incoming.snap_io_max_bytes_per_sec.0 > 0 {
                incoming.snap_io_max_bytes_per_sec.0 as f64
            } else {
                f64::INFINITY
            };
            let max_total_size = if incoming.snap_max_total_size.0 > 0 {
                incoming.snap_max_total_size.0
            } else {
                u64::MAX
            };
            self.snap_mgr.set_speed_limit(limit);
            self.snap_mgr.set_max_total_snap_size(max_total_size);
            info!("refresh snapshot manager config";
            "speed_limit"=> limit,
            "max_total_snap_size"=> max_total_size);
            self.cfg = incoming.clone();
        }
    }

    fn receiving_busy(&self) -> Option<tonic::Status> {
        let task_num = self.recving_count.load(Ordering::SeqCst);
        if task_num >= self.cfg.concurrent_recv_snap_limit {
            warn!("too many recving snapshot tasks, ignore");
            return Some(tonic::Status::resource_exhausted(format!(
                "the number of received snapshot tasks {} exceeded the limitation {}",
                task_num, self.cfg.concurrent_recv_snap_limit
            )));
        }

        None
    }
}

impl<R: RaftExtension + 'static> Runnable for Runner<R> {
    type Task = Task;

    fn run(&mut self, mut task: Task) {
        match task {
            Task::Recv { stream, tx } => {
                if let Some(status) = self.receiving_busy() {
                    if let Err(e) = tx.send(Err(status)) {
                        warn!("report RecvTablet result failed"; "err" => ?e);
                    }
                    return;
                }

                SNAP_TASK_COUNTER_STATIC.recv.inc();

                let snap_mgr = self.snap_mgr.clone();
                let raft_router = self.raft_router.clone();
                let recving_count = Arc::clone(&self.recving_count);
                recving_count.fetch_add(1, Ordering::SeqCst);
                let task = async move {
                    let result = recv_snap(stream, tx, snap_mgr, raft_router).await;
                    recving_count.fetch_sub(1, Ordering::SeqCst);
                    if let Err(e) = result {
                        error!("failed to recv snapshot"; "err" => %e);
                    }
                };
                self.pool.spawn(task);
            }
            Task::RecvTablet { stream, tx } => {
                // let tablet_snap_mgr = match self.snap_mgr.tablet_snap_manager() {
                // Some(s) => s.clone(),
                // None => {
                // let status = tonic::Status::unimplemented("tablet snap is not supported");
                // if let Err(e) = tx.send(Err(status)) {
                // warn!("report RecvTablet result failed"; "err" => ?e);
                // }
                // return;
                // }
                // };
                //
                // if let Some(status) = self.receiving_busy() {
                // self.pool.spawn(sink.fail(status));
                // return;
                // }
                //
                // SNAP_TASK_COUNTER_STATIC.recv_v2.inc();
                //
                // let raft_router = self.raft_router.clone();
                // let recving_count = self.recving_count.clone();
                // recving_count.fetch_add(1, Ordering::SeqCst);
                // let limiter = self.snap_mgr.limiter().clone();
                // let snap_mgr_v1 = self.snap_mgr.clone();
                // let task = async move {
                // let result = crate::server::tablet_snap::recv_snap(
                // stream,
                // sink,
                // tablet_snap_mgr,
                // raft_router,
                // NoSnapshotCache, // do not use cache in v1
                // limiter,
                // Some(snap_mgr_v1),
                // )
                // .await;
                // recving_count.fetch_sub(1, Ordering::SeqCst);
                // if let Err(e) = result {
                // error!("failed to recv snapshot"; "err" => %e);
                // }
                // };
                // self.pool.spawn(task);
                todo!()
            }
            Task::Send { addr, msg, cb } => {
                fail_point!("send_snapshot");
                let region_id = msg.get_region_id();
                if self.sending_count.load(Ordering::SeqCst) >= self.cfg.concurrent_send_snap_limit
                {
                    warn!(
                        "too many sending snapshot tasks, drop Send Snap[to: {}, snap: {:?}]",
                        addr, msg
                    );
                    cb(Err(Error::Other("Too many sending snapshot tasks".into())));
                    return;
                }
                SNAP_TASK_COUNTER_STATIC.send.inc();

                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let sending_count = Arc::clone(&self.sending_count);
                sending_count.fetch_add(1, Ordering::SeqCst);
                let send_task = send_snap(
                    mgr,
                    security_mgr,
                    &self.cfg.clone(),
                    &addr,
                    msg,
                    self.grpc_handle.clone(),
                );
                let task = async move {
                    let res = match send_task {
                        Err(e) => Err(e),
                        Ok(f) => f.await,
                    };
                    match res {
                        Ok(stat) => {
                            info!(
                                "sent snapshot";
                                "region_id" => stat.key.region_id,
                                "snap_key" => %stat.key,
                                "size" => stat.total_size,
                                "duration" => ?stat.elapsed
                            );
                            cb(Ok(()));
                        }
                        Err(e) => {
                            error!("failed to send snap"; "to_addr" => addr, "region_id" => region_id, "err" => ?e);
                            cb(Err(e));
                        }
                    };
                    sending_count.fetch_sub(1, Ordering::SeqCst);
                };

                self.pool.spawn(task);
            }
            Task::RefreshConfigEvent => {
                self.refresh_cfg();
            }
            Task::Validate(f) => {
                f(&self.cfg);
            }
        }
    }
}
