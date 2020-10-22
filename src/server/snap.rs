// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{Future, TryFutureExt};
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use futures::task::{Context, Poll};
use grpcio::{
    ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcStatus, RpcStatusCode,
    WriteFlags,
};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_serverpb::{Done, SnapshotChunk};
use kvproto::tikvpb::TikvClient;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use engine_rocks::RocksEngine;
use engine_traits::KvEngine;
use raftstore::router::RaftStoreRouter;
use raftstore::store::{GenericSnapshot, SnapEntry, SnapKey, SnapManager};
use security::SecurityManager;
use tikv_util::worker::Runnable;
use tikv_util::DeferContext;

use super::metrics::*;
use super::{Config, Error, Result};

pub type Callback = Box<dyn FnOnce(Result<()>) + Send>;

const DEFAULT_POOL_SIZE: usize = 4;

/// A task for either receiving Snapshot or sending Snapshot
pub enum Task {
    Recv {
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    },
    Send {
        addr: String,
        msg: RaftMessage,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Recv { .. } => write!(f, "Recv"),
            Task::Send {
                ref addr, ref msg, ..
            } => write!(f, "Send Snap[to: {}, snap: {:?}]", addr, msg),
        }
    }
}

struct SnapChunk {
    first: Option<SnapshotChunk>,
    snap: Box<dyn GenericSnapshot>,
    remain_bytes: usize,
}

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

impl Stream for SnapChunk {
    type Item = Result<(SnapshotChunk, WriteFlags)>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(t) = self.first.take() {
            let write_flags = WriteFlags::default().buffer_hint(true);
            return Poll::Ready(Some(Ok((t, write_flags))));
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
                Poll::Ready(Some(Ok((chunk, WriteFlags::default().buffer_hint(true)))))
            }
            Err(e) => Poll::Ready(Some(Err(box_err!("failed to read snapshot chunk: {}", e)))),
        }
    }
}

struct SendStat {
    key: SnapKey,
    total_size: u64,
    elapsed: Duration,
}

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the snapshot file.
fn send_snap(
    env: Arc<Environment>,
    mgr: SnapManager,
    security_mgr: Arc<SecurityManager>,
    cfg: &Config,
    addr: &str,
    msg: RaftMessage,
) -> Result<impl Future<Output = Result<SendStat>>> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();

    let send_timer = SEND_SNAP_HISTOGRAM.start_coarse_timer();

    let key = {
        let snap = msg.get_message().get_snapshot();
        SnapKey::from_snap(snap)?
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
    let total_size = s.total_size()?;

    let mut chunks = {
        let mut first_chunk = SnapshotChunk::default();
        first_chunk.set_message(msg);

        SnapChunk {
            first: Some(first_chunk),
            snap: s,
            remain_bytes: total_size as usize,
        }
    };

    let cb = ChannelBuilder::new(env)
        .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
        .keepalive_time(cfg.grpc_keepalive_time.0)
        .keepalive_timeout(cfg.grpc_keepalive_timeout.0)
        .default_compression_algorithm(cfg.grpc_compression_algorithm());

    let channel = security_mgr.connect(cb, addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = client.snapshot()?;

    let send_task = async move {
        let mut sink = sink.sink_map_err(Error::from);
        sink.send_all(&mut chunks).await?;
        sink.close().await?;
        let recv_result = receiver.map_err(Error::from).await;
        send_timer.observe_duration();
        drop(deregister);
        drop(client);
        match recv_result {
            Ok(_) => {
                fail_point!("snapshot_delete_after_send");
                chunks.snap.delete();
                // TODO: improve it after rustc resolves the bug.
                // Call `info` in the closure directly will cause rustc
                // panic with `Cannot create local mono-item for DefId`.
                Ok(SendStat {
                    key,
                    total_size,
                    elapsed: timer.elapsed(),
                })
            }
            Err(e) => Err(e),
        }
    };
    Ok(send_task)
}

struct RecvSnapContext {
    key: SnapKey,
    file: Option<Box<dyn GenericSnapshot>>,
    raft_msg: RaftMessage,
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

        let snap = {
            let data = meta.get_message().get_snapshot().get_data();
            let s = match snap_mgr.get_snapshot_for_receiving(&key, data) {
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
        })
    }

    fn finish<R, E>(self, raft_router: R) -> Result<()>
    where R: RaftStoreRouter<E>,
          E: KvEngine,
    {
        let key = self.key;
        if let Some(mut file) = self.file {
            info!("saving snapshot file"; "snap_key" => %key, "file" => file.path());
            if let Err(e) = file.save() {
                let path = file.path();
                let e = box_err!("{} failed to save snapshot file {}: {:?}", key, path, e);
                return Err(e);
            }
        }
        if let Err(e) = raft_router.send_raft_msg(self.raft_msg) {
            return Err(box_err!("{} failed to send snapshot to raft: {}", key, e));
        }
        Ok(())
    }
}

fn recv_snap<R, E>(
    stream: RequestStream<SnapshotChunk>,
    sink: ClientStreamingSink<Done>,
    snap_mgr: SnapManager,
    raft_router: R,
) -> impl Future<Output = Result<()>>
where R: RaftStoreRouter<E> + 'static,
      E: KvEngine,
{
    let recv_task = async move {
        let mut stream = stream.map_err(Error::from);
        let head = stream.next().await.transpose()?;
        let mut context = RecvSnapContext::new(head, &snap_mgr)?;
        if context.file.is_none() {
            return context.finish(raft_router);
        }
        let context_key = context.key.clone();
        snap_mgr.register(context.key.clone(), SnapEntry::Receiving);

        while let Some(item) = stream.next().await {
            let mut chunk = item?;
            let data = chunk.take_data();
            if data.is_empty() {
                return Err(box_err!("{} receive chunk with empty data", context.key));
            }
            if let Err(e) = context.file.as_mut().unwrap().write_all(&data) {
                let key = &context.key;
                let path = context.file.as_mut().unwrap().path();
                let e = box_err!("{} failed to write snapshot file {}: {}", key, path, e);
                return Err(e);
            }
        }

        let res = context.finish(raft_router);
        snap_mgr.deregister(&context_key, &SnapEntry::Receiving);
        res
    };

    async move {
        match recv_task.await {
            Ok(()) => sink.success(Done::default()).await.map_err(Error::from),
            Err(e) => {
                let status = RpcStatus::new(RpcStatusCode::UNKNOWN, Some(format!("{:?}", e)));
                sink.fail(status).await.map_err(Error::from)
            }
        }
    }
}

pub struct Runner<R: RaftStoreRouter<RocksEngine> + 'static> {
    env: Arc<Environment>,
    snap_mgr: SnapManager,
    pool: Runtime,
    raft_router: R,
    security_mgr: Arc<SecurityManager>,
    cfg: Arc<Config>,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
}

impl<R: RaftStoreRouter<RocksEngine> + 'static> Runner<R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: SnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
        cfg: Arc<Config>,
    ) -> Runner<R> {
        Runner {
            env,
            snap_mgr,
            pool: RuntimeBuilder::new()
                .threaded_scheduler()
                .thread_name(thd_name!("snap-sender"))
                .core_threads(DEFAULT_POOL_SIZE)
                .on_thread_start(tikv_alloc::add_thread_memory_accessor)
                .on_thread_stop(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
            raft_router: r,
            security_mgr,
            cfg,
            sending_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<R: RaftStoreRouter<RocksEngine> + 'static> Runnable for Runner<R> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Recv { stream, sink } => {
                let task_num = self.recving_count.load(Ordering::SeqCst);
                if task_num >= self.cfg.concurrent_recv_snap_limit {
                    warn!("too many recving snapshot tasks, ignore");
                    let status = RpcStatus::new(
                        RpcStatusCode::RESOURCE_EXHAUSTED,
                        Some(format!(
                            "the number of received snapshot tasks {} exceeded the limitation {}",
                            task_num, self.cfg.concurrent_recv_snap_limit
                        )),
                    );
                    self.pool.spawn(sink.fail(status));
                    return;
                }
                SNAP_TASK_COUNTER_STATIC.recv.inc();

                let snap_mgr = self.snap_mgr.clone();
                let raft_router = self.raft_router.clone();
                let recving_count = Arc::clone(&self.recving_count);
                recving_count.fetch_add(1, Ordering::SeqCst);
                let task = async move {
                    let result = recv_snap(stream, sink, snap_mgr, raft_router).await;
                    recving_count.fetch_sub(1, Ordering::SeqCst);
                    if let Err(e) = result {
                        error!("failed to recv snapshot"; "err" => %e);
                    }
                };
                self.pool.spawn(task);
            }
            Task::Send { addr, msg, cb } => {
                fail_point!("send_snapshot");
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

                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let sending_count = Arc::clone(&self.sending_count);
                sending_count.fetch_add(1, Ordering::SeqCst);

                let send_task = send_snap(env, mgr, security_mgr, &self.cfg, &addr, msg);
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
                            error!("failed to send snap"; "to_addr" => addr, "err" => ?e);
                            cb(Err(e));
                        }
                    };
                    sending_count.fetch_sub(1, Ordering::SeqCst);
                };

                self.pool.spawn(task);
            }
        }
    }
}
