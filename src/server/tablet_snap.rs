// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fs::{self, File},
    io::{Read, Write},
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use engine_traits::KvEngine;
use file_system::{IoType, WithIoType};
use futures::{
    future::{Future, TryFutureExt},
    sink::{Sink, SinkExt},
    stream::{StreamExt, TryStreamExt},
};
use grpcio::{
    ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcStatus, RpcStatusCode,
    WriteFlags,
};
use kvproto::{
    raft_serverpb::{Done, RaftMessage, RaftSnapshotData, SnapshotChunk},
    tikvpb::TikvClient,
};
use protobuf::Message;
use raftstore::{
    router::RaftStoreRouter,
    store::snap::{TabletSnapKey, TabletSnapManager},
};
use security::SecurityManager;
use tikv_util::{
    config::{Tracker, VersionTrack},
    time::Instant,
    worker::Runnable,
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use super::{metrics::*, snap::Task, Config, Error, Result};
use crate::tikv_util::sys::thread::ThreadBuildWrapper;

pub type Callback = Box<dyn FnOnce(Result<()>) + Send>;

const DEFAULT_POOL_SIZE: usize = 4;

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

struct RecvTabletSnapContext {
    key: TabletSnapKey,
    raft_msg: RaftMessage,
    io_type: IoType,
    start: Instant,
}

impl RecvTabletSnapContext {
    fn new(head_chunk: Option<SnapshotChunk>) -> Result<Self> {
        // head_chunk is None means the stream is empty.
        let mut head = head_chunk.ok_or_else(|| Error::Other("empty gRPC stream".into()))?;
        if !head.has_message() {
            return Err(box_err!("no raft message in the first chunk"));
        }

        let meta = head.take_message();
        let snapshot = meta.get_message().get_snapshot();
        let key = TabletSnapKey::from_region_snap(
            meta.get_region_id(),
            meta.get_to_peer().get_id(),
            meta.get_message().get_snapshot(),
        );

        let data = snapshot.get_data();
        let mut snapshot_data = RaftSnapshotData::default();
        snapshot_data.merge_from_bytes(data)?;
        let snapshot_meta = snapshot_data.take_meta();
        let io_type = if snapshot_meta.get_for_balance() {
            IoType::LoadBalance
        } else {
            IoType::Replication
        };
        let _with_io_type = WithIoType::new(io_type);

        Ok(RecvTabletSnapContext {
            key,
            raft_msg: meta,
            io_type,
            start: Instant::now(),
        })
    }

    fn finish<R: RaftStoreRouter<impl KvEngine>>(self, raft_router: R) -> Result<()> {
        let _with_io_type = WithIoType::new(self.io_type);
        let key = self.key;
        if let Err(e) = raft_router.send_raft_msg(self.raft_msg) {
            return Err(box_err!("{} failed to send snapshot to raft: {}", key, e));
        }
        info!("saving all snapshot files"; "snap_key" => %key, "takes" => ?self.start.saturating_elapsed());
        Ok(())
    }
}

async fn send_snap_files(
    mgr: &TabletSnapManager,
    mut sender: impl Sink<(SnapshotChunk, WriteFlags), Error = Error> + Unpin,
    msg: RaftMessage,
    key: TabletSnapKey,
) -> Result<u64> {
    let path = mgr.get_tablet_checkpointer_path(&key);
    let files = fs::read_dir(&path)?
        .map(|d| Ok(d?.path()))
        .collect::<Result<Vec<_>>>()?;
    let mut total_sent = msg.compute_size() as u64;
    let mut chunk = SnapshotChunk::default();
    chunk.set_message(msg);
    sender
        .feed((chunk, WriteFlags::default().buffer_hint(true)))
        .await?;
    for path in files {
        let name = path.file_name().unwrap().to_str().unwrap();
        let mut buffer = Vec::with_capacity(SNAP_CHUNK_LEN);
        buffer.push(name.len() as u8);
        buffer.extend_from_slice(name.as_bytes());
        let mut f = File::open(&path)?;
        let file_size = f.metadata()?.len();
        let mut size = 0;
        let mut off = buffer.len();
        loop {
            unsafe { buffer.set_len(SNAP_CHUNK_LEN) }
            let readed = f.read(&mut buffer[off..])?;
            let new_len = readed + off;
            total_sent += new_len as u64;

            unsafe {
                buffer.set_len(new_len);
            }
            let mut chunk = SnapshotChunk::default();
            chunk.set_data(buffer);
            sender
                .feed((chunk, WriteFlags::default().buffer_hint(true)))
                .await?;
            size += readed;
            if new_len < SNAP_CHUNK_LEN {
                break;
            }
            buffer = Vec::with_capacity(SNAP_CHUNK_LEN);
            off = 0
        }
        info!("sent snap file finish"; "file" => %path.display(), "size" => file_size, "sent" => size);
    }
    sender.close().await?;
    Ok(total_sent)
}

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the
/// snapshot file.
pub fn send_snap(
    env: Arc<Environment>,
    mgr: TabletSnapManager,
    security_mgr: Arc<SecurityManager>,
    cfg: &Config,
    addr: &str,
    msg: RaftMessage,
) -> Result<impl Future<Output = Result<SendStat>>> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();
    let send_timer = SEND_SNAP_HISTOGRAM.start_coarse_timer();

    let key = TabletSnapKey::from_region_snap(
        msg.get_region_id(),
        msg.get_to_peer().get_id(),
        msg.get_message().get_snapshot(),
    );

    let cb = ChannelBuilder::new(env)
        .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
        .keepalive_time(cfg.grpc_keepalive_time.0)
        .keepalive_timeout(cfg.grpc_keepalive_timeout.0)
        .default_compression_algorithm(cfg.grpc_compression_algorithm())
        .default_gzip_compression_level(cfg.grpc_gzip_compression_level)
        .default_grpc_min_message_size_to_compress(cfg.grpc_min_message_size_to_compress);

    let channel = security_mgr.connect(cb, addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = client.snapshot()?;
    let send_task = async move {
        let sink = sink.sink_map_err(Error::from);
        let total_size = send_snap_files(&mgr, sink, msg, key.clone()).await?;
        let recv_result = receiver.map_err(Error::from).await;
        send_timer.observe_duration();
        drop(client);
        match recv_result {
            Ok(_) => {
                fail_point!("snapshot_delete_after_send");
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

fn recv_snap<R: RaftStoreRouter<impl KvEngine> + 'static>(
    stream: RequestStream<SnapshotChunk>,
    sink: ClientStreamingSink<Done>,
    snap_mgr: TabletSnapManager,
    raft_router: R,
) -> impl Future<Output = Result<()>> {
    let recv_task = async move {
        let mut stream = stream.map_err(Error::from);
        let head = stream.next().await.transpose()?;
        let context = RecvTabletSnapContext::new(head)?;
        let path = snap_mgr.get_tmp_name_for_recv(&context.key);
        fs::create_dir_all(&path)?;
        loop {
            fail_point!("receiving_snapshot_net_error", |_| {
                Err(box_err!("{} failed to receive snapshot", context.key))
            });
            let mut chunk = match stream.try_next().await? {
                // todo: need to check data
                Some(mut c) if !c.has_message() => c.take_data(),
                Some(_) => return Err(box_err!("duplicated metadata")),
                None => break,
            };
            // the format of chunk:
            // |--name_len--|--name--|--content--|
            let len = chunk[0] as usize;
            let file_name = box_try!(std::str::from_utf8(&chunk[1..len + 1]));
            let p = path.join(file_name);
            let mut f = File::create(&p)?;
            let mut size = chunk.len() - len - 1;
            f.write_all(&chunk[len + 1..])?;
            while chunk.len() >= SNAP_CHUNK_LEN {
                chunk = match stream.try_next().await? {
                    Some(mut c) if !c.has_message() => c.take_data(),
                    Some(_) => return Err(box_err!("duplicated metadata")),
                    None => return Err(box_err!("missing chunk")),
                };
                f.write_all(&chunk[..])?;
                size += chunk.len();
            }
            info!("received snap file"; "file" => %p.display(), "size" => size);
            SNAP_LIMIT_TRANSPORT_BYTES_COUNTER_STATIC
                .recv
                .inc_by(size as u64);
            f.sync_data()?;
        }

        let final_path = snap_mgr.get_final_name_for_recv(&context.key);
        fs::rename(&path, final_path)?;
        context.finish(raft_router)
    };
    async move {
        match recv_task.await {
            Ok(()) => sink.success(Done::default()).await.map_err(Error::from),
            Err(e) => {
                let status = RpcStatus::with_message(RpcStatusCode::UNKNOWN, format!("{:?}", e));
                sink.fail(status).await.map_err(Error::from)
            }
        }
    }
}

pub struct TabletRunner<E, R>
where
    E: KvEngine,
    R: RaftStoreRouter<E> + 'static,
{
    env: Arc<Environment>,
    snap_mgr: TabletSnapManager,
    security_mgr: Arc<SecurityManager>,
    pool: Runtime,
    raft_router: R,
    cfg_tracker: Tracker<Config>,
    cfg: Config,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
    engine: PhantomData<E>,
}

impl<E, R> TabletRunner<E, R>
where
    E: KvEngine,
    R: RaftStoreRouter<E> + 'static,
{
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: TabletSnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
        cfg: Arc<VersionTrack<Config>>,
    ) -> TabletRunner<E, R> {
        let cfg_tracker = cfg.clone().tracker("tablet-snap-sender".to_owned());
        let snap_worker = TabletRunner {
            env,
            snap_mgr,
            pool: RuntimeBuilder::new_multi_thread()
                .thread_name(thd_name!("tablet-snap-sender"))
                .worker_threads(DEFAULT_POOL_SIZE)
                .after_start_wrapper(tikv_alloc::add_thread_memory_accessor)
                .before_stop_wrapper(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
            raft_router: r,
            security_mgr,
            cfg_tracker,
            cfg: cfg.value().clone(),
            sending_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
            engine: PhantomData,
        };
        snap_worker
    }

    fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            // todo: need to check limit
            let max_total_size = if incoming.snap_max_total_size.0 > 0 {
                incoming.snap_max_total_size.0
            } else {
                u64::MAX
            };
            info!("refresh snapshot manager config";
            "max_total_snap_size"=> max_total_size);
            self.cfg = incoming.clone();
        }
    }
}

pub struct SendStat {
    key: TabletSnapKey,
    total_size: u64,
    elapsed: Duration,
}

impl<E, R> Runnable for TabletRunner<E, R>
where
    E: KvEngine,
    R: RaftStoreRouter<E> + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Recv { stream, sink } => {
                let task_num = self.recving_count.load(Ordering::SeqCst);
                if task_num >= self.cfg.concurrent_recv_snap_limit {
                    warn!("too many recving snapshot tasks, ignore");
                    let status = RpcStatus::with_message(
                        RpcStatusCode::RESOURCE_EXHAUSTED,
                        format!(
                            "the number of received snapshot tasks {} exceeded the limitation {}",
                            task_num, self.cfg.concurrent_recv_snap_limit
                        ),
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

                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let sending_count = Arc::clone(&self.sending_count);
                sending_count.fetch_add(1, Ordering::SeqCst);
                let send_task = send_snap(env, mgr, security_mgr, &self.cfg.clone(), &addr, msg);
                let task = async move {
                    let res = match send_task {
                        Err(e) => Err(e),
                        Ok(f) => f.await,
                    };
                    match res {
                        Ok(stat) => {
                            info!(
                                "sent snapshot";
                                "region_id" => region_id,
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
