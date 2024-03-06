// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::{TryFrom, TryInto},
    fs::{self, File},
    io::{Read, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use file_system::{IoType, WithIoType};
use futures::{
    future::{Future, TryFutureExt},
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt, TryStreamExt},
};
use grpcio::{
    self, ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcStatus,
    RpcStatusCode, WriteFlags,
};
use kvproto::{
    raft_serverpb::{Done, RaftMessage, RaftSnapshotData, SnapshotChunk},
    tikvpb::TikvClient,
};
use protobuf::Message;
use raftstore::store::snap::{TabletSnapKey, TabletSnapManager};
use security::SecurityManager;
use tikv_kv::RaftExtension;
use tikv_util::{
    config::{Tracker, VersionTrack},
    time::Instant,
    worker::Runnable,
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use super::{
    metrics::*,
    snap::{Task, DEFAULT_POOL_SIZE, SNAP_CHUNK_LEN},
    Config, Error, Result,
};
use crate::tikv_util::{sys::thread::ThreadBuildWrapper, time::Limiter};

struct RecvTabletSnapContext {
    key: TabletSnapKey,
    raft_msg: RaftMessage,
    io_type: IoType,
    start: Instant,
    chunk_size: usize,
}

impl RecvTabletSnapContext {
    fn new(mut head: SnapshotChunk) -> Result<Self> {
        if !head.has_message() {
            return Err(box_err!("no raft message in the first chunk"));
        }

        let chunk_size = match head.take_data().try_into() {
            Ok(buff) => usize::from_ne_bytes(buff),
            Err(_) => return Err(box_err!("failed to get chunk size")),
        };
        let meta = head.take_message();
        let key = TabletSnapKey::from_region_snap(
            meta.get_region_id(),
            meta.get_to_peer().get_id(),
            meta.get_message().get_snapshot(),
        );
        let io_type = io_type_from_raft_message(&meta)?;

        Ok(RecvTabletSnapContext {
            key,
            raft_msg: meta,
            io_type,
            start: Instant::now(),
            chunk_size,
        })
    }

    fn finish<R: RaftExtension>(self, raft_router: R) -> Result<()> {
        let key = self.key;
        raft_router.feed(self.raft_msg, true);
        info!("saving all snapshot files"; "snap_key" => %key, "takes" => ?self.start.saturating_elapsed());
        Ok(())
    }
}

fn io_type_from_raft_message(msg: &RaftMessage) -> Result<IoType> {
    let snapshot = msg.get_message().get_snapshot();
    let data = snapshot.get_data();
    let mut snapshot_data = RaftSnapshotData::default();
    snapshot_data.merge_from_bytes(data)?;
    let snapshot_meta = snapshot_data.get_meta();
    if snapshot_meta.get_for_balance() {
        Ok(IoType::LoadBalance)
    } else {
        Ok(IoType::Replication)
    }
}

async fn send_snap_files(
    mgr: &TabletSnapManager,
    mut sender: impl Sink<(SnapshotChunk, WriteFlags), Error = Error> + Unpin,
    msg: RaftMessage,
    key: TabletSnapKey,
    limiter: Limiter,
) -> Result<u64> {
    let path = mgr.tablet_gen_path(&key);
    info!("begin to send snapshot file";"snap_key" => %key);
    let files = fs::read_dir(&path)?
        .map(|f| Ok(f?.path()))
        .filter(|f| f.is_ok() && f.as_ref().unwrap().is_file())
        .collect::<Result<Vec<_>>>()?;
    let io_type = io_type_from_raft_message(&msg)?;
    let _with_io_type = WithIoType::new(io_type);
    let mut total_sent = msg.compute_size() as u64;
    let mut chunk = SnapshotChunk::default();
    chunk.set_message(msg);
    chunk.set_data(usize::to_ne_bytes(SNAP_CHUNK_LEN).to_vec());
    sender
        .feed((chunk, WriteFlags::default().buffer_hint(true)))
        .await?;
    for path in files {
        let name = path.file_name().unwrap().to_str().unwrap();
        let mut buffer = Vec::with_capacity(SNAP_CHUNK_LEN);
        buffer.push(name.len() as u8);
        buffer.extend_from_slice(name.as_bytes());
        let mut f = File::open(&path)?;
        let mut off = buffer.len();
        loop {
            unsafe {
                buffer.set_len(SNAP_CHUNK_LEN);
            }
            // it should break if readed len is zero or the buffer is full.
            while off < SNAP_CHUNK_LEN {
                let readed = f.read(&mut buffer[off..])?;
                if readed == 0 {
                    unsafe {
                        buffer.set_len(off);
                    }
                    break;
                }
                off += readed;
            }
            limiter.consume(off);
            total_sent += off as u64;
            let mut chunk = SnapshotChunk::default();
            chunk.set_data(buffer);
            sender
                .feed((chunk, WriteFlags::default().buffer_hint(true)))
                .await?;
            // It should switch the next file if the read buffer len is less than the
            // SNAP_CHUNK_LEN.
            if off < SNAP_CHUNK_LEN {
                break;
            }
            buffer = Vec::with_capacity(SNAP_CHUNK_LEN);
            off = 0
        }
    }
    info!("sent all snap file finish"; "snap_key" => %key);
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
    limiter: Limiter,
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
        let total_size = send_snap_files(&mgr, sink, msg, key.clone(), limiter).await?;
        let recv_result = receiver.map_err(Error::from).await;
        send_timer.observe_duration();
        drop(client);
        match recv_result {
            Ok(_) => {
                mgr.delete_snapshot(&key);
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

async fn recv_snap_files(
    snap_mgr: TabletSnapManager,
    mut stream: impl Stream<Item = Result<SnapshotChunk>> + Unpin,
    limit: Limiter,
) -> Result<RecvTabletSnapContext> {
    let head = stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| Error::Other("empty gRPC stream".into()))?;
    let context = RecvTabletSnapContext::new(head)?;
    let chunk_size = context.chunk_size;
    let path = snap_mgr.tmp_recv_path(&context.key);
    info!("begin to receive tablet snapshot files"; "file" => %path.display());
    fs::create_dir_all(&path)?;
    let _with_io_type = WithIoType::new(context.io_type);
    loop {
        let mut chunk = match stream.next().await {
            Some(Ok(mut c)) if !c.has_message() => c.take_data(),
            Some(_) => {
                return Err(box_err!("duplicated metadata"));
            }
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
        // It should switch next file if the chunk size is less than the SNAP_CHUNK_LEN.
        while chunk.len() >= chunk_size {
            chunk = match stream.next().await {
                Some(Ok(mut c)) if !c.has_message() => c.take_data(),
                Some(_) => return Err(box_err!("duplicated metadata")),
                None => return Err(box_err!("missing chunk")),
            };
            f.write_all(&chunk[..])?;
            limit.consume(chunk.len());
            size += chunk.len();
        }
        debug!("received snap file"; "file" => %p.display(), "size" => size);
        SNAP_LIMIT_TRANSPORT_BYTES_COUNTER_STATIC
            .recv
            .inc_by(size as u64);
        f.sync_data()?;
    }
    info!("received all tablet snapshot file"; "snap_key" => %context.key);
    let final_path = snap_mgr.final_recv_path(&context.key);
    fs::rename(&path, final_path)?;
    Ok(context)
}

fn recv_snap<R: RaftExtension + 'static>(
    stream: RequestStream<SnapshotChunk>,
    sink: ClientStreamingSink<Done>,
    snap_mgr: TabletSnapManager,
    raft_router: R,
    limit: Limiter,
) -> impl Future<Output = Result<()>> {
    let recv_task = async move {
        let stream = stream.map_err(Error::from);
        let context = recv_snap_files(snap_mgr, stream, limit).await?;
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

pub struct TabletRunner<R: RaftExtension + 'static> {
    env: Arc<Environment>,
    snap_mgr: TabletSnapManager,
    security_mgr: Arc<SecurityManager>,
    pool: Runtime,
    raft_router: R,
    cfg_tracker: Tracker<Config>,
    cfg: Config,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
    limiter: Limiter,
}

impl<R: RaftExtension> TabletRunner<R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: TabletSnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
        cfg: Arc<VersionTrack<Config>>,
    ) -> Self {
        let config = cfg.value().clone();
        let cfg_tracker = cfg.tracker("tablet-sender".to_owned());
        let limit = i64::try_from(config.snap_max_write_bytes_per_sec.0)
            .unwrap_or_else(|_| panic!("snap_max_write_bytes_per_sec > i64::max_value"));
        let limiter = Limiter::new(if limit > 0 {
            limit as f64
        } else {
            f64::INFINITY
        });

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
            cfg: config,
            sending_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
            limiter,
        };
        snap_worker
    }

    fn refresh_cfg(&mut self) {
        if let Some(incoming) = self.cfg_tracker.any_new() {
            let limit = if incoming.snap_max_write_bytes_per_sec.0 > 0 {
                incoming.snap_max_write_bytes_per_sec.0 as f64
            } else {
                f64::INFINITY
            };
            self.limiter.set_speed_limit(limit);
            info!("refresh snapshot manager config";
            "speed_limit"=> limit);
            self.cfg = incoming.clone();
        }
    }
}

pub struct SendStat {
    key: TabletSnapKey,
    total_size: u64,
    elapsed: Duration,
}

impl<R: RaftExtension> Runnable for TabletRunner<R> {
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
                let recving_count = self.recving_count.clone();
                recving_count.fetch_add(1, Ordering::SeqCst);
                let limit = self.limiter.clone();
                let task = async move {
                    let result = recv_snap(stream, sink, snap_mgr, raft_router, limit).await;
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
                let limit = self.limiter.clone();
                let send_task =
                    send_snap(env, mgr, security_mgr, &self.cfg.clone(), &addr, msg, limit);
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

#[cfg(test)]
mod tests {
    use std::{
        fs::{create_dir_all, File},
        io::Write,
    };

    use futures::{
        channel::mpsc::{self},
        executor::block_on,
        sink::SinkExt,
    };
    use futures_util::StreamExt;
    use grpcio::WriteFlags;
    use kvproto::raft_serverpb::{RaftMessage, SnapshotChunk};
    use raftstore::store::snap::{TabletSnapKey, TabletSnapManager};
    use tempfile::TempDir;
    use tikv_util::{store::new_peer, time::Limiter};

    use super::{super::Error, recv_snap_files, send_snap_files, SNAP_CHUNK_LEN};

    #[test]
    fn test_send_tablet() {
        let limiter = Limiter::new(f64::INFINITY);
        let snap_key = TabletSnapKey::new(1, 1, 1, 1);
        let mut msg = RaftMessage::default();
        msg.set_region_id(1);
        msg.set_to_peer(new_peer(1, 1));
        msg.mut_message().mut_snapshot().mut_metadata().set_index(1);
        msg.mut_message().mut_snapshot().mut_metadata().set_term(1);
        let send_path = TempDir::new().unwrap();
        let send_snap_mgr =
            TabletSnapManager::new(send_path.path().join("snap_dir").to_str().unwrap()).unwrap();
        let snap_path = send_snap_mgr.tablet_gen_path(&snap_key);
        create_dir_all(snap_path.as_path()).unwrap();
        // send file should skip directory
        create_dir_all(snap_path.join("dir")).unwrap();
        for i in 0..2 {
            let mut f = File::create(snap_path.join(i.to_string())).unwrap();
            let count = SNAP_CHUNK_LEN - 2;
            let mut data = std::iter::repeat("a".as_bytes())
                .take(count)
                .collect::<Vec<_>>();
            for buffer in data.iter_mut() {
                f.write_all(buffer).unwrap();
            }
            f.sync_data().unwrap();
        }

        let recv_path = TempDir::new().unwrap();
        let recv_snap_manager =
            TabletSnapManager::new(recv_path.path().join("snap_dir").to_str().unwrap()).unwrap();
        let (tx, rx) = mpsc::unbounded();
        let sink = tx.sink_map_err(Error::from);
        block_on(send_snap_files(
            &send_snap_mgr,
            sink,
            msg,
            snap_key.clone(),
            limiter.clone(),
        ))
        .unwrap();

        let stream = rx.map(|x: (SnapshotChunk, WriteFlags)| Ok(x.0));
        let final_path = recv_snap_manager.final_recv_path(&snap_key);
        let r = block_on(recv_snap_files(recv_snap_manager, stream, limiter)).unwrap();
        assert_eq!(r.key, snap_key);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let dir = std::fs::read_dir(final_path).unwrap();
        assert_eq!(2, dir.count());
        send_snap_mgr.delete_snapshot(&snap_key);
        assert!(!snap_path.exists());
    }
}
