// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This file contains the implementation of sending and receiving tablet
//! snapshot.
//!
//! Different from v1, tablet snapshot always tries to use cache to speed up
//! transfering speed. The protocol is quite simple:
//!
//! sender                            receiver
//! send snapshot meta    ---->     receive snapshot meta
//! extra snapshot preview          collect cache meta
//! send all preview      ---->     receive preview and clean up miss cache
//! files receive files list      <-----  send missing file list
//! send missing files    ---->     receive missing files
//! close sender          ---->     persist snapshot and report to raftstore
//! wait for receiver       <-----  close sender
//! finish

#[cfg(any(test, feature = "testexport"))]
use std::io;
use std::{
    cmp,
    convert::TryFrom,
    fmt::Debug,
    fs::{self, File},
    io::{BorrowedBuf, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use collections::HashMap;
use crc64fast::Digest;
use engine_traits::{Checkpointer, KvEngine, TabletRegistry};
use file_system::{IoType, OpenOptions, WithIoType};
use futures::{
    future::{Future, FutureExt},
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt, TryStreamExt},
};
use grpcio::{
    self, ChannelBuilder, DuplexSink, Environment, RequestStream, RpcStatus, RpcStatusCode,
    WriteFlags,
};
use kvproto::{
    raft_serverpb::{
        RaftMessage, RaftSnapshotData, TabletSnapshotFileChunk, TabletSnapshotFileMeta,
        TabletSnapshotPreview, TabletSnapshotRequest, TabletSnapshotResponse,
    },
    tikvpb::TikvClient,
};
use protobuf::Message;
use raftstore::store::snap::{ReceivingGuard, TabletSnapKey, TabletSnapManager};
use security::SecurityManager;
use tikv_kv::RaftExtension;
use tikv_util::{
    config::{ReadableSize, Tracker, VersionTrack},
    time::Instant,
    worker::Runnable,
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use super::{
    metrics::*,
    snap::{Task, DEFAULT_POOL_SIZE},
    Config, Error, Result,
};
use crate::tikv_util::{sys::thread::ThreadBuildWrapper, time::Limiter};

const PREVIEW_CHUNK_LEN: usize = ReadableSize::kb(1).0 as usize;
const PREVIEW_BATCH_SIZE: usize = 256;
const FILE_CHUNK_LEN: usize = ReadableSize::mb(1).0 as usize;
const USE_CACHE_THRESHOLD: u64 = ReadableSize::mb(4).0;

fn is_sst(file_name: &str) -> bool {
    file_name.ends_with(".sst")
}

async fn read_to(f: &mut File, to: &mut Vec<u8>, size: usize, limiter: &Limiter) -> Result<()> {
    // It's likely in page cache already.
    limiter.consume(size / 2).await;
    to.clear();
    to.reserve_exact(size);
    let mut buf: BorrowedBuf<'_> = to.spare_capacity_mut().into();
    f.read_buf_exact(buf.unfilled())?;
    unsafe {
        to.set_len(size);
    }
    Ok(())
}

pub trait SnapCacheBuilder: Send + Sync {
    fn build(&self, region_id: u64, path: &Path) -> Result<()>;
}

impl<EK: KvEngine> SnapCacheBuilder for TabletRegistry<EK> {
    fn build(&self, region_id: u64, path: &Path) -> Result<()> {
        if let Some(mut c) = self.get(region_id) && let Some(db) = c.latest() {
            let mut checkpointer = db.new_checkpointer()?;
            // Avoid flush.
            checkpointer.create_at(path, None, u64::MAX)?;
            Ok(())
        } else {
            Err(Error::Other(format!("region {} not found", region_id).into()))
        }
    }
}

#[derive(Clone)]
pub struct NoSnapshotCache;

impl SnapCacheBuilder for NoSnapshotCache {
    fn build(&self, _: u64, _: &Path) -> Result<()> {
        Err(Error::Other("cache is disabled".into()))
    }
}

struct RecvTabletSnapContext<'a> {
    key: TabletSnapKey,
    raft_msg: RaftMessage,
    use_cache: bool,
    io_type: IoType,
    // Lock to avoid receive the same snapshot concurrently.
    _receiving_guard: ReceivingGuard<'a>,
    start: Instant,
}

impl<'a> RecvTabletSnapContext<'a> {
    fn new(mut head: TabletSnapshotRequest, mgr: &'a TabletSnapManager) -> Result<Self> {
        if !head.has_head() {
            return Err(box_err!("no raft message in the first chunk"));
        }
        let mut head = head.take_head();
        let meta = head.take_message();
        let key = TabletSnapKey::from_region_snap(
            meta.get_region_id(),
            meta.get_to_peer().get_id(),
            meta.get_message().get_snapshot(),
        );
        let io_type = io_type_from_raft_message(&meta)?;
        let receiving_guard = match mgr.start_receive(key.clone()) {
            Some(g) => g,
            None => return Err(box_err!("failed to start receive snapshot")),
        };

        Ok(RecvTabletSnapContext {
            key,
            raft_msg: meta,
            use_cache: head.use_cache,
            io_type,
            _receiving_guard: receiving_guard,
            start: Instant::now(),
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

fn protocol_error(exp: &str, act: impl Debug) -> Error {
    Error::Other(format!("protocol error: expect {exp}, but got {act:?}").into())
}

/// Check if a local SST file matches the preview meta.
///
/// It's considered matched when:
/// 1. Have the same file size;
/// 2. The first `PREVIEW_CHUNK_LEN` bytes are the same, this contains the
/// actual data of an SST;
/// 3. The last `PREVIEW_CHUNK_LEN` bytes are the same, this contains checksum,
/// properties and other medata of an SST.
async fn is_sst_match_preview(
    preview_meta: &TabletSnapshotFileMeta,
    target: &Path,
    buffer: &mut Vec<u8>,
    limiter: &Limiter,
) -> Result<bool> {
    let mut f = File::open(target)?;
    let exist_len = f.metadata()?.len();
    if exist_len != preview_meta.file_size {
        return Ok(false);
    }

    let head_len = preview_meta.head_chunk.len();
    let trailing_len = preview_meta.trailing_chunk.len();
    if head_len as u64 > preview_meta.file_size || trailing_len as u64 > preview_meta.file_size {
        return Err(Error::Other(
            format!(
                "invalid chunk length {} {} {}",
                preview_meta.file_size, head_len, trailing_len
            )
            .into(),
        ));
    }
    read_to(&mut f, buffer, head_len, limiter).await?;
    if *buffer != preview_meta.head_chunk {
        return Ok(false);
    }

    if preview_meta.trailing_chunk.is_empty() {
        // A safet check to detect wrong protocol implementation. Only head chunk
        // contains all the data can trailing chunk be empty.
        return Ok(head_len as u64 == preview_meta.file_size);
    }

    f.seek(SeekFrom::End(-(trailing_len as i64)))?;
    read_to(&mut f, buffer, trailing_len, limiter).await?;
    Ok(*buffer == preview_meta.trailing_chunk)
}

async fn cleanup_cache(
    path: &Path,
    stream: &mut (impl Stream<Item = Result<TabletSnapshotRequest>> + Unpin),
    sink: &mut (impl Sink<(TabletSnapshotResponse, WriteFlags), Error = grpcio::Error> + Unpin),
    limiter: &Limiter,
) -> Result<(u64, Vec<String>)> {
    let mut reused = 0;
    let mut exists = HashMap::default();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let ft = entry.file_type()?;
        if ft.is_dir() {
            fs::remove_dir_all(entry.path())?;
            continue;
        }
        if ft.is_file() {
            let os_name = entry.file_name();
            let name = os_name.to_str().unwrap();
            if is_sst(name) {
                // Collect length requires another IO, delay till we are sure
                // it's probably be reused.
                exists.insert(name.to_string(), entry.path());
                continue;
            }
        }
        fs::remove_file(entry.path())?;
    }
    let mut missing = vec![];
    loop {
        let mut preview = match stream.next().await {
            Some(Ok(mut req)) if req.has_preview() => req.take_preview(),
            res => return Err(protocol_error("preview", res)),
        };
        let mut buffer = Vec::with_capacity(PREVIEW_CHUNK_LEN);
        for meta in preview.take_metas().into_vec() {
            if is_sst(&meta.file_name) && let Some(p) = exists.remove(&meta.file_name) {
                if is_sst_match_preview(&meta, &p, &mut buffer, limiter).await? {
                    reused += meta.file_size;
                    continue;
                }
                // We should not write to the file directly as it's hard linked.
                fs::remove_file(p)?;
            }
            missing.push(meta.file_name);
        }
        if preview.end {
            break;
        }
    }
    for (_, p) in exists {
        fs::remove_file(p)?;
    }
    let mut resp = TabletSnapshotResponse::default();
    resp.mut_files().set_file_name(missing.clone().into());
    sink.send((resp, WriteFlags::default())).await?;
    Ok((reused, missing))
}

async fn accept_one_file(
    path: &Path,
    mut chunk: TabletSnapshotFileChunk,
    stream: &mut (impl Stream<Item = Result<TabletSnapshotRequest>> + Unpin),
    limiter: &Limiter,
    digest: &mut Digest,
) -> Result<u64> {
    let name = chunk.file_name;
    digest.write(name.as_bytes());
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.join(&name))?;
    let exp_size = chunk.file_size;
    let mut file_size = 0;
    loop {
        let chunk_len = chunk.data.len();
        file_size += chunk_len as u64;
        if file_size > exp_size {
            return Err(Error::Other(
                format!("file {} too long {} {}", name, file_size, exp_size).into(),
            ));
        }
        limiter.consume(chunk_len).await;
        digest.write(&chunk.data);
        f.write_all(&chunk.data)?;
        if exp_size == file_size {
            f.sync_data()?;
            return Ok(exp_size);
        }
        chunk = match stream.next().await {
            Some(Ok(mut req)) if req.has_chunk() => req.take_chunk(),
            res => return Err(protocol_error("chunk", res)),
        };
        if !chunk.file_name.is_empty() {
            return Err(protocol_error(&name, &chunk.file_name));
        }
    }
}

async fn accept_missing(
    path: &Path,
    missing_ssts: Vec<String>,
    stream: &mut (impl Stream<Item = Result<TabletSnapshotRequest>> + Unpin),
    limiter: &Limiter,
) -> Result<u64> {
    let mut digest = Digest::default();
    let mut received_bytes: u64 = 0;
    for name in missing_ssts {
        let chunk = match stream.next().await {
            Some(Ok(mut req)) if req.has_chunk() => req.take_chunk(),
            res => return Err(protocol_error("chunk", res)),
        };
        if chunk.file_name != name {
            return Err(protocol_error(&name, &chunk.file_name));
        }
        received_bytes += accept_one_file(path, chunk, stream, limiter, &mut digest).await?;
    }
    // Now receive other files.
    loop {
        let chunk = match stream.next().await {
            Some(Ok(mut req)) if req.has_chunk() => req.take_chunk(),
            Some(Ok(req)) if req.has_end() => {
                let checksum = req.get_end().get_checksum();
                if checksum != digest.sum64() {
                    return Err(Error::Other(
                        format!("checksum mismatch {} {}", checksum, digest.sum64()).into(),
                    ));
                }
                File::open(path)?.sync_data()?;
                let res = stream.next().await;
                return if res.is_none() {
                    Ok(received_bytes)
                } else {
                    Err(protocol_error("None", res))
                };
            }
            res => return Err(protocol_error("chunk", res)),
        };
        if chunk.file_name.is_empty() {
            return Err(protocol_error("file_name", &chunk.file_name));
        }
        received_bytes += accept_one_file(path, chunk, stream, limiter, &mut digest).await?;
    }
}

async fn recv_snap_files<'a>(
    snap_mgr: &'a TabletSnapManager,
    cache_builder: impl SnapCacheBuilder,
    mut stream: impl Stream<Item = Result<TabletSnapshotRequest>> + Unpin,
    sink: &mut (impl Sink<(TabletSnapshotResponse, WriteFlags), Error = grpcio::Error> + Unpin),
    limiter: Limiter,
) -> Result<RecvTabletSnapContext<'a>> {
    let head = stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| Error::Other("empty gRPC stream".into()))?;
    let context = RecvTabletSnapContext::new(head, snap_mgr)?;
    let _with_io_type = WithIoType::new(context.io_type);
    let region_id = context.key.region_id;
    let final_path = snap_mgr.final_recv_path(&context.key);
    if final_path.exists() {
        // The snapshot is received already, should wait for peer to apply. If the
        // snapshot is corrupted, the peer should destroy it first then request again.
        return Err(Error::Other(
            format!("snapshot {} already exists", final_path.display()).into(),
        ));
    }
    let path = snap_mgr.tmp_recv_path(&context.key);
    info!("begin to receive tablet snapshot files"; "file" => %path.display(), "region_id" => region_id);
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    let (reused, missing_ssts) = if context.use_cache {
        if let Err(e) = cache_builder.build(region_id, &path) {
            info!("not using cache"; "region_id" => region_id, "err" => ?e);
            fs::create_dir_all(&path)?;
        }
        cleanup_cache(&path, &mut stream, sink, &limiter).await?
    } else {
        info!("not using cache"; "region_id" => region_id);
        fs::create_dir_all(&path)?;
        (0, vec![])
    };
    let received = accept_missing(&path, missing_ssts, &mut stream, &limiter).await?;
    info!("received all tablet snapshot file"; "snap_key" => %context.key, "region_id" => region_id, "received" => received, "reused" => reused);
    let final_path = snap_mgr.final_recv_path(&context.key);
    fs::rename(&path, final_path)?;
    Ok(context)
}

async fn recv_snap<R: RaftExtension + 'static>(
    stream: RequestStream<TabletSnapshotRequest>,
    sink: DuplexSink<TabletSnapshotResponse>,
    snap_mgr: TabletSnapManager,
    raft_router: R,
    cache_builder: impl SnapCacheBuilder,
    limiter: Limiter,
) -> Result<()> {
    let stream = stream.map_err(Error::from);
    let mut sink = sink;
    let res = recv_snap_files(&snap_mgr, cache_builder, stream, &mut sink, limiter)
        .await
        .and_then(|context| context.finish(raft_router));
    match res {
        Ok(()) => sink.close().await.map_err(Error::from),
        Err(e) => {
            let status = RpcStatus::with_message(RpcStatusCode::UNKNOWN, format!("{:?}", e));
            sink.fail(status).await.map_err(Error::from)
        }
    }
}

async fn build_one_preview(
    path: &Path,
    iter: &mut impl Iterator<Item = (&String, &u64)>,
    limiter: &Limiter,
) -> Result<TabletSnapshotRequest> {
    let mut preview = TabletSnapshotPreview::default();
    for _ in 0..PREVIEW_BATCH_SIZE {
        let (name, size) = match iter.next() {
            Some((name, size)) => (name, *size),
            None => break,
        };
        let mut meta = TabletSnapshotFileMeta::default();
        meta.file_name = name.clone();
        meta.file_size = size;
        let mut f = File::open(path.join(name))?;
        let to_read = cmp::min(size as usize, PREVIEW_CHUNK_LEN);
        read_to(&mut f, &mut meta.head_chunk, to_read, limiter).await?;
        if size > PREVIEW_CHUNK_LEN as u64 {
            f.seek(SeekFrom::End(-(to_read as i64)))?;
            read_to(&mut f, &mut meta.trailing_chunk, to_read, limiter).await?;
        }
        preview.mut_metas().push(meta);
    }
    let mut req = TabletSnapshotRequest::default();
    req.set_preview(preview);
    Ok(req)
}

async fn find_missing(
    path: &Path,
    mut head: TabletSnapshotRequest,
    sender: &mut (impl Sink<(TabletSnapshotRequest, WriteFlags), Error = Error> + Unpin),
    receiver: &mut (impl Stream<Item = grpcio::Result<TabletSnapshotResponse>> + Unpin),
    limiter: &Limiter,
) -> Result<Vec<(String, u64)>> {
    let mut sst_sizes = 0;
    let mut ssts = HashMap::default();
    let mut other_files = vec![];
    for f in fs::read_dir(path)? {
        let entry = f?;
        let ft = entry.file_type()?;
        // What if it's titan?
        if !ft.is_file() {
            continue;
        }
        let os_name = entry.file_name();
        let name = os_name.to_str().unwrap().to_string();
        let file_size = entry.metadata()?.len();
        if is_sst(&name) {
            sst_sizes += file_size;
            ssts.insert(name, file_size);
        } else {
            other_files.push((name, file_size));
        }
    }
    if sst_sizes < USE_CACHE_THRESHOLD {
        sender
            .send((head, WriteFlags::default().buffer_hint(true)))
            .await?;
        other_files.extend(ssts);
        return Ok(other_files);
    }

    head.mut_head().set_use_cache(true);
    // Send immediately to make receiver collect cache earlier.
    sender.send((head, WriteFlags::default())).await?;
    let sst_count = ssts.len();
    // PREVIEW_BATCH_SIZE -> 1, PREVIEW_BATCH_SIZE + 1 = 2. sst_count can't be 0.
    let batch_count = (sst_count - 1) / PREVIEW_BATCH_SIZE + 1;
    let mut ssts_iter = ssts.iter();
    for _ in 0..batch_count {
        let req = build_one_preview(path, &mut ssts_iter, limiter).await?;
        sender
            .send((req, WriteFlags::default().buffer_hint(true)))
            .await?;
    }
    let mut req = build_one_preview(path, &mut ssts_iter, limiter).await?;
    req.mut_preview().end = true;
    sender.send((req, WriteFlags::default())).await?;

    let accepted = match receiver.next().await {
        Some(Ok(mut req)) if req.has_files() => req.take_files().take_file_name(),
        res => return Err(protocol_error("missing files", res)),
    };
    let mut missing = Vec::with_capacity(accepted.len());
    for name in &accepted {
        let s = match ssts.remove_entry(name) {
            Some(s) => s,
            None => return Err(Error::Other(format!("missing file {}", name).into())),
        };
        missing.push(s);
    }
    missing.extend(other_files);
    Ok(missing)
}

async fn send_missing(
    path: &Path,
    missing: Vec<(String, u64)>,
    sender: &mut (impl Sink<(TabletSnapshotRequest, WriteFlags), Error = Error> + Unpin),
    limiter: &Limiter,
) -> Result<(u64, u64)> {
    let mut total_sent = 0;
    let mut digest = Digest::default();
    for (name, mut file_size) in missing {
        let mut chunk = TabletSnapshotFileChunk::default();
        chunk.file_name = name;
        digest.write(chunk.file_name.as_bytes());
        chunk.file_size = file_size;
        total_sent += file_size;
        if file_size == 0 {
            let mut req = TabletSnapshotRequest::default();
            req.set_chunk(chunk);
            sender
                .send((req, WriteFlags::default().buffer_hint(true)))
                .await?;
            continue;
        }

        let mut f = File::open(path.join(&chunk.file_name))?;
        loop {
            let to_read = cmp::min(FILE_CHUNK_LEN as u64, file_size) as usize;
            read_to(&mut f, &mut chunk.data, to_read, limiter).await?;
            digest.write(&chunk.data);
            let mut req = TabletSnapshotRequest::default();
            req.set_chunk(chunk);
            sender
                .send((req, WriteFlags::default().buffer_hint(true)))
                .await?;
            if file_size == to_read as u64 {
                break;
            }
            chunk = TabletSnapshotFileChunk::default();
            file_size -= to_read as u64;
        }
    }
    Ok((total_sent, digest.sum64()))
}

async fn send_snap_files(
    mgr: &TabletSnapManager,
    mut sender: impl Sink<(TabletSnapshotRequest, WriteFlags), Error = Error> + Unpin,
    receiver: &mut (impl Stream<Item = grpcio::Result<TabletSnapshotResponse>> + Unpin),
    msg: RaftMessage,
    key: TabletSnapKey,
    limiter: Limiter,
) -> Result<u64> {
    let region_id = key.region_id;
    let to_peer = key.to_peer;
    let path = mgr.tablet_gen_path(&key);
    info!("begin to send snapshot file"; "snap_key" => %key, "region_id" => region_id, "to_peer" => to_peer);
    let io_type = io_type_from_raft_message(&msg)?;
    let _with_io_type = WithIoType::new(io_type);
    let mut head = TabletSnapshotRequest::default();
    head.mut_head().set_message(msg);
    let missing = find_missing(&path, head, &mut sender, receiver, &limiter).await?;
    let (total_sent, checksum) = send_missing(&path, missing, &mut sender, &limiter).await?;
    // In gRPC, stream in serverside can finish without error (when the connection
    // is closed). So we need to use an explicit `Done` to indicate all messages
    // are sent. In V1, we have checksum and meta list, so this is not a
    // problem.
    let mut req = TabletSnapshotRequest::default();
    req.mut_end().set_checksum(checksum);
    sender.send((req, WriteFlags::default())).await?;
    SNAP_LIMIT_TRANSPORT_BYTES_COUNTER_STATIC
        .send
        .inc_by(total_sent);
    info!("sent all snap file finish"; "snap_key" => %key, "region_id" => region_id, "to_peer" => to_peer);
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
    let (sink, mut receiver) = client.tablet_snapshot()?;
    let send_task = async move {
        let sink = sink.sink_map_err(Error::from);
        let total_size =
            send_snap_files(&mgr, sink, &mut receiver, msg, key.clone(), limiter).await?;
        let recv_result = receiver.next().await;
        send_timer.observe_duration();
        drop(client);
        mgr.delete_snapshot(&key);
        match recv_result {
            None => Ok(SendStat {
                key,
                total_size,
                elapsed: timer.saturating_elapsed(),
            }),
            Some(Err(e)) => Err(e.into()),
            Some(Ok(resp)) => Err(Error::Other(
                format!("receive unexpected response {:?}", resp).into(),
            )),
        }
    };
    Ok(send_task)
}

pub struct TabletRunner<B, R: RaftExtension + 'static> {
    env: Arc<Environment>,
    snap_mgr: TabletSnapManager,
    security_mgr: Arc<SecurityManager>,
    pool: Runtime,
    raft_router: R,
    cfg_tracker: Tracker<Config>,
    cfg: Config,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
    cache_builder: B,
    limiter: Limiter,
}

impl<B, R: RaftExtension> TabletRunner<B, R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: TabletSnapManager,
        cache_builder: B,
        r: R,
        security_mgr: Arc<SecurityManager>,
        cfg: Arc<VersionTrack<Config>>,
    ) -> Self {
        let config = cfg.value().clone();
        let cfg_tracker = cfg.tracker("tablet-sender".to_owned());
        let limit = i64::try_from(config.snap_io_max_bytes_per_sec.0)
            .unwrap_or_else(|_| panic!("snap_io_max_bytes_per_sec > i64::max_value"));
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
            cache_builder,
            limiter,
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

impl<B, R> Runnable for TabletRunner<B, R>
where
    B: SnapCacheBuilder + Clone + 'static,
    R: RaftExtension,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Recv { sink, .. } => {
                let status = RpcStatus::with_message(
                    RpcStatusCode::UNIMPLEMENTED,
                    "tablet snap is not supported".to_string(),
                );
                self.pool.spawn(sink.fail(status).map(|_| ()));
            }
            Task::RecvTablet { stream, sink } => {
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
                let limiter = self.limiter.clone();
                let cache_builder = self.cache_builder.clone();
                let task = async move {
                    let result =
                        recv_snap(stream, sink, snap_mgr, raft_router, cache_builder, limiter)
                            .await;
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
                let limiter = self.limiter.clone();
                let send_task = send_snap(
                    env,
                    mgr,
                    security_mgr,
                    &self.cfg.clone(),
                    &addr,
                    msg,
                    limiter,
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

// A helper function to copy snapshot.
#[cfg(any(test, feature = "testexport"))]
pub fn copy_tablet_snapshot(
    key: TabletSnapKey,
    msg: RaftMessage,
    sender_snap_mgr: &TabletSnapManager,
    recver_snap_mgr: &TabletSnapManager,
) -> Result<()> {
    let sender_path = sender_snap_mgr.tablet_gen_path(&key);
    let files = fs::read_dir(sender_path)?
        .map(|f| Ok(f?.path()))
        .filter(|f| f.is_ok() && f.as_ref().unwrap().is_file())
        .collect::<Result<Vec<_>>>()?;

    let mut head = TabletSnapshotRequest::default();
    head.mut_head().set_message(msg);

    let recv_context = RecvTabletSnapContext::new(head, recver_snap_mgr)?;
    let recv_path = recver_snap_mgr.tmp_recv_path(&recv_context.key);
    fs::create_dir_all(&recv_path)?;

    for path in files {
        let sender_name = path.file_name().unwrap().to_str().unwrap();
        let mut sender_f = File::open(&path)?;

        let recv_p = recv_path.join(sender_name);
        let mut recv_f = File::create(recv_p)?;

        while io::copy(&mut sender_f, &mut recv_f)? != 0 {}
    }

    let final_path = recver_snap_mgr.final_recv_path(&recv_context.key);
    fs::rename(&recv_path, final_path)?;
    Ok(())
}
