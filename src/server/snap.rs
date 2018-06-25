// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::boxed::FnBox;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{future, Async, Future, Poll, Stream};
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};
use grpc::{
    ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcStatus, RpcStatusCode,
    WriteFlags,
};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_serverpb::{Done, SnapshotChunk};
use kvproto::tikvpb_grpc::TikvClient;

use raftstore::store::{SnapEntry, SnapKey, SnapManager, Snapshot};
use util::security::SecurityManager;
use util::worker::Runnable;
use util::DeferContext;

use super::metrics::*;
use super::transport::RaftStoreRouter;
use super::{Config, Error, Result};

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_POOL_SIZE: usize = 4;

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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
    snap: Box<Snapshot>,
    remain_bytes: usize,
}

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

impl Stream for SnapChunk {
    type Item = (SnapshotChunk, WriteFlags);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        if let Some(t) = self.first.take() {
            let write_flags = WriteFlags::default().buffer_hint(true);
            return Ok(Async::Ready(Some((t, write_flags))));
        }

        let mut buf = match self.remain_bytes {
            0 => return Ok(Async::Ready(None)),
            n if n > SNAP_CHUNK_LEN => vec![0; SNAP_CHUNK_LEN],
            n => vec![0; n],
        };
        let result = self.snap.read_exact(buf.as_mut_slice());
        match result {
            Ok(_) => {
                self.remain_bytes -= buf.len();
                let mut chunk = SnapshotChunk::new();
                chunk.set_data(buf);
                Ok(Async::Ready(Some((
                    chunk,
                    WriteFlags::default().buffer_hint(true),
                ))))
            }
            Err(e) => Err(box_err!("failed to read snapshot chunk: {}", e)),
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
) -> Result<impl Future<Item = SendStat, Error = Error>> {
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

    let chunks = {
        let mut first_chunk = SnapshotChunk::new();
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

    let send = chunks.forward(sink).map_err(Error::from);
    let send = send
        .and_then(|(s, _)| receiver.map_err(Error::from).map(|_| s))
        .then(move |result| {
            send_timer.observe_duration();
            drop(deregister);
            drop(client);
            result.map(|s| {
                fail_point!("snapshot_delete_after_send");
                s.snap.delete();
                // TODO: improve it after rustc resolves the bug.
                // Call `info` in the closure directly will cause rustc
                // panic with `Cannot create local mono-item for DefId`.
                SendStat {
                    key,
                    total_size,
                    elapsed: timer.elapsed(),
                }
            })
        });
    Ok(send)
}

struct RecvSnapContext {
    key: SnapKey,
    file: Option<Box<Snapshot>>,
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
                info!("{} snapshot file {} already exists, skip receiving", key, p);
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

    fn finish<R: RaftStoreRouter>(self, raft_router: R) -> Result<()> {
        let key = self.key;
        if let Some(mut file) = self.file {
            info!("{} saving snapshot file {}", key, file.path());
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

fn recv_snap<R: RaftStoreRouter + 'static>(
    stream: RequestStream<SnapshotChunk>,
    sink: ClientStreamingSink<Done>,
    snap_mgr: SnapManager,
    raft_router: R,
) -> impl Future<Item = (), Error = Error> {
    let stream = stream.map_err(Error::from);

    let f = stream.into_future().map_err(|(e, _)| e).and_then(
        move |(head, chunks)| -> Box<Future<Item = (), Error = Error> + Send> {
            let context = match RecvSnapContext::new(head, &snap_mgr) {
                Ok(context) => context,
                Err(e) => return box future::err(e),
            };

            if context.file.is_none() {
                return box future::result(context.finish(raft_router));
            }

            let context_key = context.key.clone();
            snap_mgr.register(context.key.clone(), SnapEntry::Receiving);

            let recv_chunks = chunks.fold(context, |mut context, mut chunk| -> Result<_> {
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
                Ok(context)
            });

            box recv_chunks
                .and_then(move |context| context.finish(raft_router))
                .then(move |r| {
                    snap_mgr.deregister(&context_key, &SnapEntry::Receiving);
                    r
                })
        },
    );
    f.and_then(move |_| sink.success(Done::new()).map_err(Error::from))
}

pub struct Runner<R: RaftStoreRouter + 'static> {
    env: Arc<Environment>,
    snap_mgr: SnapManager,
    pool: CpuPool,
    raft_router: R,
    security_mgr: Arc<SecurityManager>,
    cfg: Arc<Config>,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
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
            pool: CpuPoolBuilder::new()
                .name_prefix(thd_name!("snap sender"))
                .pool_size(DEFAULT_POOL_SIZE)
                .create(),
            raft_router: r,
            security_mgr,
            cfg,
            sending_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<R: RaftStoreRouter + 'static> Runnable<Task> for Runner<R> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Recv { stream, sink } => {
                if self.recving_count.load(Ordering::SeqCst) >= self.cfg.concurrent_recv_snap_limit
                {
                    warn!("too many recving snapshot tasks, ignore");
                    let status = RpcStatus::new(RpcStatusCode::ResourceExhausted, None);
                    self.pool.spawn(sink.fail(status)).forget();
                    return;
                }
                SNAP_TASK_COUNTER.with_label_values(&["recv"]).inc();

                let snap_mgr = self.snap_mgr.clone();
                let raft_router = self.raft_router.clone();
                let recving_count = Arc::clone(&self.recving_count);
                recving_count.fetch_add(1, Ordering::SeqCst);
                let f = recv_snap(stream, sink, snap_mgr, raft_router).then(move |result| {
                    recving_count.fetch_sub(1, Ordering::SeqCst);
                    if let Err(e) = result {
                        error!("failed to recv snapshot {}", e);
                    }
                    future::ok::<_, ()>(())
                });
                self.pool.spawn(f).forget();
            }
            Task::Send { addr, msg, cb } => {
                if self.sending_count.load(Ordering::SeqCst) >= self.cfg.concurrent_send_snap_limit
                {
                    warn!(
                        "too many sending snapshot tasks, drop Send Snap[to: {}, snap: {:?}]",
                        addr, msg
                    );
                    cb(Err(Error::Other("Too many sending snapshot tasks".into())));
                    return;
                }
                SNAP_TASK_COUNTER.with_label_values(&["send"]).inc();

                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let sending_count = Arc::clone(&self.sending_count);
                sending_count.fetch_add(1, Ordering::SeqCst);

                let f = future::result(send_snap(env, mgr, security_mgr, &self.cfg, &addr, msg))
                    .flatten()
                    .then(move |res| {
                        match res {
                            Ok(stat) => {
                                info!(
                                    "[region {}] sent snapshot {} [size: {}, dur: {:?}]",
                                    stat.key.region_id, stat.key, stat.total_size, stat.elapsed,
                                );
                                cb(Ok(()));
                            }
                            Err(e) => {
                                error!("failed to send snap to {}: {:?}", addr, e);
                                cb(Err(e));
                            }
                        };
                        sending_count.fetch_sub(1, Ordering::SeqCst);
                        future::ok::<_, ()>(())
                    });

                self.pool.spawn(f).forget();
            }
        }
    }
}
