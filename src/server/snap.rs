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

use std::fmt::{self, Display, Formatter};
use std::boxed::FnBox;
use std::time::Instant;
use std::sync::{Arc, Mutex};

use futures::{future, Async, Future, Poll, Sink, Stream};
use futures::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};
use grpc::{ChannelBuilder, Environment, Error as GrpcError, WriteFlags};
use kvproto::raft_serverpb::SnapshotChunk;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use raftstore::store::{SnapEntry, SnapKey, SnapManager, Snapshot};
use util::security::SecurityManager;

use super::metrics::*;
use super::{Error, Result};
use super::transport::RaftStoreRouter;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;
// How many snapshots can be sent concurrently.
const DEFAULT_SENDER_CONCURRENT: usize = 3;
// How many snapshots can be received concurrent.
const DEFAULT_RECVER_CONCURRENT: usize = 3;

pub type RecvTask = Box<Stream<Item = SnapshotChunk, Error = GrpcError> + Send>;

pub struct SendTask {
    pub addr: String,
    pub msg: RaftMessage,
    pub cb: Callback,
}

impl Display for SendTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Send Snap[to: {}, snap: {:?}]", self.addr, self.msg)
    }
}

struct SnapChunk {
    _client: TikvClient, // Keep the channel alive during stream sending.
    mgr: SnapManager,
    key: SnapKey,
    snap: Box<Snapshot>,
    first: Option<(SnapshotChunk, WriteFlags)>,
    remain_bytes: usize,
}

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

impl Drop for SnapChunk {
    fn drop(&mut self) {
        self.snap.delete();
        self.mgr.deregister(&self.key, &SnapEntry::Sending);
    }
}

impl Stream for SnapChunk {
    type Item = (SnapshotChunk, WriteFlags);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        if let Some(first) = self.first.take() {
            return Ok(Async::Ready(Some(first)));
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

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the snapshot file.
fn send_snap(
    addr: &str,
    msg: RaftMessage,
    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,
    mgr: SnapManager,
) -> Box<Future<Item = (), Error = Error> + Send> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();
    let send_timer = SEND_SNAP_HISTOGRAM.start_coarse_timer();

    let key = {
        let snap = msg.get_message().get_snapshot();
        future_try!(SnapKey::from_snap(snap).map_err(Error::from))
    };

    let channel_builder = ChannelBuilder::new(env);
    let channel = security_mgr.connect(channel_builder, addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = future_try!(client.snapshot().map_err(Error::from));

    let chunks = future_try!(get_snap_stream_for_send(
        mgr.clone(),
        key.clone(),
        msg,
        client
    ));
    let total_size = chunks.remain_bytes;

    let send = chunks.forward(sink);
    box send.and_then(|_| receiver.map_err(Error::from))
        .then(move |r| {
            send_timer.observe_duration();
            match r {
                Ok(_) => {
                    info!(
                        "[region {}] sent snapshot {} [size: {}, dur: {:?}]",
                        key.region_id,
                        key,
                        total_size,
                        timer.elapsed()
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        })
}

fn get_snap_stream_for_send(
    mgr: SnapManager,
    key: SnapKey,
    msg: RaftMessage,
    client: TikvClient,
) -> Result<SnapChunk> {
    mgr.register(key.clone(), SnapEntry::Sending);

    // snapshot file has been validated when created, so no need to validate again.
    let s = match mgr.get_snapshot_for_sending(&key) {
        Ok(s) => s,
        Err(e) => {
            mgr.deregister(&key, &SnapEntry::Sending);
            return Err(Error::from(e));
        }
    };
    if !s.exists() {
        mgr.deregister(&key, &SnapEntry::Sending);
        return Err(box_err!("missing snap file: {:?}", s.path()));
    }

    let total_size = match s.total_size() {
        Ok(size) => size as usize,
        Err(e) => {
            mgr.deregister(&key, &SnapEntry::Sending);
            return Err(Error::from(e));
        }
    };

    let mut first = SnapshotChunk::new();
    first.set_message(msg);

    Ok(SnapChunk {
        _client: client,
        mgr: mgr,
        key: key,
        snap: s,
        first: Some((first, WriteFlags::default().buffer_hint(true))),
        remain_bytes: total_size,
    })
}

struct RecvSnapContext {
    key: SnapKey,
    file: Option<Box<Snapshot>>,
    raft_msg: RaftMessage,
}

/// Receive the snapshot from a chunk stream.
fn recv_snap<R: RaftStoreRouter + 'static>(
    task: RecvTask,
    snap_mgr: SnapManager,
    raft_router: R,
) -> impl Future<Item = (), Error = ()> {
    let head_and_chunks = task.into_future().map_err(|(e, _)| {
        error!("receive first chunk from gRPC: {}", e);
    });

    let snap_mgr_1 = snap_mgr.clone();
    let all_chunks_success = head_and_chunks.and_then(
        move |(head, chunks)| -> Box<Future<Item = RecvSnapContext, Error = ()> + Send> {
            let context = match get_context_from_head_chunk(head, snap_mgr_1) {
                Ok(context) => context,
                Err(_) => return box future::err(()),
            };
            if context.file.is_none() {
                return box future::ok(context);
            }

            let key = context.key.clone();
            let chunks = chunks.map_err(move |e| {
                error!("{} receive chunks fail from gRPC: {}", key, e);
            });

            box chunks.fold(context, |mut context, mut chunk| {
                let data = chunk.take_data();
                if !data.is_empty() {
                    let file = context.file.as_mut().unwrap();
                    let key = context.key.clone();
                    file.write_all(&data).map_err(|e| {
                        let path = file.path();
                        error!("{} failed to write snapshot file {}: {}", key, path, e);
                    })?;
                } else {
                    error!("{} receive chunk with empty data", context.key);
                    return Err(());
                }
                Ok(context)
            })
        },
    );

    all_chunks_success.and_then(move |context| {
        let (key, file, raft_msg) = (context.key, context.file, context.raft_msg);
        if let Some(mut file) = file {
            file.save().map_err(|e| {
                let path = file.path();
                error!("{} failed to save snapshot file {}: {:?}", key, path, e);
            })?;
        }
        raft_router.send_raft_msg(raft_msg).map_err(|e| {
            error!("{} failed to send snapshot to raft: {}", key, e);
        })?;
        Ok(())
    })
}

fn get_context_from_head_chunk(
    head_chunk: Option<SnapshotChunk>,
    snap_mgr: SnapManager,
) -> ::std::result::Result<RecvSnapContext, ()> {
    let mut head = head_chunk.ok_or(())?;
    if !head.has_message() {
        error!("no raft message in the first chunk");
        return Err(());
    }

    let meta = head.take_message();
    let key = SnapKey::from_snap(meta.get_message().get_snapshot()).map_err(|e| {
        error!("failed to create snap key: {:?}", e);
    })?;

    let snap = {
        let s = snap_mgr
            .get_snapshot_for_receiving(&key, meta.get_message().get_snapshot().get_data())
            .map_err(|e| {
                error!("{} failed to create snapshot file: {:?}", key, e);
            })?;
        if s.exists() {
            let p = s.path();
            info!("{} snapshot file {} already exists, skip receiving", key, p);
            None
        } else {
            Some(s)
        }
    };

    Ok(RecvSnapContext {
        key: key,
        file: snap,
        raft_msg: meta,
    })
}

// TODO: support `RecvTask` too.
pub struct TaskHandler {
    pool: CpuPool,
    send_task_sink: UnboundedSender<SendTask>,
    recv_task_sink: Sender<RecvTask>,
    task_streams: Option<(UnboundedReceiver<SendTask>, Receiver<RecvTask>)>,
}

impl Default for TaskHandler {
    fn default() -> TaskHandler {
        let pool = CpuPoolBuilder::new()
            .name_prefix(thd_name!("snap sender"))
            .pool_size(DEFAULT_SENDER_POOL_SIZE)
            .create();

        let (stx, srx) = mpsc::unbounded();
        let (rtx, rrx) = mpsc::channel(DEFAULT_RECVER_CONCURRENT);

        TaskHandler {
            pool: pool,
            send_task_sink: stx,
            recv_task_sink: rtx,
            task_streams: Some((srx, rrx)),
        }
    }
}

impl TaskHandler {
    pub fn start<R: RaftStoreRouter + 'static>(
        &mut self,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        snap_mgr: SnapManager,
        raft_router: R,
    ) {
        if let Some((send_tasks, recv_tasks)) = self.task_streams.take() {
            let pool = self.pool.clone();
            let snap_mgr_1 = snap_mgr.clone();
            let deal_send_task = Arc::new(move |task: SendTask| {
                let SendTask { addr, msg, cb } = task;
                let (e, sec, snp) = (
                    Arc::clone(&env),
                    Arc::clone(&security_mgr),
                    snap_mgr_1.clone(),
                );
                send_snap(&addr, msg, e, sec, snp).then(move |r| {
                    if r.is_err() {
                        error!("failed to send snap to {}: {:?}", addr, r);
                    }
                    Ok(cb(r))
                })
            });
            run_task(pool, send_tasks, DEFAULT_SENDER_CONCURRENT, deal_send_task);

            let pool = self.pool.clone();
            let raft_router = Mutex::new(raft_router);
            let deal_recv_task = Arc::new(move |task: RecvTask| {
                let (snp, router) = (snap_mgr.clone(), raft_router.lock().unwrap().clone());
                recv_snap(task, snp, router)
            });
            run_task(pool, recv_tasks, DEFAULT_RECVER_CONCURRENT, deal_recv_task);
        }
    }

    pub fn get_send_task_sink(&self) -> UnboundedSender<SendTask> {
        self.send_task_sink.clone()
    }

    pub fn get_recv_task_sink(&self) -> Sender<RecvTask> {
        self.recv_task_sink.clone()
    }
}

fn run_task<T, R, D, F>(pool: CpuPool, tasks: R, concurrent: usize, f: Arc<D>)
where
    T: Send + 'static,
    R: Stream<Item = T, Error = ()> + Send + 'static,
    D: Fn(T) -> F + Send + Sync + 'static,
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    // Init a bounded channel with fixed capacity as a token bucket.
    let (mut tx, rx) = mpsc::channel::<i32>(concurrent);
    for _ in 0..concurrent {
        tx = tx.send(1).wait().unwrap();
    }
    let inner_pool = pool.clone();
    let future = rx.zip(tasks).for_each(move |(token, task)| {
        let tx = tx.clone();
        let f_inner = Arc::clone(&f);
        let ff = move || {
            let release_token = tx.send(token);
            f_inner(task).then(|_| release_token)
        };
        inner_pool.spawn_fn(ff).forget();
        future::ok(())
    });
    pool.spawn(future).forget();
}
