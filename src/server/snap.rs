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
use std::sync::Arc;

use mio::Token;
use futures::{future, Async, Future, Poll, Sink, Stream};
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};
use grpc::{ChannelBuilder, Environment, WriteFlags};
use kvproto::raft_serverpb::SnapshotChunk;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use raftstore::store::{SnapEntry, SnapKey, SnapManager, Snapshot};
use util::worker::Runnable;
use util::buf::PipeBuffer;
use util::security::SecurityManager;
use util::collections::{HashMap, HashMapEntry as Entry};

use super::metrics::*;
use super::{Error, Result};
use super::transport::RaftStoreRouter;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;
// How many snapshots can be sent concurrent.
const DEFAULT_SENDER_CONCURRENT: usize = 3;

macro_rules! future_try {
    ($e: expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => return box future::err(e),
        }
    }
}

/// `Task` that `Runner` can handle.
///
/// `Register` register a pending snapshot file with token;
/// `Write` write data to snapshot file;
/// `Close` save the snapshot file;
/// `Discard` discard all the unsaved changes made to snapshot file;
pub enum Task {
    Register(Token, RaftMessage),
    Write(Token, PipeBuffer),
    Close(Token),
    Discard(Token),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Register(token, ref meta) => write!(f, "Register {:?} token: {:?}", meta, token),
            Task::Write(token, _) => write!(f, "Write snap for {:?}", token),
            Task::Close(token) => write!(f, "Close file {:?}", token),
            Task::Discard(token) => write!(f, "Discard file {:?}", token),
        }
    }
}

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

    let chunks = future_try!(get_snap_stream_for_send(mgr.clone(), key, msg));
    let total_size = chunks.remain_bytes;

    let channel_builder = ChannelBuilder::new(env);
    let channel = security_mgr.connect(channel_builder, &addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = future_try!(client.snapshot().map_err(Error::from));

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
                Err(e) => Err(Error::from(e)),
            }
        })
}

fn get_snap_stream_for_send(mgr: SnapManager, key: SnapKey, msg: RaftMessage) -> Result<SnapChunk> {
    mgr.register(key, SnapEntry::Sending);

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
        mgr: mgr,
        key: key,
        snap: s,
        first: Some((first, WriteFlags::default().buffer_hint(true))),
        remain_bytes: total_size,
    })
}

// TODO: support `RecvTask` too.
pub struct TaskHandler {
    pool: CpuPool,
    send_task_sink: UnboundedSender<SendTask>,
    send_task_stream: Option<UnboundedReceiver<SendTask>>,
}

impl TaskHandler {
    pub fn new() -> TaskHandler {
        let pool = CpuPoolBuilder::new()
            .name_prefix(thd_name!("snap sender"))
            .pool_size(DEFAULT_SENDER_POOL_SIZE)
            .create();

        let (tx, rx) = mpsc::unbounded();

        TaskHandler {
            pool: pool,
            send_task_sink: tx,
            send_task_stream: Some(rx),
        }
    }

    pub fn start(
        &mut self,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        snap_mgr: SnapManager,
    ) {
        if let Some(tasks) = self.send_task_stream.take() {
            let pool = self.pool.clone();
            let deal_task = Arc::new(move |task: SendTask| {
                let SendTask { addr, msg, cb } = task;
                let (e, sec, snp) = (env.clone(), security_mgr.clone(), snap_mgr.clone());
                send_snap(&addr, msg, e, sec, snp).then(move |r| {
                    if r.is_err() {
                        error!("failed to send snap to {}: {:?}", addr, r);
                    }
                    Ok(cb(r))
                })
            });
            run_task(pool, tasks, DEFAULT_SENDER_CONCURRENT, deal_task);
        }
    }

    pub fn get_send_task_sink(&self) -> UnboundedSender<SendTask> {
        self.send_task_sink.clone()
    }
}

pub struct Runner<R: RaftStoreRouter + 'static> {
    snap_mgr: SnapManager,
    files: HashMap<Token, (Box<Snapshot>, RaftMessage)>,
    raft_router: R,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
    pub fn new(snap_mgr: SnapManager, r: R) -> Runner<R> {
        Runner {
            snap_mgr: snap_mgr,
            files: map![],
            raft_router: r,
        }
    }
}

impl<R: RaftStoreRouter + 'static> Runnable<Task> for Runner<R> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Register(token, meta) => {
                SNAP_TASK_COUNTER.with_label_values(&["register"]).inc();
                let mgr = self.snap_mgr.clone();
                let key = match SnapKey::from_snap(meta.get_message().get_snapshot()) {
                    Ok(k) => k,
                    Err(e) => {
                        error!("failed to create snap key for token {:?}: {:?}", token, e);
                        return;
                    }
                };
                match mgr.get_snapshot_for_receiving(
                    &key,
                    meta.get_message().get_snapshot().get_data(),
                ) {
                    Ok(snap) => {
                        if snap.exists() {
                            info!(
                                "snapshot file {} already exists, skip receiving.",
                                snap.path()
                            );
                            if let Err(e) = self.raft_router.send_raft_msg(meta) {
                                error!("send snapshot for key {} token {:?}: {:?}", key, token, e);
                            }
                            return;
                        }
                        debug!("begin to receive snap {:?}", meta);
                        mgr.register(key, SnapEntry::Receiving);
                        self.files.insert(token, (snap, meta));
                    }
                    Err(e) => {
                        error!(
                            "failed to create snapshot file for token {:?}: {:?}",
                            token, e
                        );
                        return;
                    }
                }
            }
            Task::Write(token, mut data) => {
                SNAP_TASK_COUNTER.with_label_values(&["write"]).inc();
                match self.files.entry(token) {
                    Entry::Occupied(mut e) => {
                        if let Err(err) = data.write_all_to(&mut e.get_mut().0) {
                            error!(
                                "failed to write data to snapshot file {} for token {:?}: {:?}",
                                e.get_mut().0.path(),
                                token,
                                err
                            );
                            let (_, msg) = e.remove();
                            let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                            self.snap_mgr.deregister(&key, &SnapEntry::Receiving);
                        }
                    }
                    Entry::Vacant(_) => error!("invalid snap token {:?}", token),
                }
            }
            Task::Close(token) => {
                SNAP_TASK_COUNTER.with_label_values(&["close"]).inc();
                match self.files.remove(&token) {
                    Some((mut snap, msg)) => {
                        let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                        info!("saving snapshot to {}", snap.path());
                        defer!({
                            self.snap_mgr.deregister(&key, &SnapEntry::Receiving);
                        });
                        if let Err(e) = snap.save() {
                            error!(
                                "failed to save snapshot file {} for token {:?}: {:?}",
                                snap.path(),
                                token,
                                e
                            );
                            return;
                        }
                        if let Err(e) = self.raft_router.send_raft_msg(msg) {
                            error!("send snapshot for token {:?} err {:?}", token, e);
                        }
                    }
                    None => error!("invalid snap token {:?}", token),
                }
            }
            Task::Discard(token) => {
                SNAP_TASK_COUNTER.with_label_values(&["discard"]).inc();
                if let Some((_, msg)) = self.files.remove(&token) {
                    debug!("discard snapshot: {:?}", msg);
                    // because token is inserted, following can't panic.
                    let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                    self.snap_mgr.deregister(&key, &SnapEntry::Receiving);
                }
            }
        }
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
        let f_inner = f.clone();
        let ff = move || {
            let release_token = tx.send(token);
            f_inner(task).then(|_| release_token)
        };
        inner_pool.spawn_fn(ff).forget();
        future::ok(())
    });
    pool.spawn(future).forget();
}
