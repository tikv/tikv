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
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use mio::Token;
use futures::{future, Async, Future, Poll, Stream};
use futures_cpupool::{Builder as CpuPoolBuilder, CpuPool};
use grpc::{ChannelBuilder, Environment, WriteFlags};
use kvproto::raft_serverpb::SnapshotChunk;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::tikvpb_grpc::TikvClient;

use raftstore::store::{SnapEntry, SnapKey, SnapManager, Snapshot};
use util::DeferContext;
use util::worker::Runnable;
use util::buf::PipeBuffer;
use util::security::SecurityManager;
use util::collections::{HashMap, HashMapEntry as Entry};

use super::metrics::*;
use super::{Error, Result};
use super::transport::RaftStoreRouter;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;
// How many snapshots can be sent concurrently.
const MAX_SENDER_CONCURRENT: usize = 64;

/// `Task` that `Runner` can handle.
///
/// `Register` register a pending snapshot file with token;
/// `Write` write data to snapshot file;
/// `Close` save the snapshot file;
/// `Discard` discard all the unsaved changes made to snapshot file;
/// `SendTo` send the snapshot file to specified address.
pub enum Task {
    Register(Token, RaftMessage),
    Write(Token, PipeBuffer),
    Close(Token),
    Discard(Token),
    SendTo {
        addr: String,
        msg: RaftMessage,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Register(token, ref meta) => write!(f, "Register {:?} token: {:?}", meta, token),
            Task::Write(token, _) => write!(f, "Write snap for {:?}", token),
            Task::Close(token) => write!(f, "Close file {:?}", token),
            Task::Discard(token) => write!(f, "Discard file {:?}", token),
            Task::SendTo {
                ref addr, ref msg, ..
            } => write!(f, "SendTo Snap[to: {}, snap: {:?}]", addr, msg),
        }
    }
}

struct SnapChunk {
    first: Option<(SnapshotChunk, WriteFlags)>,
    snap: Box<Snapshot>,
    remain_bytes: usize,
}

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

impl Stream for SnapChunk {
    type Item = (SnapshotChunk, WriteFlags);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        if let Some(t) = self.first.take() {
            return Ok(Async::Ready(Some(t)));
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
            first: Some((first_chunk, WriteFlags::default().buffer_hint(true))),
            snap: s,
            remain_bytes: total_size as usize,
        }
    };

    let cb = ChannelBuilder::new(env);
    let channel = security_mgr.connect(cb, addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = client.snapshot()?;

    let send = chunks.forward(sink).map_err(Error::from);
    let send = send.and_then(|(s, _)| receiver.map_err(Error::from).map(|_| s))
        .then(move |result| {
            send_timer.observe_duration();
            drop(deregister);
            drop(client);
            result.map(|s| {
                s.snap.delete();
                // TODO: improve it after rustc resolves the bug.
                // Call `info` in the closure directly will cause rustc
                // panic with `Cannot create local mono-item for DefId`.
                SendStat {
                    key: key,
                    total_size,
                    elapsed: timer.elapsed(),
                }
            })
        });
    Ok(send)
}

pub struct Runner<R: RaftStoreRouter + 'static> {
    env: Arc<Environment>,
    snap_mgr: SnapManager,
    files: HashMap<Token, (Box<Snapshot>, RaftMessage)>,
    pool: CpuPool,
    raft_router: R,
    security_mgr: Arc<SecurityManager>,
    sending_count: Arc<AtomicUsize>,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: SnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
    ) -> Runner<R> {
        Runner {
            env: env,
            snap_mgr: snap_mgr,
            files: map![],
            pool: CpuPoolBuilder::new()
                .name_prefix(thd_name!("snap sender"))
                .pool_size(DEFAULT_SENDER_POOL_SIZE)
                .create(),
            raft_router: r,
            security_mgr: security_mgr,
            sending_count: Arc::new(AtomicUsize::new(0)),
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
            Task::SendTo { addr, msg, cb } => {
                if self.sending_count.load(Ordering::Acquire) >= MAX_SENDER_CONCURRENT {
                    info!("drop SendTo Snap[to: {}, snap: {:?}] silently", addr, msg);
                    cb(Err(Error::Other("Too many snapshot send task".into())));
                    return;
                }
                SNAP_TASK_COUNTER.with_label_values(&["send"]).inc();

                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let sending_count = Arc::clone(&self.sending_count);
                sending_count.fetch_add(1, Ordering::SeqCst);

                let f = future::result(send_snap(env, mgr, security_mgr, &addr, msg))
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
