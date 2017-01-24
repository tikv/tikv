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

use std::fmt::{self, Formatter, Display};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::io::Read;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::boxed::FnBox;
use std::time::{Instant, Duration};
use threadpool::ThreadPool;
use mio::Token;

use super::metrics::*;
use super::{Result, ConnData, Msg};
use super::transport::RaftStoreRouter;
use raftstore::store::{SnapManager, SnapKey, SnapEntry, RecvSnapshotFile, decode_cf_files_size};
use util::worker::Runnable;
use util::codec::rpc;
use util::buf::PipeBuffer;
use util::HandyRwLock;
use util::transport::SendCh;

use protobuf::Message;
use kvproto::raft_serverpb::{RaftMessage, RaftSnapshotData};

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;
const DEFAULT_READ_TIMEOUT: u64 = 30;
const DEFAULT_WRITE_TIMEOUT: u64 = 30;

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
        addr: SocketAddr,
        data: ConnData,
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
            Task::SendTo { ref addr, ref data, .. } => {
                write!(f, "SendTo Snap[to: {}, snap: {:?}]", addr, data.msg)
            }
        }
    }
}

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the snapshot file.
fn send_snap(mgr: SnapManager, addr: SocketAddr, data: ConnData) -> Result<()> {
    assert!(data.is_snapshot());
    let timer = Instant::now();

    let send_timer = SEND_SNAP_HISTOGRAM.start_timer();

    let snap = data.msg.get_raft().get_message().get_snapshot();
    let key = try!(SnapKey::from_snap(&snap));
    mgr.wl().register(key.clone(), SnapEntry::Sending);
    let mut snapshot_files_reader = box_try!(mgr.rl().get_send_snap_file_reader(&key));
    {
        if !snapshot_files_reader.exists() {
            // clean up snapshot file
            snapshot_files_reader.delete();
            mgr.wl().deregister(&key, &SnapEntry::Sending);

            return Err(box_err!("missing snap file: {:?}",
                                snapshot_files_reader.display_path()));
        }
    }
    // snapshot file has been validated when created, so no need to validate again.

    let mut conn = try!(TcpStream::connect(&addr));
    try!(conn.set_nodelay(true));
    try!(conn.set_read_timeout(Some(Duration::from_secs(DEFAULT_READ_TIMEOUT))));
    try!(conn.set_write_timeout(Some(Duration::from_secs(DEFAULT_WRITE_TIMEOUT))));

    let res = rpc::encode_msg(&mut conn, data.msg_id, &data.msg)
        .and_then(|_| io::copy(&mut snapshot_files_reader, &mut conn).map_err(From::from))
        .and_then(|_| conn.read(&mut [0]).map_err(From::from))
        .map(|_| ())
        .map_err(From::from);
    let size = snapshot_files_reader.total_size().unwrap();
    info!("[region {}] sent snapshot {} [size: {}, dur: {:?}]",
          key.region_id,
          key,
          size,
          timer.elapsed());

    send_timer.observe_duration();

    // clean up snapshot file
    snapshot_files_reader.delete();
    mgr.wl().deregister(&key, &SnapEntry::Sending);

    res
}

pub struct Runner<R: RaftStoreRouter + 'static> {
    snap_mgr: SnapManager,
    files: HashMap<Token, (RecvSnapshotFile, RaftMessage)>,
    pool: ThreadPool,
    ch: SendCh<Msg>,
    raft_router: R,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
    pub fn new(snap_mgr: SnapManager, r: R, ch: SendCh<Msg>) -> Runner<R> {
        Runner {
            snap_mgr: snap_mgr,
            files: map![],
            pool: ThreadPool::new_with_name(thd_name!("snap sender"), DEFAULT_SENDER_POOL_SIZE),
            raft_router: r,
            ch: ch,
        }
    }

    pub fn close(&self, token: Token) {
        if let Err(e) = self.ch.send(Msg::CloseConn { token: token }) {
            error!("failed to close connection {:?}: {:?}", token, e);
        }
    }
}

fn get_cf_file_sizes(msg: &RaftMessage) -> Result<Vec<(String, u64)>> {
    let mut snap_data = RaftSnapshotData::new();
    let data = msg.get_message().get_snapshot().get_data();
    try!(snap_data.merge_from_bytes(data));
    let res = try!(decode_cf_files_size(snap_data.get_data()));
    Ok(res)
}

impl<R: RaftStoreRouter + 'static> Runnable<Task> for Runner<R> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Register(token, meta) => {
                SNAP_TASK_COUNTER.with_label_values(&["register"]).inc();
                let mgr = self.snap_mgr.clone();
                let cf_sizes = match get_cf_file_sizes(&meta) {
                    Ok(res) => res,
                    Err(e) => {
                        error!("failed to get cf file sizes from snapshot message, err: {:?}",
                               e);
                        self.close(token);
                        return;
                    }
                };
                match SnapKey::from_snap(meta.get_message().get_snapshot())
                    .and_then(|key| mgr.rl().get_recv_snap_file(&key, cf_sizes).map(|r| (r, key))) {
                    Ok((mut f, k)) => {
                        if f.exists() {
                            info!("file {} already exists, skip receiving.", f.display_path());
                            if let Err(e) = self.raft_router.send_raft_msg(meta) {
                                error!("send snapshot for token {:?} err {:?}", token, e);
                            }
                            self.close(token);
                            return;
                        } else if let Err(e) = f.init() {
                            error!("failed to init snap file for {:?}: {:?}", token, e);
                            self.close(token);
                            return;
                        }
                        debug!("begin to receive snap {:?}", meta);
                        mgr.wl().register(k.clone(), SnapEntry::Receiving);
                        self.files.insert(token, (f, meta));
                    }
                    Err(e) => {
                        error!("failed to create snap file for {:?}: {:?}", token, e);
                        self.close(token);
                    }
                }
            }
            Task::Write(token, mut data) => {
                SNAP_TASK_COUNTER.with_label_values(&["write"]).inc();
                let mut should_close = false;
                match self.files.entry(token) {
                    Entry::Occupied(mut e) => {
                        if let Err(err) = data.write_all_to(&mut e.get_mut().0) {
                            error!("failed to write data to {:?}: {:?}", token, err);
                            let (_, msg) = e.remove();
                            let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                            self.snap_mgr.wl().deregister(&key, &SnapEntry::Receiving);
                            should_close = true;
                        }
                    }
                    Entry::Vacant(_) => error!("invalid snap token {:?}", token),
                }
                if should_close {
                    self.close(token);
                }
            }
            Task::Close(token) => {
                SNAP_TASK_COUNTER.with_label_values(&["close"]).inc();
                match self.files.remove(&token) {
                    Some((mut writer, msg)) => {
                        let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                        info!("saving snapshot to {}", writer.display_path());
                        defer!({
                            self.snap_mgr.wl().deregister(&key, &SnapEntry::Receiving);
                            self.close(token);
                        });
                        // don't need to write checksum, because it's already included in the file.
                        if let Err(e) = writer.save() {
                            writer.delete();
                            error!("failed to save file {:?}: {:?}", token, e);
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
                    self.snap_mgr.wl().deregister(&key, &SnapEntry::Receiving);
                }
            }
            Task::SendTo { addr, data, cb } => {
                SNAP_TASK_COUNTER.with_label_values(&["send"]).inc();
                let mgr = self.snap_mgr.clone();
                self.pool.execute(move || {
                    let res = send_snap(mgr, addr, data);
                    if res.is_err() {
                        error!("failed to send snap to {}: {:?}", addr, res);
                    }
                    cb(res)
                });
            }
        }
    }
}
