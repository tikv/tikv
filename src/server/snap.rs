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
use std::fs::File;
use std::net::{SocketAddr, TcpStream};
use std::io::{Read, Write};
use std::collections::HashMap;
use std::boxed::FnBox;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use threadpool::ThreadPool;
use mio::Token;
use bytes::{Buf, ByteBuf};
use protobuf::Message;

use super::{Result, ConnData, SendCh, Msg};
use super::transport::RaftStoreRouter;
use raftstore::store::{SnapFile, SnapManager, SnapKey, SnapEntry};
use util::worker::Runnable;
use util::codec::rpc;
use util::codec::number::NumberEncoder;
use util::HandyRwLock;

use kvproto::raft_serverpb::RaftMessage;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;

/// `Task` that `Runner` can handle.
///
/// `Register` register a pending snapshot file with token;
/// `Write` write data to snapshot file;
/// `Close` save the snapshot file;
/// `Discard` discard all the unsaved changes made to snapshot file;
/// `SendTo` send the snapshot file to specified address.
pub enum Task {
    Register(Token, RaftMessage),
    Write(Token, ByteBuf),
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
    let snap = data.msg.get_raft().get_message().get_snapshot();
    let key = try!(SnapKey::from_snap(&snap));
    mgr.wl().register(key.clone(), SnapEntry::Sending);
    let snap_file = box_try!(mgr.rl().get_snap_file(&key, true));
    defer!({
        snap_file.delete();
        mgr.wl().deregister(&key, &SnapEntry::Sending);
    });
    if !snap_file.exists() {
        return Err(box_err!("missing snap file: {:?}", snap_file.path()));
    }
    // snapshot file has been validated when created, so no need to validate again.

    let mut f = try!(File::open(snap_file.path()));
    let mut conn = try!(TcpStream::connect(&addr));
    try!(conn.set_nodelay(true));

    let res = rpc::encode_msg(&mut conn, data.msg_id, &data.msg)
        .and_then(|_| io::copy(&mut f, &mut conn).map_err(From::from))
        .and_then(|_| conn.read(&mut [0]).map_err(From::from))
        .map(|_| ())
        .map_err(From::from);
    if let Ok(meta) = snap_file.meta() {
        debug!("sending snapshot[path: {}, size: {}] takes {:?}",
               snap_file.path().display(),
               meta.len(),
               timer.elapsed());
    }
    metric_time!("server.send_snap", timer.elapsed());
    res
}

pub struct Runner<R: RaftStoreRouter + 'static> {
    snap_mgr: SnapManager,
    files: HashMap<Token, (SnapFile, RaftMessage)>,
    pool: ThreadPool,
    ch: SendCh,
    raft_router: Arc<RwLock<R>>,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
    pub fn new(snap_mgr: SnapManager, r: Arc<RwLock<R>>, ch: SendCh) -> Runner<R> {
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

impl<R: RaftStoreRouter + 'static> Runnable<Task> for Runner<R> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Register(token, meta) => {
                let mgr = self.snap_mgr.clone();
                match SnapKey::from_snap(meta.get_message().get_snapshot())
                    .and_then(|key| mgr.rl().get_snap_file(&key, false).map(|r| (r, key))) {
                    Ok((mut f, k)) => {
                        if f.exists() {
                            info!("file {} already exists, skip receiving.",
                                  f.path().display());
                            if let Err(e) = self.raft_router.rl().send_raft_msg(meta) {
                                error!("send snapshot for token {:?} err {:?}", token, e);
                            }
                            self.close(token);
                            return;
                        }
                        debug!("begin to receive snap {:?}", meta);
                        mgr.wl().register(k.clone(), SnapEntry::Receiving);
                        let mut res = f.encode_u64(meta.compute_size() as u64);
                        if res.is_ok() {
                            res = meta.write_to_writer(&mut f).map_err(From::from);
                        }
                        if let Err(e) = res {
                            error!("failed to write meta: {:?}", e);
                            self.close(token);
                            mgr.wl().deregister(&k, &SnapEntry::Receiving);
                            return;
                        }
                        self.files.insert(token, (f, meta));
                    }
                    Err(e) => error!("failed to create snap file for {:?}: {:?}", token, e),
                }
            }
            Task::Write(token, data) => {
                match self.files.get_mut(&token) {
                    Some(&mut (ref mut writer, _)) => {
                        if let Err(e) = writer.write_all(Buf::bytes(&data)) {
                            error!("failed to write data to {:?}: {:?}", token, e);
                        }
                    }
                    None => error!("invalid snap token {:?}", token),
                }
            }
            Task::Close(token) => {
                match self.files.remove(&token) {
                    Some((mut writer, msg)) => {
                        let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                        info!("saving snapshot to {}", writer.path().display());
                        defer!({
                            self.snap_mgr.wl().deregister(&key, &SnapEntry::Receiving);
                            self.close(token);
                        });
                        if let Err(e) = writer.save() {
                            error!("failed to save file {:?}: {:?}", token, e);
                            return;
                        }
                        if let Err(e) = self.raft_router.rl().send_raft_msg(msg) {
                            error!("send snapshot for token {:?} err {:?}", token, e);
                        }
                    }
                    None => error!("invalid snap token {:?}", token),
                }
            }
            Task::Discard(token) => {
                if let Some((_, msg)) = self.files.remove(&token) {
                    debug!("discard snapshot: {:?}", msg);
                    // because token is inserted, following can't panic.
                    let key = SnapKey::from_snap(msg.get_message().get_snapshot()).unwrap();
                    self.snap_mgr.wl().deregister(&key, &SnapEntry::Receiving);
                }
            }
            Task::SendTo { addr, data, cb } => {
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
