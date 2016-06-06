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
use std::net::{SocketAddr, TcpStream, Shutdown};
use std::io::{Read, Write};
use std::collections::HashMap;
use std::path::PathBuf;
use std::boxed::{Box, FnBox};
use std::sync::{Arc, RwLock};
use threadpool::ThreadPool;
use mio::Token;
use bytes::{Buf, ByteBuf};
use protobuf::Message;

use super::{Result, ConnData};
use super::transport::RaftStoreRouter;
use raftstore::store::{SNAP_GEN_PREFIX, SNAP_REV_PREFIX, SnapFile};
use util::worker::Runnable;
use util::codec::rpc;
use util::codec::number::NumberEncoder;
use util::HandyRwLock;

use kvproto::raft_serverpb::RaftMessage;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;

// TODO make it zero copy
pub enum Task {
    Register(Token, RaftMessage),
    Write(Token, ByteBuf),
    Close(Token),
    SendTo {
        addr: SocketAddr,
        data: ConnData,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Register(token, ref meta) => write!(f, "snap {:?} token: {:?}", meta, token),
            Task::Write(token, _) => write!(f, "Snap Write Task for {:?}", token),
            Task::Close(token) => write!(f, "Close file {:?}", token),
            Task::SendTo { ref addr, ref data, .. } => {
                write!(f, "Snap[to: {}, snap: {:?}]", addr, data.msg)
            }
        }
    }
}

fn send_snap(snap_dir: PathBuf, addr: SocketAddr, data: ConnData) -> Result<()> {
    assert!(data.is_snapshot());

    let snap = data.msg.get_raft().get_message().get_snapshot();
    let f = box_try!(SnapFile::from_snap(snap_dir.as_path(), &snap, SNAP_GEN_PREFIX));
    if !f.exists() {
        return Err(box_err!("missing snap file: {:?}", f.path()));
    }
    // TODO: validation.

    let mut f = try!(File::open(f.path()));
    let mut conn = try!(TcpStream::connect(&addr));

    rpc::encode_msg(&mut conn, data.msg_id, &data.msg)
        .and_then(|_| io::copy(&mut f, &mut conn).map_err(From::from))
        .and_then(|_| conn.shutdown(Shutdown::Write).map_err(From::from))
        .and_then(|_| conn.read(&mut []).map_err(From::from))
        .map(|_| ())
        .map_err(From::from)
}

// TODO: check if files can lead to memory leak
pub struct Runner<R: RaftStoreRouter + 'static> {
    snap_dir: PathBuf,
    files: HashMap<Token, (SnapFile, RaftMessage)>,
    pool: ThreadPool,
    raft_router: Arc<RwLock<R>>,
}

impl<R: RaftStoreRouter + 'static> Runner<R> {
    pub fn new<T: Into<PathBuf>>(snap_dir: T, r: Arc<RwLock<R>>) -> Runner<R> {
        Runner {
            snap_dir: snap_dir.into(),
            files: map![],
            pool: ThreadPool::new_with_name("snap sender".to_owned(), DEFAULT_SENDER_POOL_SIZE),
            raft_router: r,
        }
    }
}

impl<R: RaftStoreRouter + 'static> Runnable<Task> for Runner<R> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Register(token, meta) => {
                // TODO: forbid concurrent write.
                match SnapFile::from_snap(self.snap_dir.as_path(),
                                          meta.get_message().get_snapshot(),
                                          SNAP_REV_PREFIX) {
                    Ok(mut f) => {
                        let mut res = f.encode_u64(meta.compute_size() as u64);
                        if res.is_ok() {
                            res = meta.write_to_writer(&mut f).map_err(From::from);
                        }
                        if let Err(e) = res {
                            error!("failed to write meta: {:?}", e);
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
                        if let Err(e) = writer.save() {
                            error!("failed to save file {:?}: {:?}", token, e);
                        }
                        if let Err(e) = self.raft_router.rl().send_raft_msg(msg) {
                            error!("send snapshot for token {:?} err {:?}", token, e);
                        }
                    }
                    None => error!("invalid snap token {:?}", token),
                }
            }
            Task::SendTo { addr, data, cb } => {
                let path = self.snap_dir.clone();
                self.pool.execute(move || {
                    match send_snap(path, addr, data) {
                        Err(e) => error!("failed to send snap to {}: {:?}", addr, e),
                        res => cb(res),
                    }
                });
            }
        }
    }
}
