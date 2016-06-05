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
use std::{io, thread};
use std::fs::{self, File};
use std::sync::Mutex;
use std::net::{SocketAddr, TcpStream, Shutdown};
use std::io::{Read, Write};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::boxed::{Box, FnBox};
use threadpool::ThreadPool;


use super::{Result, ConnData, SendCh, Msg, Error};
use raftstore::store::SnapFile;
use byteorder::{ByteOrder, LittleEndian};
use util::worker::{Runnable, Worker};
use util::codec::rpc;
use mio::Token;
use bytes::{Buf, ByteBuf};

use kvproto::msgpb::{self, SnapFileMeta, MessageType, Message};
use kvproto::raftpb::Snapshot;
use kvproto::raft_serverpb::RaftMessage;

pub type Callback = Box<FnBox(Result<()>) + Send>;

const DEFAULT_SENDER_POOL_SIZE: usize = 3;

// TODO make it zero copy
pub enum Task {
    Register(Token, SnapFileMeta),
    Write(Token, ByteBuf),
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
            Task::SendTo { ref addr, ref data, .. } => write!(f, "Snap[to: {}]", addr),
        }
    }
}

fn loop_send_file(snap_dir: PathBuf, addr: SocketAddr, mut data: ConnData) -> Result<()> {
    assert_eq!(data.msg.get_msg_type(), MessageType::Snapshot);
    let mut meta = data.msg.take_snap_file_meta();

    let f = try!(SnapFile::new(snap_dir.as_path(),
                               meta.get_region(),
                               meta.get_term(),
                               meta.get_index()));
    if !f.exists() {
        return Err(box_err!("missing snap file: {:?}", f.path()));
    }
    let file_meta = try!(f.meta());
    meta.set_size(file_meta.len());

    let mut msg = Message::new();
    msg.set_msg_type(MessageType::Snapshot);
    msg.set_snap_file_meta(meta);

    let mut f = try!(File::open(f.path()));
    let mut conn = try!(TcpStream::connect(&addr));

    rpc::encode_msg(&mut conn, data.msg_id, &msg)
        .and_then(|_| io::copy(&mut f, &mut conn).map_err(From::from))
        .and_then(|_| conn.shutdown(Shutdown::Write).map_err(From::from))
        .and_then(|_| conn.read(&mut []).map_err(From::from))
        .map(|_| ())
        .map_err(From::from)
}

pub struct Runner {
    snap_dir: PathBuf,
    files: HashMap<Token, SnapFile>,
    pool: ThreadPool,
}

impl Runner {
    pub fn new<T: Into<PathBuf>>(snap_dir: T) -> Runner {
        Runner {
            snap_dir: snap_dir.into(),
            files: map![],
            pool: ThreadPool::new_with_name("snap sender".to_owned(), DEFAULT_SENDER_POOL_SIZE),
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Register(token, meta) => {
                match SnapFile::new(self.snap_dir.as_path(),
                                    meta.get_region(),
                                    meta.get_term(),
                                    meta.get_index()) {
                    Ok(f) => {
                        self.files.insert(token, f);
                    }
                    Err(e) => error!("failed to create snap file for {:?}: {:?}", token, e),
                }
            }
            Task::Write(token, data) => {
                match self.files.get_mut(&token) {
                    Some(writer) => writer.write_all(Buf::bytes(&data)).unwrap(),
                    None => error!("invalid snap token {:?}", token),
                }
            }
            Task::SendTo { addr, data, cb } => {
                let path = self.snap_dir.clone();
                self.pool.execute(move || {
                    match loop_send_file(path, addr, data) {
                        Err(e) => error!("failed to send snap to {}: {:?}", addr, e),
                        res => cb(res),
                    }
                });
            }
        }
    }
}
