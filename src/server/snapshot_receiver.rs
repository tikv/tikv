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
use std::{io, fs};
use std::boxed::{Box, FnBox};
use std::sync::{RwLock, Arc};
use std::io::Read;

use super::{Result, Error};
use util::worker::{Runnable, Worker};
use util::HandyRwLock;
use bytes::{ByteBuf, MutByteBuf};
use byteorder::{ByteOrder, LittleEndian};
use protobuf::Message;
use raftstore::store::worker::snap::snapshot_file_path;
use super::transport::RaftStoreRouter;

use kvproto::raftpb::Snapshot;
use kvproto::msgpb::SnapshotFile;
use kvproto::raft_serverpb::RaftMessage;

pub type Callback = Box<FnBox(Result<u64>) + Send>;

// TODO make it zero copy
pub struct Task {
    buf: ByteBuf,
    cb: Callback,
    last: bool,
}

impl Task {
    pub fn new(buf: &[u8], cb: Callback, last: bool) -> Task {
        Task {
            buf: ByteBuf::from_slice(buf),
            cb: cb,
            last: last,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot File Receiver Task")
    }
}

pub struct Runner<T: RaftStoreRouter> {
    file_name: String,
    pub file: fs::File,
    msg: RaftMessage,
    file_info: SnapshotFile,
    raft_router: Arc<RwLock<T>>,
}

impl<F: RaftStoreRouter> Runner<F> {
    pub fn new(path: &str,
                file_info: SnapshotFile,
                msg: RaftMessage,
                raft_router: Arc<RwLock<F>>) -> Runner<F> {
        let file_name = snapshot_file_path(path, 0, &file_info);
        Runner {
            file_name: file_name.to_owned(),
            file: fs::File::create(file_name).unwrap(),
            msg: msg,
            file_info: file_info,
            raft_router: raft_router,
        }
    }
}

impl<T: RaftStoreRouter> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        let mut buf = task.buf;
        let resp = io::copy(&mut buf, &mut self.file);
        if task.last {
            // self.file.flush();

            // TODO change here when region size goes to 1G
            print!("send snapshot to store...\n");
            let snapshot = load_snapshot(&self.file_name).unwrap();
            self.msg.mut_message().set_snapshot(snapshot);
            if let Err(e) = self.raft_router.rl().send_(self.msg) {
                error!("send snapshot raft message failed, err={:?}", e);
            }
        }
        task.cb.call_box((resp.map_err(Error::Io),))
    }
}

fn load_snapshot(file_name: &str) -> Result<Snapshot> {
    let mut file = fs::File::open(file_name).unwrap();
    let mut buf: [u8; 4] = [0; 4];
    try!(file.read(&mut buf));
    let len = LittleEndian::read_u32(&buf);

    let mut vec: Vec<u8> = Vec::with_capacity(10);
    try!(file.read(vec.as_mut_slice()));

    let mut msg = Snapshot::new();
    try!(msg.merge_from_bytes(vec.as_slice()));
    Ok(msg)
}

pub struct SnapshotReceiver {
    pub worker: Worker<Task>,
    pub buf: MutByteBuf,
    pub more: bool,
}
