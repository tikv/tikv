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
use std::path::{Path, PathBuf};
use std::boxed::{Box, FnBox};
use std::sync::mpsc::Sender;
use std::io::Read;

use super::{Result, Error, ConnData};
use util::worker::{Runnable, Worker};
use bytes::{ByteBuf, MutByteBuf};
use byteorder::{ByteOrder, LittleEndian};
use protobuf::Message;
use raftstore::store::worker::snap::snapshot_file_path;

use kvproto::raftpb::Snapshot;
use kvproto::msgpb::{self, SnapshotFile};
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

pub struct Runner {
    file_path: PathBuf,
    pub file: fs::File,
    msg: RaftMessage,
    msg_id: u64,
    // file_info: SnapshotFile,
    tx: Sender<ConnData>,
}

impl Runner {
    pub fn new(path: &Path,
               file_info: SnapshotFile,
               msg: RaftMessage,
               msg_id: u64,
               tx: Sender<ConnData>)
               -> Runner {
        let file_path = snapshot_file_path(path, &file_info);
        Runner {
            file_path: file_path.to_path_buf(),
            file: fs::File::create(file_path).unwrap(),
            msg: msg,
            msg_id: msg_id,
            // file_info: file_info,
            tx: tx,
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        let mut buf = task.buf;
        let resp = io::copy(&mut buf, &mut self.file);
        if task.last {
            // self.file.flush();

            // TODO change here when region size goes to 1G
            debug!("send snapshot to store...\n");
            let snapshot = load_snapshot(&self.file_path).unwrap();
            self.msg.mut_message().set_snapshot(snapshot);

            let mut msg = msgpb::Message::new();
            msg.set_msg_type(msgpb::MessageType::Raft);
            msg.set_raft(self.msg.clone());
            if let Err(e) = self.tx.send(ConnData {
                msg_id: self.msg_id,
                msg: msg,
            }) {
                error!("send snapshot raft message failed, err={:?}", e);
            }
        }
        task.cb.call_box((resp.map_err(Error::Io),))
    }
}

fn load_snapshot(file_path: &Path) -> Result<Snapshot> {
    let mut file = fs::File::open(file_path).unwrap();
    let mut buf: [u8; 4] = [0; 4];
    try!(file.read(&mut buf));
    let len = LittleEndian::read_u32(&buf);
    let mut vec: Vec<u8> = Vec::with_capacity(len as usize);
    try!(file.read(vec.as_mut_slice()));
    let mut msg = Snapshot::new();
    try!(msg.merge_from_bytes(vec.as_slice()));
    Ok(msg)
}

pub struct SnapshotReceiver {
    pub worker: Worker<Task>,
    pub buf: MutByteBuf,
    pub more: bool,
    pub file_size: usize,
    pub read_size: usize,
}
