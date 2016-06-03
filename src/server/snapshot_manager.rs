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
use std::{io, fs, net, thread};
use std::sync::Mutex;
use std::net::SocketAddr;
use std::io::{Read, Write};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::boxed::{Box, FnBox};


use super::{Result, ConnData, SendCh, Msg, Error};
use byteorder::{ByteOrder, LittleEndian};
use util::worker::{Runnable, Worker};
use util::codec::rpc;
use mio::Token;
use bytes::ByteBuf;
use raftstore::store::worker::snap::{snapshot_file_path, load_snapshot};

use kvproto::msgpb::{self, SnapshotFile, MessageType};
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
    token: Token,
    ch: SendCh,
}

impl Runner {
    pub fn new(path: &Path,
               file_info: SnapshotFile,
               msg: RaftMessage,
               msg_id: u64,
               token: Token,
               ch: SendCh)
               -> Runner {
        let file_path = snapshot_file_path(path, &file_info);
        debug!("runner save the file path to {:?} should not same!!\n",
               &file_path);
        Runner {
            file_path: file_path.to_path_buf(),
            file: fs::File::create(file_path).unwrap(),
            msg: msg,
            msg_id: msg_id,
            token: token,
            ch: ch,
        }
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        let mut buf = task.buf;
        let resp = io::copy(&mut buf, &mut self.file);
        if task.last {
            // self.file.flush();
            debug!("snapshot receiver finish\n");
            // TODO change here when region size goes to 1G
            let snapshot = load_snapshot(&self.file_path).unwrap();
            self.msg.mut_message().set_snapshot(snapshot);

            let mut msg = msgpb::Message::new();
            msg.set_msg_type(msgpb::MessageType::Raft);
            msg.set_raft(self.msg.clone());
            if let Err(e) = self.ch.send(Msg::Snapshot {
                token: self.token,
                data: ConnData {
                    msg_id: self.msg_id,
                    msg: msg,
                },
            }) {
                error!("send snapshot raft message failed, err={:?}", e);
            }
            debug!("send snapshot to store...\n");
        }
        task.cb.call_box((resp.map_err(Error::Io),))
    }
}

type SafeWorker = Mutex<Worker<Task>>;

pub struct SnapshotManager {
    snap_path: PathBuf,
    workers: HashMap<Token, SafeWorker>,
    ch: SendCh,
}

impl SnapshotManager {
    pub fn new(snap_path: PathBuf, ch: SendCh) -> SnapshotManager {
        SnapshotManager {
            snap_path: PathBuf::from(snap_path),
            workers: HashMap::new(),
            ch: ch.clone(),
        }
    }

    // , reporter: SnapshotReporter
    pub fn send_snap(&self,
                     sock_addr: SocketAddr,
                     data: ConnData,
                     cb: Box<FnBox(Result<()>) + Send>) {
        let region_id: u64;
        let term: u64;
        let index: u64;
        {
            let raft_msg = data.msg.get_raft();
            region_id = raft_msg.get_region_id();
            let snapshot = raft_msg.get_message().get_snapshot();
            term = snapshot.get_metadata().get_term();
            index = snapshot.get_metadata().get_index();
        }

        let mut final_data = data;
        final_data.msg.set_msg_type(MessageType::Snapshot);

        let mut file_info = SnapshotFile::new();
        file_info.set_region(region_id);
        file_info.set_term(term);
        file_info.set_index(index);

        final_data.msg.set_snapshot_file(file_info);

        let snap_path = self.snap_path.clone();
        // snapshot message consistent of two part:
        // one is snapshot file, the other is the message itself
        // we use separate connection for snapshot file and also the message
        // because if we send them separately in different connection
        // receiver can't assure their order!
        thread::spawn(move || {
            let file_name = snapshot_file_path(&snap_path, final_data.msg.get_snapshot_file());
            debug!("send_snapshot_sock, new thread to send file: {:?}\n",
                   file_name);
            let attr = fs::metadata(&file_name).unwrap();

            let mut file = fs::File::open(&file_name).unwrap();

            let mut conn = net::TcpStream::connect(&sock_addr).unwrap();
            if let Err(e) = rpc::encode_msg(&mut conn, final_data.msg_id, &final_data.msg) {
                error!("write handshake error err {:?}", e);
                cb.call_box((Err(Error::Codec(e)),));
                return;
            }

            let mut buf: [u8; 4] = [0; 4];
            debug!("write file size: {}\n", attr.len());
            LittleEndian::write_u32(&mut buf, attr.len() as u32);
            if let Err(e) = conn.write(&buf) {
                error!("write data error: {}", e);
                cb.call_box((Err(Error::Io(e)),));
                return;
            }
            if let Err(e) = io::copy(&mut file, &mut conn) {
                error!("write data error: {}", e);
                cb.call_box((Err(Error::Io(e)),));
                return;
            }

            debug!("send data finish, wait for close connection\n");
            // wait for reader to consume the data and close connection
            if let Err(e) = conn.read(&mut buf) {
                error!("reader should consume the whole data and close connection: {}",
                       e);
                cb.call_box((Err(Error::Io(e)),));
                return;
            }
            cb.call_box((Ok(()),));
            debug!("send snapshot socket finish!!\n");
        });
    }

    pub fn new_worker(&mut self,
                      token: Token,
                      file_info: SnapshotFile,
                      raft: RaftMessage,
                      msg_id: u64)
                      -> Result<()> {
        let mut worker = Worker::new("snapshot receiver".to_owned());
        let runner = Runner::new(&self.snap_path,
                                 file_info,
                                 raft,
                                 msg_id,
                                 token,
                                 self.ch.clone());
        try!(worker.start(runner));
        self.workers.insert(token, Mutex::new(worker));
        Ok(())
    }

    pub fn add_task(&self, token: &Token, task: Task) -> Result<()> {
        try!(self.workers[token].lock().unwrap().schedule(task));
        Ok(())
    }
}
