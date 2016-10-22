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

use std::cmp;
use std::io::Read;

use mio::{Token, EventLoop, EventSet, PollOpt};
use mio::tcp::TcpStream;
use protobuf::Message as PbMessage;

use kvproto::msgpb::Message;
use kvproto::raft_serverpb::RaftSnapshotData;
use super::{Result, ConnData};
use super::server::Server;
use util::codec::rpc;
use super::transport::RaftStoreRouter;
use super::resolve::StoreAddrResolver;
use super::snap::Task as SnapTask;
use util::worker::Scheduler;
use util::buf::PipeBuffer;


#[derive(PartialEq)]
enum ConnType {
    Handshake,
    Rpc,
    Snapshot,
}

const SNAPSHOT_PAYLOAD_BUF: usize = 4 * 1024 * 1024;
const DEFAULT_SEND_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_RECV_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_BUFFER_SHRINK_THRESHOLD: usize = 1024 * 1024;

pub struct Conn {
    pub sock: TcpStream,
    pub token: Token,
    pub interest: EventSet,

    conn_type: ConnType,

    // store id is for remote store, we only set this
    // when we connect to the remote store.
    pub store_id: Option<u64>,

    // message header
    last_msg_id: Option<u64>,

    expect_size: usize,

    snap_scheduler: Scheduler<SnapTask>,

    send_buffer: PipeBuffer,
    recv_buffer: Option<PipeBuffer>,

    pub buffer_shrink_threshold: usize,
}

impl Conn {
    pub fn new(sock: TcpStream,
               token: Token,
               store_id: Option<u64>,
               snap_scheduler: Scheduler<SnapTask>)
               -> Conn {
        Conn {
            sock: sock,
            token: token,
            interest: EventSet::readable() | EventSet::hup(),
            conn_type: ConnType::Handshake,
            expect_size: 0,
            last_msg_id: None,
            snap_scheduler: snap_scheduler,
            store_id: store_id,
            // TODO: Maybe we should need max size to shrink later.
            send_buffer: PipeBuffer::new(DEFAULT_SEND_BUFFER_SIZE),
            recv_buffer: Some(PipeBuffer::new(DEFAULT_RECV_BUFFER_SIZE)),
            buffer_shrink_threshold: DEFAULT_BUFFER_SHRINK_THRESHOLD,
        }
    }

    pub fn close(&mut self) {
        if self.conn_type == ConnType::Snapshot {
            if let Err(e) = self.snap_scheduler.schedule(SnapTask::Discard(self.token)) {
                error!("failed to cleanup snapshot: {:?}", e);
            }
        }
    }

    pub fn reregister<T, S>(&mut self, event_loop: &mut EventLoop<Server<T, S>>) -> Result<()>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        try!(event_loop.reregister(&self.sock, self.token, self.interest, PollOpt::edge()));
        Ok(())
    }

    pub fn on_readable<T, S>(&mut self,
                             event_loop: &mut EventLoop<Server<T, S>>)
                             -> Result<Vec<ConnData>>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        let mut bufs = vec![];
        match self.conn_type {
            ConnType::Handshake => try!(self.handshake(event_loop, &mut bufs)),
            ConnType::Rpc => try!(self.read_rpc(event_loop, &mut bufs)),
            ConnType::Snapshot => try!(self.read_snapshot(event_loop)),
        };
        Ok(bufs)
    }

    fn handshake<T, S>(&mut self,
                       event_loop: &mut EventLoop<Server<T, S>>,
                       bufs: &mut Vec<ConnData>)
                       -> Result<()>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        let mut data = match try!(self.read_one_message()) {
            Some(data) => data,
            None => return Ok(()),
        };
        if data.is_snapshot() {
            self.conn_type = ConnType::Snapshot;

            let mut snap_data = RaftSnapshotData::new();
            try!(snap_data.merge_from_bytes(
                data.msg.get_raft().get_message().get_snapshot().get_data()));
            self.expect_size = snap_data.get_file_size() as usize;
            let expect_cap = cmp::min(SNAPSHOT_PAYLOAD_BUF, self.expect_size);
            // no need to shrink, the connection will be closed soon.
            self.recv_buffer.as_mut().unwrap().ensure(expect_cap);

            let register_task = SnapTask::Register(self.token, data.msg.take_raft());
            box_try!(self.snap_scheduler.schedule(register_task));

            return self.read_snapshot(event_loop);
        }
        bufs.push(data);
        self.conn_type = ConnType::Rpc;
        self.read_rpc(event_loop, bufs)
    }

    fn read_snapshot<T, S>(&mut self, _: &mut EventLoop<Server<T, S>>) -> Result<()>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        // all content should be read, ignore any read operation.
        if self.recv_buffer.is_none() {
            return Ok(());
        }
        // TODO: limit rate
        loop {
            {
                let recv_buffer = self.recv_buffer.as_mut().unwrap();
                try!(recv_buffer.read_from(&mut self.sock));
                // if the snapshot is too small, the default buffer may be not filled.
                if !recv_buffer.is_full() && recv_buffer.len() < self.expect_size {
                    break;
                }
                if recv_buffer.len() > self.expect_size {
                    return Err(box_err!("recv too much data!"));
                }
            }
            let recv_buffer = self.recv_buffer.take().unwrap();
            self.expect_size -= recv_buffer.len();

            let task = SnapTask::Write(self.token, recv_buffer);
            box_try!(self.snap_scheduler.schedule(task));

            if self.expect_size == 0 {
                // last chunk
                box_try!(self.snap_scheduler.schedule(SnapTask::Close(self.token)));
                // let snap_scheduler to close the connection.
                break;
            } else if SNAPSHOT_PAYLOAD_BUF >= self.expect_size {
                self.recv_buffer = Some(PipeBuffer::new(self.expect_size));
            } else {
                self.recv_buffer = Some(PipeBuffer::new(SNAPSHOT_PAYLOAD_BUF));
            }
        }
        Ok(())
    }

    fn read_one_message(&mut self) -> Result<Option<ConnData>> {
        let recv_buffer = self.recv_buffer.as_mut().unwrap();
        if self.last_msg_id.is_none() {
            recv_buffer.ensure(rpc::MSG_HEADER_LEN);
            if recv_buffer.len() < rpc::MSG_HEADER_LEN {
                try!(recv_buffer.read_from(&mut self.sock));
            }
            if recv_buffer.len() < rpc::MSG_HEADER_LEN {
                // we need to read more data for header
                return Ok(None);
            }
            // we have already read whole header, parse it and begin to read payload.
            let (msg_id, payload_len) = try!(rpc::decode_msg_header(recv_buffer));
            self.last_msg_id = Some(msg_id);
            self.expect_size = payload_len;
        }
        recv_buffer.ensure(self.expect_size);
        try!(recv_buffer.read_from(&mut self.sock));
        if recv_buffer.len() < self.expect_size {
            // we need to read more data for payload
            return Ok(None);
        }
        let mut msg = Message::new();
        try!(rpc::decode_body(&mut recv_buffer.take(self.expect_size as u64), &mut msg));
        let msg_id = self.last_msg_id.unwrap();
        self.last_msg_id = None;
        Ok(Some(ConnData {
            msg_id: msg_id,
            msg: msg,
        }))
    }

    fn read_rpc<T, S>(&mut self,
                      _: &mut EventLoop<Server<T, S>>,
                      bufs: &mut Vec<ConnData>)
                      -> Result<()>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        loop {
            // Because we use the edge trigger, so here we must read whole data.
            match try!(self.read_one_message()) {
                None => break,
                Some(d) => bufs.push(d),
            };
        }

        Ok(())
    }

    pub fn on_writable<T, S>(&mut self, event_loop: &mut EventLoop<Server<T, S>>) -> Result<()>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        try!(self.send_buffer.write_to(&mut self.sock));
        if !self.send_buffer.is_empty() {
            // we don't write all data, so must try later.
            // we have already registered writable, no need registering again.
            return Ok(());
        }

        // no data for writing, remove writable
        if self.send_buffer.capacity() > self.buffer_shrink_threshold {
            self.send_buffer.shrink_to(DEFAULT_SEND_BUFFER_SIZE);
        }
        self.interest.remove(EventSet::writable());
        try!(self.reregister(event_loop));

        Ok(())
    }


    pub fn append_write_buf<T, S>(&mut self,
                                  event_loop: &mut EventLoop<Server<T, S>>,
                                  msg: ConnData)
                                  -> Result<()>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        msg.encode_to(&mut self.send_buffer).unwrap();

        if !self.interest.is_writable() {
            // re-register writable if we have not,
            // if registered, we can only remove this flag when
            // writing all data in writable function.
            self.interest.insert(EventSet::writable());
            try!(self.reregister(event_loop));
        }

        Ok(())
    }
}
