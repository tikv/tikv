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

use std::collections::VecDeque;
use std::net::Shutdown;
use std::cmp;

use mio::{Token, EventLoop, EventSet, PollOpt, TryRead, TryWrite};
use mio::tcp::TcpStream;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf, alloc};
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


#[derive(PartialEq)]
enum ConnType {
    Handshake,
    Rpc,
    Snapshot,
}

const SNAPSHOT_PAYLOAD_BUF: usize = 4 * 1024 * 1024;

pub struct Conn {
    pub sock: TcpStream,
    pub token: Token,
    pub interest: EventSet,

    conn_type: ConnType,

    // store id is for remote store, we only set this
    // when we connect to the remote store.
    pub store_id: Option<u64>,

    // message header
    last_msg_id: u64,
    header: MutByteBuf,
    // message
    payload: Option<MutByteBuf>,

    file_size: usize,
    read_size: usize,
    snap_scheduler: Scheduler<SnapTask>,

    // write buffer, including msg header already.
    res: VecDeque<ByteBuf>,
}

fn try_read_data<T: TryRead, B: MutBuf>(r: &mut T, buf: &mut B) -> Result<()> {
    if buf.remaining() == 0 {
        return Ok(());
    }

    // TODO: use try_read_buf directly if we can solve the compile problem.
    unsafe {
        // header is not full read, we will try read more.
        if let Some(n) = try!(r.try_read(buf.mut_bytes())) {
            if n == 0 {
                // 0 means remote has closed the socket.
                return Err(box_err!("remote has closed the connection"));
            }
            buf.advance(n)
        }
    }

    Ok(())
}

fn create_mem_buf(s: usize) -> MutByteBuf {
    unsafe {
        ByteBuf::from_mem_ref(alloc::heap(s.next_power_of_two()), s as u32, 0, s as u32).flip()
    }
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
            header: create_mem_buf(rpc::MSG_HEADER_LEN),
            read_size: 0,
            file_size: 0,
            payload: None,
            res: VecDeque::new(),
            last_msg_id: 0,
            snap_scheduler: snap_scheduler,
            store_id: store_id,
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
            self.file_size = snap_data.get_file_size() as usize;
            self.payload = Some(create_mem_buf(cmp::min(SNAPSHOT_PAYLOAD_BUF, self.file_size)));

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
        // TODO: limit rate
        while try!(self.read_payload()) {
            try!(self.handle_snapshot_payload());
        }
        Ok(())
    }

    fn handle_snapshot_payload(&mut self) -> Result<()> {
        let mut payload = self.payload.take().unwrap();
        let cap = payload.capacity();

        let task = SnapTask::Write(self.token, payload.flip());
        box_try!(self.snap_scheduler.schedule(task));

        if self.read_size + cap == self.file_size {
            // last chunk
            box_try!(self.snap_scheduler.schedule(SnapTask::Close(self.token)));
            if let Err(e) = self.sock.shutdown(Shutdown::Both) {
                error!("shutdown connection error: {}", e);
            }
        }

        payload = if self.read_size == self.file_size {
            create_mem_buf(1)
        } else if self.read_size + cap >= self.file_size {
            create_mem_buf(self.file_size - self.read_size)
        } else {
            create_mem_buf(cap)
        };
        self.payload = Some(payload);
        Ok(())
    }

    fn read_payload(&mut self) -> Result<bool> {
        let payload = self.payload.as_mut().unwrap();
        try!(try_read_data(&mut self.sock, payload));
        let ret = payload.remaining() == 0;
        Ok(ret)
    }

    fn read_one_message(&mut self) -> Result<Option<ConnData>> {
        if self.payload.is_none() {
            try!(try_read_data(&mut self.sock, &mut self.header));
            if self.header.remaining() > 0 {
                // we need to read more data for header
                return Ok(None);
            }

            // we have already read whole header, parse it and begin to read payload.
            let (msg_id, payload_len) = try!(rpc::decode_msg_header(self.header
                .bytes()));
            self.last_msg_id = msg_id;
            self.payload = Some(create_mem_buf(payload_len));
        }

        // payload here can't be None.
        let mut payload = self.payload.take().unwrap();
        try!(try_read_data(&mut self.sock, &mut payload));
        if payload.remaining() > 0 {
            // we need to read more data for payload
            self.payload = Some(payload);
            return Ok(None);
        }

        let mut msg = Message::new();
        try!(rpc::decode_body(payload.bytes(), &mut msg));
        self.header.clear();
        Ok(Some(ConnData {
            msg_id: self.last_msg_id,
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

    fn write_buf(&mut self) -> Result<usize> {
        // we check empty before.
        let mut buf = self.res.front_mut().unwrap();

        if let Some(n) = try!(self.sock.try_write(buf.bytes())) {
            buf.advance(n)
        }

        Ok(buf.remaining())
    }

    pub fn write<T, S>(&mut self, event_loop: &mut EventLoop<Server<T, S>>) -> Result<()>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        while !self.res.is_empty() {
            let remaining = try!(self.write_buf());

            if remaining > 0 {
                // we don't write all data, so must try later.
                // we have already registered writable, no need registering again.
                return Ok(());
            }
            self.res.pop_front();
        }

        // no data for writing, remove writable
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
        // Now we just push data to a write buffer and register writable for later writing.
        // Later we can write data directly, if meet WOUNDBLOCK error(don't write all data OK),
        // we can register writable at that time.
        // We must also check `socket is not connected` error too, when we connect to a remote
        // store, mio puts this socket in event loop immediately, but this socket may not be
        // connected at that time, so we must register writable too for this case.
        self.res.push_back(msg.encode_to_buf());

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
