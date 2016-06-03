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

use std::vec::Vec;
use std::collections::VecDeque;
use std::option::Option;
use std::boxed::{Box, FnBox};
use std::net::Shutdown;
use std::sync::{Arc, RwLock};

use mio::{Token, EventLoop, EventSet, PollOpt, TryRead, TryWrite};
use mio::tcp::TcpStream;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf, alloc};
use kvproto::msgpb::{Message, MessageType};
use super::{Result, ConnData};
use super::server::Server;
use util::codec::rpc;
use util::HandyRwLock;
use super::transport::RaftStoreRouter;
use super::resolve::StoreAddrResolver;
use super::snapshot_manager::{SnapshotManager, Task};
use byteorder::{ByteOrder, LittleEndian};

pub type OnClose = Box<FnBox() + Send>;
pub type OnWriteComplete = Box<FnBox() + Send>;

enum ConnType {
    HandShake,
    Rpc,
    Snapshot,
}

const SNAPSHOT_PAYLOAD_BUF: usize = 4 * (1 << 20);

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
    snap_manager: Arc<RwLock<SnapshotManager>>,

    // write buffer, including msg header already.
    res: VecDeque<ByteBuf>,

    on_close: Option<OnClose>,
    on_write_complete: Option<OnWriteComplete>,
}

// TODO: this API is disgusting, it's semantic vagueness.
fn try_read_data<T: TryRead, B: MutBuf>(r: &mut T, buf: &mut B) -> Result<()> {
    if buf.remaining() == 0 {
        return Ok(());
    }

    // TODO: use try_read_buf directly if we can solve the compile problem.
    unsafe {
        // header is not full read, we will try read more.
        if let Some(n) = try!(r.try_read(buf.mut_bytes())) {
            if n == 0 {
                debug!("remote close the conn\n");
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
               snap_manager: Arc<RwLock<SnapshotManager>>)
               -> Conn {
        Conn {
            sock: sock,
            token: token,
            interest: EventSet::readable() | EventSet::hup(),
            conn_type: ConnType::HandShake,
            header: create_mem_buf(rpc::MSG_HEADER_LEN),
            read_size: 0,
            file_size: 0,
            payload: None,
            res: VecDeque::new(),
            last_msg_id: 0,
            snap_manager: snap_manager,
            store_id: store_id,
            on_write_complete: None,
            on_close: None,
        }
    }

    pub fn close(&mut self) {
        if self.on_close.is_some() {
            let cb = self.on_close.take().unwrap();
            cb.call_box(());
        }
    }

    // pub fn set_close_callback(&mut self, cb: Option<OnClose>) {
    //     self.on_close = cb
    // }

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
        match self.conn_type {
            ConnType::HandShake => self.handshake(event_loop),
            ConnType::Rpc => self.read_rpc(event_loop),
            ConnType::Snapshot => self.read_snapshot(event_loop),
        }
    }

    fn handshake<T, S>(&mut self, event_loop: &mut EventLoop<Server<T, S>>) -> Result<Vec<ConnData>>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        let msg_or_none = try!(self.read_one_message());
        if msg_or_none.is_none() {
            return Ok(vec![]);
        }

        let mut msg = msg_or_none.unwrap();
        match msg.get_msg_type() {
            MessageType::Snapshot => {
                try!(self.snap_manager.wl().new_worker(self.token,
                                                       msg.take_snapshot_file(),
                                                       msg.take_raft(),
                                                       self.last_msg_id));
                self.conn_type = ConnType::Snapshot;
                self.payload = Some(create_mem_buf(4));
                self.read_snapshot(event_loop)
            }
            _ => {
                self.conn_type = ConnType::Rpc;
                let mut first = vec![ConnData {
                                         msg_id: self.last_msg_id,
                                         msg: msg,
                                     }];
                let mut rem = try!(self.read_rpc(event_loop));
                first.append(&mut rem);
                Ok(first)
            }
        }
    }

    fn read_snapshot<T, S>(&mut self, _: &mut EventLoop<Server<T, S>>) -> Result<Vec<ConnData>>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        while try!(self.read_payload()) {
            try!(self.handle_snapshot_payload());
        }
        Ok(vec![])
    }

    fn handle_snapshot_payload(&mut self) -> Result<()> {
        let mut payload = self.payload.take().unwrap();
        let mut finish = false;
        if self.file_size == 0 {
            // header
            self.file_size = LittleEndian::read_u32(payload.bytes()) as usize;
            payload = create_mem_buf(SNAPSHOT_PAYLOAD_BUF);
        } else if self.read_size + payload.capacity() == self.file_size {
            // last chunk
            finish = true;
            if let Err(e) = self.sock.shutdown(Shutdown::Both) {
                error!("shutdown connection error: {}", e);
            }
        }

        let task = Task::new(payload.bytes(), box move |_| {}, finish);
        try!(self.snap_manager.rl().add_task(&self.token, task));

        if self.read_size + payload.capacity() >= self.file_size {
            payload = create_mem_buf(self.file_size - self.read_size);
        }
        payload.clear();
        self.payload = Some(payload);
        Ok(())
    }

    fn read_payload(&mut self) -> Result<bool> {
        let mut payload = self.payload.take().unwrap();
        try!(try_read_data(&mut self.sock, &mut payload));
        let ret = payload.remaining() == 0;
        self.payload = Some(payload);
        Ok(ret)
    }

    fn read_one_message(&mut self) -> Result<Option<Message>> {
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
        Ok(Some(msg))
    }

    fn read_rpc<T, S>(&mut self, _: &mut EventLoop<Server<T, S>>) -> Result<Vec<ConnData>>
        where T: RaftStoreRouter,
              S: StoreAddrResolver
    {
        let mut bufs = vec![];
        loop {
            // Because we use the edge trigger, so here we must read whole data.
            let msg = try!(self.read_one_message());
            if msg.is_none() {
                break;
            }
            bufs.push(ConnData {
                msg_id: self.last_msg_id,
                msg: msg.unwrap(),
            })
        }

        Ok(bufs)
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

        if self.on_write_complete.is_some() {
            let cb = self.on_write_complete.take().unwrap();
            cb.call_box(());
        }

        Ok(())
    }


    pub fn append_write_buf<T, S>(&mut self,
                                  event_loop: &mut EventLoop<Server<T, S>>,
                                  msg: ConnData,
                                  cb: Option<OnWriteComplete>)
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

        self.on_write_complete = cb;

        Ok(())
    }
}
