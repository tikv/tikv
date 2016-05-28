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
use std::sync::{Arc, RwLock};
use std::sync::mpsc::{Sender, Receiver, channel};

use mio::{Token, EventLoop, EventSet, PollOpt, TryRead, TryWrite};
use mio::tcp::TcpStream;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf, alloc};

use kvproto::msgpb::{Message, MessageType};
use super::{Result, ConnData};
use super::server::Server;
use util::codec::rpc;
use util::worker::Worker;
use super::transport::RaftStoreRouter;
use super::resolve::StoreAddrResolver;
use super::snapshot_receiver::{SnapshotReceiver, Task, Runner};

pub type OnClose = Box<FnBox() + Send>;
pub type OnWriteComplete = Box<FnBox() + Send>;

enum ConnType {
    HandShake,
    Rpc,
    Snapshot,
}

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

    snapshot_receiver: Option<SnapshotReceiver>,
    tx: Sender<ConnData>,
    rx: Receiver<ConnData>,

    // write buffer, including msg header already.
    res: VecDeque<ByteBuf>,

    on_close: Option<OnClose>,
    on_write_complete: Option<OnWriteComplete>,
}

fn remaining_mutbuf<T: MutBuf>(r: &T) -> usize {
    r.remaining()
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
                print!("remote close the conn\n");
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
               store_id: Option<u64>) -> Conn {
        let (tx, rx) = channel();
        Conn{
            sock: sock,
            token: token,
            interest: EventSet::readable() | EventSet::hup(),
            conn_type: ConnType::HandShake,
            header: create_mem_buf(rpc::MSG_HEADER_LEN),
            payload: None,
            res: VecDeque::new(),
            last_msg_id: 0,
            snapshot_receiver: None,
            tx: tx,
            rx: rx,
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

    pub fn set_close_callback(&mut self, cb: Option<OnClose>) {
        self.on_close = cb
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
                let mut worker = Worker::new("snapshot receiver".to_owned());
                // TODO we need store id here!!
                let runner = Runner::new("/tmp/",
                                         msg.take_snapshot(),
                                         msg.take_raft(),
                                         self.last_msg_id,
                                         self.tx.clone());
                try!(worker.start(runner));
                print!("receive a snapshot connection\n");
                self.snapshot_receiver = Some(SnapshotReceiver {
                    buf: create_mem_buf(10 * (1 << 20)),
                    worker: worker,
                    more: false,
                });
                self.conn_type = ConnType::Snapshot;
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
        if let Some(ref mut receiver) = self.snapshot_receiver {
            let mut finish = false;
            loop {
                let closed = try_read_data(&mut self.sock, &mut receiver.buf);
                let remaining = remaining_mutbuf(&receiver.buf);
                if remaining == 0 {
                    receiver.more = true;
                    finish = true;
                }

                // TODO should distinguish between close and error
                if let Err(e) = closed {
                    let cb = box move |_| {
                        warn!("!!!!!!!!!!connection closed!! now send callback\n");
                        // if let Err(e) = ch.send(Msg::ResolveResult {
                        //     store_id: store_id,
                        //     sock_addr: r,
                        //     data: data,
                        // }) {
                        //     error!("send store sock msg err {:?}", e);
                        // }
                    };
                    print!("close connection should go here\n");
                    try!(receiver.worker.schedule(Task::new(receiver.buf.bytes(), cb, true)));
                    return Err(e);
                }

                if remaining == receiver.buf.capacity() {
                    print!("no more available data to be read?\n");
                    break;
                }

                print!("receive data...ringbuf: {:?}\n",
                       remaining_mutbuf(&receiver.buf));
                try!(receiver.worker
                     .schedule(Task::new(receiver.buf.bytes(), box move |_| {}, false)));
                receiver.buf.clear();

                if finish {
                    break;
                }
            }
        }
        Ok(vec![])
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

        if let Ok(snapshot) = self.rx.try_recv() {
            bufs.push(snapshot);
        }

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
