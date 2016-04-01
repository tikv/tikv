use std::vec::Vec;
use std::collections::VecDeque;
use std::option::Option;

use mio::{Token, EventLoop, EventSet, PollOpt, TryRead, TryWrite};
use mio::tcp::TcpStream;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf, alloc};

use kvproto::msgpb::Message;
use super::{Result, ConnData};
use super::server::Server;
use util::codec::rpc;
use super::transport::RaftStoreRouter;

pub struct Conn {
    pub sock: TcpStream,
    pub token: Token,
    pub interest: EventSet,

    // peer_addr is for remote peer address, we only set this
    // when we connect to the remote peer.
    pub peer_addr: Option<String>,

    // message header
    last_msg_id: u64,
    header: MutByteBuf,
    // message
    payload: Option<MutByteBuf>,

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
    pub fn new(sock: TcpStream, token: Token, peer_addr: Option<String>) -> Conn {
        Conn {
            sock: sock,
            token: token,
            interest: EventSet::readable() | EventSet::hup(),
            header: create_mem_buf(rpc::MSG_HEADER_LEN),
            payload: None,
            res: VecDeque::new(),
            last_msg_id: 0,
            peer_addr: peer_addr,
        }
    }

    pub fn reregister<T: RaftStoreRouter>(&mut self,
                                          event_loop: &mut EventLoop<Server<T>>)
                                          -> Result<()> {
        try!(event_loop.reregister(&self.sock, self.token, self.interest, PollOpt::edge()));
        Ok(())
    }

    pub fn read<T: RaftStoreRouter>(&mut self,
                                    _: &mut EventLoop<Server<T>>)
                                    -> Result<Vec<ConnData>> {
        let mut bufs = vec![];

        loop {
            // Because we use the edge trigger, so here we must read whole data.
            if self.payload.is_none() {
                try!(try_read_data(&mut self.sock, &mut self.header));
                if self.header.remaining() > 0 {
                    // we need to read more data for header
                    break;
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
                break;
            }

            let mut msg = Message::new();
            try!(rpc::decode_body(payload.bytes(), &mut msg));
            bufs.push(ConnData {
                msg_id: self.last_msg_id,
                msg: msg,
            });

            self.header.clear();
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

    pub fn write<T: RaftStoreRouter>(&mut self,
                                     event_loop: &mut EventLoop<Server<T>>)
                                     -> Result<()> {
        while !self.res.is_empty() {
            let remaining = try!(self.write_buf());

            if remaining > 0 {
                // we don't write all data, so must try later.
                // we have already registered writable, no need registering again.
                return Ok(());
            }
            self.res.pop_front();
        }

        // no data for writing, remove writable.
        self.interest.remove(EventSet::writable());
        self.reregister(event_loop)
    }

    pub fn append_write_buf<T: RaftStoreRouter>(&mut self,
                                                event_loop: &mut EventLoop<Server<T>>,
                                                msg: ConnData)
                                                -> Result<()> {
        // Now we just push data to a write buffer and register writable for later writing.
        // Later we can write data directly, if meet WOUNDBLOCK error(don't write all data OK),
        // we can register writable at that time.
        // We must also check `socket is not connected` error too, when we connect to a remote
        // peer, mio puts this socket in event loop immediately, but this socket may not be
        // connected at that time, so we must register writable too for this case.
        self.res.push_back(msg.encode_to_buf());

        if !self.interest.is_writable() {
            // re-register writable if we have not,
            // if registered, we can only remove this flag when
            // writing all data in writable function.
            self.interest.insert(EventSet::writable());
            return self.reregister(event_loop);
        }

        Ok(())
    }
}
