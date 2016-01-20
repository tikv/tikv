#![allow(dead_code)]

use std::vec::Vec;
// use std::io::{Read, Write};
use std::collections::VecDeque;
use std::option::Option;

use mio::{Token, EventLoop, EventSet, PollOpt, TryRead, TryWrite};
use mio::tcp::TcpStream;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf, alloc};

use raftserver::{Result, ConnData};
use raftserver::server::Server;
use raftserver::handler::ServerHandler;
use util::codec;

pub struct Conn {
    pub sock: TcpStream,
    pub token: Token,
    pub interest: EventSet,

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
        let n = try!(r.try_read(buf.mut_bytes()));
        match n {
            None => {
                // nothing to do here now, but should we return an error or panic?
                error!("connection read None data");
            }
            Some(n) => buf.advance(n),
        }
    }

    Ok(())
}

fn try_write_data<T: TryWrite, B: Buf>(w: &mut T, buf: &mut B) -> Result<()> {
    // TODO: use try_write_buf directly if I can solve the compile problem.
    let n = try!(w.try_write(buf.bytes()));
    match n {
        None => {}
        Some(n) => buf.advance(n),
    }

    Ok(())
}

fn create_mem_buf(s: usize) -> MutByteBuf {
    unsafe {
        ByteBuf::from_mem_ref(alloc::heap(s.next_power_of_two()), s as u32, 0, s as u32).flip()
    }
}


impl Conn {
    pub fn new(sock: TcpStream, token: Token) -> Conn {
        Conn {
            sock: sock,
            token: token,
            interest: EventSet::readable(),
            header: create_mem_buf(codec::MSG_HEADER_LEN),
            payload: None,
            res: VecDeque::new(),
            last_msg_id: 0,
        }
    }

    pub fn reregister<T: ServerHandler>(&mut self,
                                        event_loop: &mut EventLoop<Server<T>>)
                                        -> Result<()> {
        try!(event_loop.reregister(&self.sock,
                                   self.token,
                                   self.interest,
                                   PollOpt::edge() | PollOpt::oneshot()));
        Ok(())
    }

    pub fn reregister_writeable<T: ServerHandler>(&mut self,
                                                  event_loop: &mut EventLoop<Server<T>>)
                                                  -> Result<()> {
        self.interest.insert(EventSet::writable());
        return self.reregister(event_loop);
    }

    pub fn read<T: ServerHandler>(&mut self,
                                  event_loop: &mut EventLoop<Server<T>>)
                                  -> Result<(Vec<ConnData>)> {
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
                let (msg_id, payload_len) = try!(codec::decode_msg_header(self.header
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

            bufs.push(ConnData {
                token: self.token,
                msg_id: self.last_msg_id,
                data: payload.flip(),
            });

            self.header.clear();
        }

        try!(self.reregister(event_loop));

        Ok((bufs))
    }

    pub fn write<T: ServerHandler>(&mut self, event_loop: &mut EventLoop<Server<T>>) -> Result<()> {
        while !self.res.is_empty() {
            let mut buf = self.res.pop_front().unwrap();

            try!(try_write_data(&mut self.sock, &mut buf));

            if buf.remaining() > 0 {
                // well, we don't write all, and need re-write later.
                self.res.push_front(buf);
                break;
            }
        }

        if self.res.is_empty() {
            // no data for writing.
            self.interest.remove(EventSet::writable());
        } else {
            // need to write next time.
            self.interest.insert(EventSet::writable());
        }

        return self.reregister(event_loop);
    }

    pub fn append_write_buf(&mut self, msg: ConnData) {
        self.res.push_back(msg.encode_to_buf());
    }
}
