use std::collections::VecDeque;

use mio::{Token, EventLoop, EventSet, TryRead, TryWrite, PollOpt};
use mio::tcp::TcpStream;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf, alloc};
use protobuf::core::Message;

use proto::kvrpcpb::{Request, Response};
use util::codec::rpc;
use kvserver::Result;
use super::server::{Server, QueueMessage};

// alloc exactly s length
fn create_mem_buf(s: usize) -> MutByteBuf {
    unsafe {
        ByteBuf::from_mem_ref(alloc::heap(s.next_power_of_two()), s as u32, 0, s as u32).flip()
    }
}

fn try_read_data<T: TryRead, B: MutBuf>(r: &mut T, buf: &mut B) -> Result<()> {
    if buf.remaining() == 0 {
        return Ok(());
    }

    // TODO: use try_read_buf directly if we can solve the compile problem.
    unsafe {
        // header is not full read, we will try read more.
        if let Some(n) = try!(r.try_read(buf.mut_bytes())) {
            buf.advance(n);
        }
    }

    Ok(())
}

// Conn is a abstraction of remote client
pub struct Conn {
    pub sock: TcpStream,
    pub token: Token,
    pub interest: EventSet,

    res: VecDeque<ByteBuf>,
    // message content
    last_msg_id: u64,
    header: MutByteBuf,
    payload: Option<MutByteBuf>,
}

impl Conn {
    pub fn new(sock: TcpStream, token: Token) -> Conn {
        Conn {
            sock: sock,
            token: token,
            interest: EventSet::readable(),
            res: VecDeque::new(),
            last_msg_id: 0,
            header: create_mem_buf(rpc::MSG_HEADER_LEN),
            payload: None,
        }
    }

    fn write_buf(&mut self) -> Result<usize> {
        // we check empty before.
        let mut buf = self.res.front_mut().unwrap();

        if let Some(n) = try!(self.sock.try_write(buf.bytes())) {
            buf.advance(n);
        }

        Ok(buf.remaining())
    }

    pub fn write(&mut self, event_loop: &mut EventLoop<Server>) -> Result<()> {
        while !self.res.is_empty() {
            let remaining = try!(self.write_buf());

            if remaining > 0 {
                // we don't write all data, so must try later.
                // we have already registered writable, no need registering again.
                return Ok(());
            }
            self.res.pop_front();
        }
        self.interest.remove(EventSet::writable());
        try!(event_loop.reregister(&self.sock, self.token, self.interest, PollOpt::edge()));
        Ok(())
    }

    pub fn read(&mut self, event_loop: &mut EventLoop<Server>) -> Result<()> {
        loop {
            // Because we use the edge trigger, so here we must read whole data.
            // self.payload.is_none() means need read header completely first
            if self.payload.is_none() {
                try!(try_read_data(&mut self.sock, &mut self.header));
                if self.header.remaining() > 0 {
                    // We need to read more data for header.
                    break;
                }
                // We have already read whole header, parse it and begin to read payload.
                let (msg_id, payload_len) = try!(rpc::decode_msg_header(self.header.bytes()));
                self.last_msg_id = msg_id;
                self.payload = Some(create_mem_buf(payload_len));
            }
            // payload here can't be None.
            let mut payload = self.payload.take().unwrap();
            try!(try_read_data(&mut self.sock, &mut payload));
            if payload.remaining() > 0 {
                // We need to read more data for payload.
                self.payload = Some(payload);
                break;
            }

            let mut m = Request::new();
            try!(rpc::decode_body(payload.bytes(), &mut m));
            let sender = event_loop.channel();
            let queue_msg: QueueMessage = QueueMessage::Request(self.token, self.last_msg_id, m);
            if let Err(e) = sender.send(queue_msg) {
                error!("{:?}", e);
            }

            self.payload = None;
            self.header.clear();
        }
        Ok(())
    }

    pub fn append_write_buf(&mut self,
                            event_loop: &mut EventLoop<Server>,
                            msg_id: u64,
                            resp: Response)
                            -> Result<()> {
        let resp_len: usize = rpc::MSG_HEADER_LEN + resp.compute_size() as usize;
        let mut resp_buf = ByteBuf::mut_with_capacity(resp_len);
        rpc::encode_msg(&mut resp_buf, msg_id, &resp).unwrap();
        self.res.push_back(resp_buf.flip());
        if !self.interest.is_writable() {
            self.interest.insert(EventSet::writable());
            try!(event_loop.reregister(&self.sock, self.token, self.interest, PollOpt::edge()));
        }
        Ok(())
    }
}
