use std::io::{Read, Write};

use mio::{Token, EventLoop, EventSet, TryRead};
use mio::tcp::TcpStream;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf, alloc};

use proto::kvrpcpb::Request;
use util::codec::rpc;
use util::codec::Error;
use super::server::{Server, QueueMessage};

// alloc exactly s length
fn create_mem_buf(s: usize) -> MutByteBuf {
    unsafe {
        ByteBuf::from_mem_ref(alloc::heap(s.next_power_of_two()), s as u32, 0, s as u32).flip()
    }
}

fn try_read_data<T: TryRead, B: MutBuf>(r: &mut T, buf: &mut B) -> Result<(), Error> {
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

// Conn is a abstraction of remote client
pub struct Conn {
    pub sock: TcpStream,
    pub interest: EventSet,
    pub res: Vec<u8>,

    // message content
    last_msg_id: u64,
    header: MutByteBuf,
    payload: Option<MutByteBuf>,
}

impl Conn {
    pub fn new(sock: TcpStream) -> Conn {
        Conn {
            sock: sock,
            interest: EventSet::readable(),
            res: Vec::with_capacity(1024),
            last_msg_id: 0,
            header: create_mem_buf(rpc::MSG_HEADER_LEN),
            payload: None,
        }
    }

    pub fn write(&mut self) {
        match self.sock.write(&self.res) {
            Ok(n) => debug!("write {} bytes successfully", n),
            Err(e) => warn!("sock write failed {:?}", e),
        }
        self.interest.remove(EventSet::writable());
        self.interest.insert(EventSet::readable());
    }

    pub fn read(&mut self, token: Token, event_loop: &mut EventLoop<Server>) -> Result<(), Error> {
        let payload: Option<MutByteBuf> = None;
        loop {
            // Because we use the edge trigger, so here we must read whole data.
            // payload.is_none() means need read header completely first
            if payload.is_none() {
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
            // Payload here can't be None.
            let mut payload = self.payload.take().unwrap();
            try!(try_read_data(&mut self.sock, &mut payload));
            if payload.remaining() > 0 {
                // We need to read more data for payload.
                self.payload = Some(payload);
                break;
            }

            let mut m = Request::new();
            try!(rpc::decode_body(&payload.bytes().to_vec(), &mut m));
            let sender = event_loop.channel();
            let queue_msg: QueueMessage = QueueMessage::Request(token, self.last_msg_id, m);
            let _ = sender.send(queue_msg).map_err(|e| error!("{:?}", e));

            self.payload = None;
            self.header.clear();
        }
        self.interest.remove(EventSet::readable());
        Ok(())
    }
}
