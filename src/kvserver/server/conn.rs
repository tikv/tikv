use std::io::Write;

use mio::{Token, EventLoop, EventSet};
use mio::tcp::TcpStream;

use proto::kvrpcpb::Request;
use util::codec::rpc::decode_msg;
use super::server::{Server, QueueMessage};

// Conn is a abstraction of remote client
pub struct Conn {
    pub sock: TcpStream,
    pub interest: EventSet,
    pub res: Vec<u8>,
}

impl Conn {
    pub fn new(sock: TcpStream) -> Conn {
        Conn {
            sock: sock,
            interest: EventSet::readable(),
            res: Vec::with_capacity(1024),
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

    pub fn read(&mut self, token: Token, event_loop: &mut EventLoop<Server>) {
        // only test here
        let mut m = Request::new();
        let msg_id = decode_msg(&mut self.sock, &mut m).unwrap();

        let sender = event_loop.channel();
        let queue_msg: QueueMessage = QueueMessage::Request(token, msg_id, m);
        let _ = sender.send(queue_msg).map_err(|e| error!("{:?}", e));
        self.interest.remove(EventSet::readable());
    }
}
