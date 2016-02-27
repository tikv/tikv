use std::string::String;

use bytes::ByteBuf;
use mio::{self, Token};
use protobuf::Message;

use raftserver::{Result, send_msg};
use util::codec::rpc;

use proto::raft_serverpb;

pub mod bootstrap;
pub mod config;
mod bench;
mod conn;
pub mod server;
mod transport;

pub use self::config::{Config, StoreConfig};
pub use self::server::{Server, create_event_loop};

pub struct ConnData {
    msg_id: u64,
    msg: raft_serverpb::Message,
}

impl ConnData {
    pub fn new(msg_id: u64, msg: raft_serverpb::Message) -> ConnData {
        ConnData {
            msg_id: msg_id,
            msg: msg,
        }
    }

    pub fn encode_to_buf(&self) -> ByteBuf {
        let mut buf = ByteBuf::mut_with_capacity(rpc::MSG_HEADER_LEN +
                                                 self.msg.compute_size() as usize);

        // Must ok here
        rpc::encode_msg(&mut buf, self.msg_id, &self.msg).unwrap();

        buf.flip()
    }
}

pub enum Msg {
    // Quit event loop.
    Quit,
    // Read data from connection.
    ReadData {
        token: Token,
        data: ConnData,
    },
    // Write data to connection.
    WriteData {
        token: Token,
        data: ConnData,
    },
    // Send data to remote peer with address.
    SendPeer {
        addr: String,
        data: ConnData,
    },
}

#[derive(Debug)]
pub struct SendCh {
    ch: mio::Sender<Msg>,
}

impl Clone for SendCh {
    fn clone(&self) -> SendCh {
        SendCh { ch: self.ch.clone() }
    }
}

impl SendCh {
    pub fn new(ch: mio::Sender<Msg>) -> SendCh {
        SendCh { ch: ch }
    }

    fn send(&self, msg: Msg) -> Result<()> {
        try!(send_msg(&self.ch, msg));
        Ok(())
    }

    pub fn kill(&self) -> Result<()> {
        try!(self.send(Msg::Quit));
        Ok(())
    }

    pub fn write_data(&self, token: Token, data: ConnData) -> Result<()> {
        try!(self.send(Msg::WriteData {
            token: token,
            data: data,
        }));

        Ok(())
    }

    pub fn send_peer(&self, addr: String, data: ConnData) -> Result<()> {
        try!(self.send(Msg::SendPeer {
            addr: addr,
            data: data,
        }));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use mio::{EventLoop, Handler};

    use super::*;

    struct SenderHandler;

    impl Handler for SenderHandler {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<SenderHandler>, msg: Msg) {
            if let Msg::Quit = msg {
                event_loop.shutdown()
            }
        }
    }

    #[test]
    fn test_sender() {
        let mut event_loop = EventLoop::new().unwrap();
        let sender = SendCh::new(event_loop.channel());
        let h = thread::spawn(move || {
            event_loop.run(&mut SenderHandler).unwrap();
        });

        sender.kill().unwrap();

        h.join().unwrap();
    }
}
