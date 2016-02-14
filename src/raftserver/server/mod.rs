use std::string::String;

use bytes::{Buf, ByteBuf};
use mio::{self, Token};

use raftserver::{Result, send_msg};
use util::codec::rpc;

mod bench;
mod conn;
mod server;
pub mod handler;
pub mod run;

pub use self::run::Runner;
pub use self::handler::ServerHandler;

const SERVER_TOKEN: Token = Token(1);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);
const INVALID_TOKEN: Token = Token(0);
const DEFAULT_BASE_TICK_MS: u64 = 100;

pub struct ConnData {
    msg_id: u64,
    data: ByteBuf,
}

impl ConnData {
    pub fn from_string<S: Into<String>>(msg_id: u64, data: S) -> ConnData {
        ConnData {
            msg_id: msg_id,
            data: ByteBuf::from_slice(data.into().as_bytes()),
        }
    }

    pub fn encode_to_buf(&self) -> ByteBuf {
        let mut buf = ByteBuf::mut_with_capacity(rpc::MSG_HEADER_LEN + self.data.bytes().len());

        // Must ok here
        rpc::encode_data(&mut buf, self.msg_id, self.data.bytes()).unwrap();

        buf.flip()
    }
}

pub enum TimerMsg {
    // None is just for test, we will remove this later.
    None,
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
    // Tick is for base internal tick message.
    Tick,
    // Timer is for custom timeout message.
    Timer {
        delay: u64,
        msg: TimerMsg,
    },
    // Send data to remote peer with address.
    SendPeer {
        addr: String,
        data: ConnData,
    },
}

#[derive(Debug)]
pub struct Sender {
    sender: mio::Sender<Msg>,
}

impl Clone for Sender {
    fn clone(&self) -> Sender {
        Sender { sender: self.sender.clone() }
    }
}

impl Sender {
    pub fn new(sender: mio::Sender<Msg>) -> Sender {
        Sender { sender: sender }
    }

    fn send(&self, msg: Msg) -> Result<()> {
        try!(send_msg(&self.sender, msg));
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

    pub fn timeout_ms(&self, delay: u64, m: TimerMsg) -> Result<()> {
        try!(self.send(Msg::Timer {
            delay: delay,
            msg: m,
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
            match msg {
                Msg::Quit => event_loop.shutdown(),
                _ => {}
            }
        }
    }

    #[test]
    fn test_sender() {
        let mut event_loop = EventLoop::new().unwrap();
        let sender = Sender::new(event_loop.channel());
        let h = thread::spawn(move || {
            event_loop.run(&mut SenderHandler).unwrap();
        });

        for _ in 1..10000 {
            sender.timeout_ms(100, TimerMsg::None).unwrap();
        }

        sender.kill().unwrap();

        h.join().unwrap();
    }
}
