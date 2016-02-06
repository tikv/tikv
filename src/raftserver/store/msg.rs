use mio;
use bytes::ByteBuf;

use raftserver::{Result, send_msg};

pub enum Msg {
    Quit,

    // For tick
    RaftBaseTick,

    // For notify
    RaftMessage(ByteBuf),
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
}
