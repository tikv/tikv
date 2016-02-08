use std::sync::Mutex;
use std::boxed::{Box, FnBox};

use mio;

use raftserver::{Result, send_msg};
use proto::raft_serverpb::RaftMessage;
use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};

pub type Callback = Box<FnBox(RaftCommandResponse) -> Result<()> + Send + Sync>;

pub enum Msg {
    Quit,

    // For tick
    RaftBaseTick,

    // For notify.
    // We can't send protobuf message directly, so using a mutex wraps it.
    // Although it looks ugly, it is still more convenient than decoding many times
    // in different threads.
    RaftMessage(Mutex<RaftMessage>),
    RaftCommand {
        request: Mutex<RaftCommandRequest>,
        callback: Callback,
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
}
