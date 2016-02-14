use std::sync::Mutex;
use std::boxed::{Box, FnBox};
use std::fmt;

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
    // We can't send protobuf message directly, the generated struct has a cell field which
    // doesn't support send + sync, so using a mutex to wrap it for send + sync.
    // Although it looks ugly, it is still more convenient than decoding many times
    // in different threads.
    RaftMessage(Mutex<RaftMessage>),
    RaftCommand {
        request: Mutex<RaftCommandRequest>,
        callback: Callback,
    },
}

impl fmt::Debug for Msg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(fmt, "Quit"),
            Msg::RaftBaseTick => write!(fmt, "Raft Base Tick"),
            Msg::RaftMessage(_) => write!(fmt, "Raft Message"),
            Msg::RaftCommand{..} => write!(fmt, "Raft Command"),
        }
    }
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
