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
    RaftMessage(RaftMessage),
    RaftCommand {
        request: RaftCommandRequest,
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

    pub fn send_raft_msg(&self, msg: RaftMessage) -> Result<()> {
        self.send(Msg::RaftMessage(msg))
    }

    pub fn send_command(&self, msg: RaftCommandRequest, cb: Callback) -> Result<()> {
        self.send(Msg::RaftCommand {
            request: msg,
            callback: cb,
        })
    }
}
