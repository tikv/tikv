use std::boxed::{Box, FnBox};
use std::fmt;

use mio;

use raftserver::{Result, send_msg};
use proto::raft_serverpb::RaftMessage;
use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};

pub type Callback = Box<FnBox(RaftCommandResponse) -> Result<()> + Send>;

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

    pub fn send_quit(&self) -> Result<()> {
        self.send(Msg::Quit)
    }
}

#[cfg(test)]
mod tests {
    use mio::{EventLoop, Handler};
    use super::*;
    use std::thread;
    use std::sync::mpsc::channel;
    use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};
    use raftserver::Result;

    struct TestHandler;

    impl Handler for TestHandler {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
            match msg {
                Msg::Quit => event_loop.shutdown(),
                Msg::RaftCommand{callback, ..} => {
                    callback.call_box((RaftCommandResponse::new(),)).unwrap()
                }
                // we only test above message types, others panic.
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_sender() {
        let mut event_loop = EventLoop::new().unwrap();
        let sender = Sender::new(event_loop.channel());

        let t = thread::spawn(move || {
            event_loop.run(&mut TestHandler).unwrap();
        });

        let (tx, rx) = channel();
        sender.send_command(RaftCommandRequest::new(),
                            Box::new(move |resp: RaftCommandResponse| -> Result<()> {
                                tx.send(1).unwrap();
                                Ok(())
                            }))
              .unwrap();

        rx.recv().unwrap();

        sender.send_quit().unwrap();

        t.join().unwrap();
    }
}
