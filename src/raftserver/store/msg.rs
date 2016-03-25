use std::boxed::{Box, FnBox};
use std::sync::{Arc, Mutex, Condvar};
use std::fmt;
use std::time::Duration;

use mio;

use raftserver::{Result, send_msg, Error};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};

pub type Callback = Box<FnBox(RaftCmdResponse) -> Result<()> + Send>;

#[derive(Debug)]
pub enum Tick {
    Raft,
    RaftLogGc,
    SplitRegionCheck,
    ReplicaCheck,
}

pub enum Msg {
    Quit,

    // For notify.
    RaftMessage(RaftMessage),
    RaftCmd {
        request: RaftCmdRequest,
        callback: Callback,
    },

    // For split check
    SplitCheckResult {
        region_id: u64,
        split_key: Vec<u8>,
    },
}

impl fmt::Debug for Msg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(fmt, "Quit"),
            Msg::RaftMessage(_) => write!(fmt, "Raft Message"),
            Msg::RaftCmd{..} => write!(fmt, "Raft Command"),
            Msg::SplitCheckResult{..} => write!(fmt, "Split Check Result"),
        }
    }
}

// Send the request and wait the response until timeout.
// Use Condvar to support call timeout. if timeout, return None.
// We should know that even timeout happens, the command may still
// be handled in store later.
pub fn call_command(sendch: &SendCh,
                    request: RaftCmdRequest,
                    timeout: Duration)
                    -> Result<RaftCmdResponse> {
    let resp: Option<RaftCmdResponse> = None;
    let pair = Arc::new((Mutex::new(resp), Condvar::new()));
    let pair2 = pair.clone();

    try!(sendch.send(Msg::RaftCmd {
        request: request,
        callback: box move |resp| {
            let &(ref lock, ref cvar) = &*pair2;
            let mut v = lock.lock().unwrap();
            *v = Some(resp);
            cvar.notify_one();
            Ok(())
        },
    }));

    let &(ref lock, ref cvar) = &*pair;
    let mut v = lock.lock().unwrap();
    while v.is_none() {
        let (resp, timeout_res) = cvar.wait_timeout(v, timeout).unwrap();
        if timeout_res.timed_out() {
            return Err(Error::Timeout(format!("request timeout for {:?}", timeout)));
        }

        v = resp
    }

    Ok(v.take().unwrap())
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

    pub fn send(&self, msg: Msg) -> Result<()> {
        try!(send_msg(&self.ch, msg));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::mpsc::channel;
    use std::time::Duration;

    use mio::{EventLoop, Handler};

    use super::*;
    use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
    use raftserver::Error;

    struct TestHandler;

    impl Handler for TestHandler {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
            match msg {
                Msg::Quit => event_loop.shutdown(),
                Msg::RaftCmd{callback, request} => {
                    // a trick for test timeout.
                    if request.get_header().get_region_id() == u64::max_value() {
                        thread::sleep(Duration::from_millis(100));
                    }
                    callback.call_box((RaftCmdResponse::new(),)).unwrap()
                }
                // we only test above message types, others panic.
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_sender() {
        let mut event_loop = EventLoop::new().unwrap();
        let sendch = &SendCh::new(event_loop.channel());

        let t = thread::spawn(move || {
            event_loop.run(&mut TestHandler).unwrap();
        });

        let (tx, rx) = channel();
        let cmd = Msg::RaftCmd {
            request: RaftCmdRequest::new(),
            callback: box move |_| {
                tx.send(1).unwrap();
                Ok(())
            },
        };
        sendch.send(cmd).unwrap();

        rx.recv().unwrap();

        let mut request = RaftCmdRequest::new();
        request.mut_header().set_region_id(u64::max_value());
        assert!(call_command(sendch, request.clone(), Duration::from_millis(500)).is_ok());
        match call_command(sendch, request.clone(), Duration::from_millis(10)) {
            Err(Error::Timeout(_)) => {}
            _ => panic!("should failed with timeout"),
        }

        sendch.send(Msg::Quit).unwrap();

        t.join().unwrap();
    }
}
