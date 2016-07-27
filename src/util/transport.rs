// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


use std::{thread, error};
use std::time::Duration;

use mio::{Sender, NotifyError};

const MAX_SEND_RETRY_CNT: usize = 5;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

pub struct SendCh<T> {
    ch: Sender<T>,
}

impl<T> SendCh<T> {
    pub fn new(ch: Sender<T>) -> SendCh<T> {
        SendCh { ch: ch }
    }

    pub fn send(&self, mut t: T) -> Result<(), Error> {
        let mut try_cnt = 0;
        while let Err(e) = self.ch.send(t) {
            t = match e {
                NotifyError::Full(m) => m,
                _ => return Err(box_err!("{:?}", e)),
            };

            warn!("notify queue is full, sleep and retry");
            try_cnt += 1;
            if try_cnt >= MAX_SEND_RETRY_CNT {
                return Err(box_err!("failed to send msg after {} tries", MAX_SEND_RETRY_CNT));
            }
            thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }
}

impl<T> Clone for SendCh<T> {
    fn clone(&self) -> SendCh<T> {
        SendCh { ch: self.ch.clone() }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use mio::{EventLoop, Handler, EventLoopBuilder};

    use super::*;

    enum Msg {
        Quit,
        Stop,
        Sleep(u64),
    }

    struct SenderHandler {
        ch: SendCh<Msg>,
    }

    impl Handler for SenderHandler {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<SenderHandler>, msg: Msg) {
            match msg {
                Msg::Quit => event_loop.shutdown(),
                Msg::Stop => self.ch.send(Msg::Quit).unwrap(),
                Msg::Sleep(millis) => thread::sleep(Duration::from_millis(millis)),
            }
        }
    }

    #[test]
    fn test_sendch() {
        let mut event_loop = EventLoop::new().unwrap();
        let ch = SendCh::new(event_loop.channel());
        let _ch = ch.clone();
        let h = thread::spawn(move || {
            let mut sender = SenderHandler { ch: _ch };
            event_loop.run(&mut sender).unwrap();
        });

        ch.send(Msg::Stop).unwrap();

        h.join().unwrap();
    }

    #[test]
    fn test_sendch_full() {
        let mut builder = EventLoopBuilder::new();
        builder.notify_capacity(2);
        let mut event_loop = builder.build().unwrap();
        let ch = SendCh::new(event_loop.channel());
        let _ch = ch.clone();
        let h = thread::spawn(move || {
            let mut sender = SenderHandler { ch: _ch };
            event_loop.run(&mut sender).unwrap();
        });

        ch.send(Msg::Sleep(1000)).unwrap();
        ch.send(Msg::Stop).unwrap();
        ch.send(Msg::Stop).unwrap();
        assert!(ch.send(Msg::Stop).is_err());

        h.join().unwrap();
    }
}
