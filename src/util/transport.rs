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
use std::fmt::Debug;
use std::time::Duration;
use super::metrics::*;

use mio::{Sender, NotifyError};

const MAX_SEND_RETRY_CNT: usize = 5;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Discard(reason: String) {
            description("message is discarded")
            display("{}", reason)
        }
        Closed {
            description("channel is closed")
            display("channel is closed")
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

impl<T: Debug> From<NotifyError<T>> for Error {
    fn from(e: NotifyError<T>) -> Error {
        match e {
            // ALLERT!! May cause sensitive data leak.
            NotifyError::Full(m) => Error::Discard(format!("Failed to send {:?} due to full", m)),
            NotifyError::Closed(..) => Error::Closed,
            _ => box_err!("{:?}", e),
        }
    }
}

pub struct SendCh<T> {
    ch: Sender<T>,
    name: &'static str,
}

impl<T: Debug> SendCh<T> {
    pub fn new(ch: Sender<T>, name: &'static str) -> SendCh<T> {
        SendCh {
            ch: ch,
            name: name,
        }
    }

    /// Try send t with default try times.
    pub fn send(&self, t: T) -> Result<(), Error> {
        self.send_with_try_times(t, MAX_SEND_RETRY_CNT)
    }

    pub fn try_send(&self, t: T) -> Result<(), Error> {
        self.send_with_try_times(t, 1)
    }

    fn send_with_try_times(&self, mut t: T, mut try_times: usize) -> Result<(), Error> {
        loop {
            t = match self.ch.send(t) {
                Ok(_) => return Ok(()),
                Err(NotifyError::Full(m)) => {
                    if try_times <= 1 {
                        CHANNEL_FULL_COUNTER_VEC.with_label_values(&[self.name]).inc();
                        return Err(NotifyError::Full(m).into());
                    }
                    try_times -= 1;
                    m
                }
                Err(e) => return Err(e.into()),
            };

            // ALLERT!! make cause sensitive data leak.
            warn!("notify queue is full, sleep and retry sending {:?}", t);
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl<T> Clone for SendCh<T> {
    fn clone(&self) -> SendCh<T> {
        SendCh {
            ch: self.ch.clone(),
            name: self.name,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use mio::{EventLoop, Handler, EventLoopBuilder};

    use super::*;

    #[derive(Debug)]
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
                Msg::Stop => self.ch.try_send(Msg::Quit).unwrap(),
                Msg::Sleep(millis) => thread::sleep(Duration::from_millis(millis)),
            }
        }
    }

    #[test]
    fn test_sendch() {
        let mut event_loop = EventLoop::new().unwrap();
        let ch = SendCh::new(event_loop.channel(), "test");
        let _ch = ch.clone();
        let h = thread::spawn(move || {
            let mut sender = SenderHandler { ch: _ch };
            event_loop.run(&mut sender).unwrap();
        });

        ch.try_send(Msg::Stop).unwrap();

        h.join().unwrap();
    }

    #[test]
    fn test_sendch_full() {
        let mut builder = EventLoopBuilder::new();
        builder.notify_capacity(2);
        let mut event_loop = builder.build().unwrap();
        let ch = SendCh::new(event_loop.channel(), "test");
        let _ch = ch.clone();
        let h = thread::spawn(move || {
            let mut sender = SenderHandler { ch: _ch };
            event_loop.run(&mut sender).unwrap();
        });

        ch.send(Msg::Sleep(1000)).unwrap();
        ch.send(Msg::Stop).unwrap();
        ch.send(Msg::Stop).unwrap();
        match ch.send(Msg::Stop) {
            Err(Error::Discard(_)) => {}
            res => panic!("expect discard error, but found: {:?}", res),
        }

        h.join().unwrap();
    }
}
