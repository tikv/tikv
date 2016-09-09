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


use std::thread;
use std::io;
use std::fmt::Debug;
use std::time::Duration;

use mio::{Sender, NotifyError};

pub const MAX_SEND_RETRY_CNT: usize = 5;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Discard(reason: String) {
            description("message is discard")
            display("{}", reason)
        }
        Io(err: io::Error) {
            from(err)
            cause(err)
            description(err.description())
            display("io error {:?}", err)
        }
    }
}

impl<T: Debug> From<NotifyError<T>> for Error {
    fn from(e: NotifyError<T>) -> Error {
        match e {
            // ALLERT!! make cause sensitive data leak.
            NotifyError::Closed(m) => {
                Error::Discard(format!("Failed to send {:?} due to closed", m))
            }
            NotifyError::Full(m) => Error::Discard(format!("Failed to send {:?} due to full", m)),
            NotifyError::Io(e) => Error::Io(e),
        }
    }
}

pub struct SendCh<T> {
    ch: Sender<T>,
}

impl<T: Debug> SendCh<T> {
    pub fn new(ch: Sender<T>) -> SendCh<T> {
        SendCh { ch: ch }
    }

    pub fn send(&self, t: T) -> Result<(), Error> {
        self.send_with_retry(t, 1)
    }

    pub fn send_with_retry(&self, mut t: T, mut try_times: usize) -> Result<(), Error> {
        loop {
            t = match self.ch.send(t) {
                Ok(_) => return Ok(()),
                Err(NotifyError::Full(m)) => {
                    if try_times <= 1 {
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
        SendCh { ch: self.ch.clone() }
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

        ch.send_with_retry(Msg::Sleep(1000), MAX_SEND_RETRY_CNT).unwrap();
        ch.send_with_retry(Msg::Stop, MAX_SEND_RETRY_CNT).unwrap();
        ch.send_with_retry(Msg::Stop, MAX_SEND_RETRY_CNT).unwrap();
        assert!(ch.send_with_retry(Msg::Stop, MAX_SEND_RETRY_CNT).is_err());

        h.join().unwrap();
    }
}
