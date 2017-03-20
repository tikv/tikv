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


use std::{thread, error, io};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::mpsc;
use std::time::Duration;
use super::metrics::*;

use mio;

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

#[derive(Debug)]
pub enum NotifyError<T> {
    Full(T),
    Closed(Option<T>),
    Io(io::Error),
}

pub trait Sender<T>: Clone {
    fn send(&self, t: T) -> Result<(), NotifyError<T>>;
}

impl<T> Sender<T> for mio::Sender<T> {
    fn send(&self, t: T) -> Result<(), NotifyError<T>> {
        match mio::Sender::send(self, t) {
            Ok(()) => Ok(()),
            Err(mio::NotifyError::Closed(t)) => Err(NotifyError::Closed(t)),
            Err(mio::NotifyError::Full(t)) => Err(NotifyError::Full(t)),
            Err(mio::NotifyError::Io(e)) => Err(NotifyError::Io(e)),
        }
    }
}

impl<T> Sender<T> for mpsc::SyncSender<T> {
    fn send(&self, t: T) -> Result<(), NotifyError<T>> {
        match mpsc::SyncSender::try_send(self, t) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(t)) => Err(NotifyError::Closed(Some(t))),
            Err(mpsc::TrySendError::Full(t)) => Err(NotifyError::Full(t)),
        }
    }
}

/// A channel that handle error with retry automatically.
pub struct RetryableSendCh<T, C: Sender<T>> {
    ch: C,
    name: &'static str,

    marker: PhantomData<T>,
}

// We only care about self.ch. When using built-in derive, the T will be
// also taken into consideration.
unsafe impl<T, C: Sender<T> + Send> Send for RetryableSendCh<T, C> {}
unsafe impl<T, C: Sender<T> + Sync> Sync for RetryableSendCh<T, C> {}

impl<T: Debug, C: Sender<T>> RetryableSendCh<T, C> {
    pub fn new(ch: C, name: &'static str) -> RetryableSendCh<T, C> {
        RetryableSendCh {
            ch: ch,
            name: name,
            marker: Default::default(),
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

impl<T, C: Sender<T>> Clone for RetryableSendCh<T, C> {
    fn clone(&self) -> RetryableSendCh<T, C> {
        RetryableSendCh {
            ch: self.ch.clone(),
            name: self.name,
            marker: Default::default(),
        }
    }
}

pub type SendCh<T> = RetryableSendCh<T, mio::Sender<T>>;
pub type SyncSendCh<T> = RetryableSendCh<T, mpsc::SyncSender<T>>;

#[cfg(test)]
mod tests {
    use std::thread;
    use std::sync::mpsc::Receiver;
    use std::time::Duration;

    use mio::{EventLoop, Handler, EventLoopBuilder};

    use super::*;

    #[derive(Debug)]
    enum Msg {
        Quit,
        Stop,
        Sleep(u64),
    }

    struct SenderHandler<S: Sender<Msg>> {
        ch: RetryableSendCh<Msg, S>,
    }

    impl<S: Sender<Msg>> SenderHandler<S> {
        fn run(&mut self, recv: Receiver<Msg>) {
            while let Ok(msg) = recv.recv() {
                if !self.on_msg(msg) {
                    break;
                }
            }
        }

        fn on_msg(&mut self, msg: Msg) -> bool {
            match msg {
                Msg::Quit => return false,
                Msg::Stop => self.ch.try_send(Msg::Quit).unwrap(),
                Msg::Sleep(millis) => thread::sleep(Duration::from_millis(millis)),
            }
            true
        }
    }

    impl<S: Sender<Msg>> Handler for SenderHandler<S> {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<SenderHandler<S>>, msg: Msg) {
            if !self.on_msg(msg) {
                event_loop.shutdown();
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

    #[test]
    fn test_sync_sendch() {
        let (tx, rx) = mpsc::sync_channel(10);
        let ch = SyncSendCh::new(tx, "test");
        let _ch = ch.clone();
        let h = thread::spawn(move || {
            let mut handler = SenderHandler { ch: _ch };
            handler.run(rx);
        });

        ch.try_send(Msg::Stop).unwrap();

        h.join().unwrap();
    }

    #[test]
    fn test_sync_sendch_full() {
        let (tx, rx) = mpsc::sync_channel(2);
        let ch = SyncSendCh::new(tx, "test");
        let _ch = ch.clone();
        let h = thread::spawn(move || {
            let mut handler = SenderHandler { ch: _ch };
            handler.run(rx);
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
