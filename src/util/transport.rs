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

use crossbeam::{SendError, TrySendError};
use prometheus::IntCounterVec;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

lazy_static! {
    pub static ref CHANNEL_FULL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_channel_full_total",
        "Total number of channel full errors.",
        &["type"]
    )
    .unwrap();
}

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
    }
}

impl<T: Debug> From<TrySendError<T>> for Error {
    #[inline]
    fn from(e: TrySendError<T>) -> Error {
        match e {
            // ALERT!! May cause sensitive data leak.
            TrySendError::Full(m) => Error::Discard(format!("Failed to send {:?} due to full", m)),
            TrySendError::Disconnected(..) => Error::Closed,
        }
    }
}

impl<T: Debug> From<SendError<T>> for Error {
    #[inline]
    fn from(_: SendError<T>) -> Error {
        // ALERT!! May cause sensitive data leak.
        Error::Closed
    }
}

pub trait Sender<T>: Clone {
    fn send(&self, t: T) -> Result<(), TrySendError<T>>;
}

impl<T> Sender<T> for mpsc::SyncSender<T> {
    fn send(&self, t: T) -> Result<(), TrySendError<T>> {
        match mpsc::SyncSender::try_send(self, t) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(t)) => Err(TrySendError::Disconnected(t)),
            Err(mpsc::TrySendError::Full(t)) => Err(TrySendError::Full(t)),
        }
    }
}

/// A channel that handles errors with retry automatically.
pub struct RetryableSendCh<T, C> {
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
            ch,
            name,
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

    pub fn into_inner(self) -> C {
        self.ch
    }

    fn send_with_try_times(&self, mut t: T, mut try_times: usize) -> Result<(), Error> {
        loop {
            t = match self.ch.send(t) {
                Ok(_) => return Ok(()),
                Err(TrySendError::Full(m)) => {
                    if try_times <= 1 {
                        CHANNEL_FULL_COUNTER_VEC
                            .with_label_values(&[self.name])
                            .inc();
                        return Err(TrySendError::Full(m).into());
                    }
                    try_times -= 1;
                    m
                }
                Err(e) => return Err(e.into()),
            };

            // ALERT!! make cause sensitive data leak.
            warn!("notify queue is full, sleep and retry sending"; "task" => ?t);
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

pub type SyncSendCh<T> = RetryableSendCh<T, mpsc::SyncSender<T>>;

#[cfg(test)]
mod tests {
    use std::sync::mpsc::Receiver;
    use std::thread;
    use std::time::Duration;

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
