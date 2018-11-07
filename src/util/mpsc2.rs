// Copyright 2018 PingCAP, Inc.
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
#![cfg_attr(feature = "cargo-clippy", allow(cast_ptr_alignment))]

use std::mem;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
pub use std::sync::mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError};
use std::sync::Arc;
use std::time::Duration;

use crossbeam::sync::AtomicOption;
use crossbeam_channel;
use futures::task::{self, Task};
use futures::{Async, Poll, Stream};

const NOTIFY_BATCH_SIZE: usize = 8;
const MAX_BATCH_SIZE: usize = 32;

pub struct State {
    sender_cnt: AtomicIsize,
    receiver_cnt: AtomicIsize,
    // Because we don't support sending messages asychrounously, so
    // only recv_task needs to be kept.
    recv_task: AtomicOption<Task>,
    // Sender sends something, but has not notified receiver.
    un_notified: AtomicUsize,
    // If external_notifier is registered, the receiver can't poll the channel.
    external_notifier: AtomicBool,
}

impl State {
    fn new() -> State {
        State {
            sender_cnt: AtomicIsize::new(1),
            receiver_cnt: AtomicIsize::new(1),
            recv_task: AtomicOption::new(),
            un_notified: AtomicUsize::new(0),
            external_notifier: AtomicBool::new(false),
        }
    }

    #[inline]
    fn is_receiver_closed(&self) -> bool {
        self.receiver_cnt.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn is_sender_closed(&self) -> bool {
        self.sender_cnt.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn inc_send_and_maybe_notify(&self) {
        let un_notified = self.un_notified.fetch_add(1, Ordering::SeqCst);
        if un_notified >= NOTIFY_BATCH_SIZE - 1 {
            self.do_notify();
        }
    }

    #[inline]
    fn do_notify(&self) {
        let task = self.recv_task.take(Ordering::AcqRel);
        if let Some(t) = task {
            self.un_notified.store(0, Ordering::SeqCst);
            t.notify();
        }
    }

    #[inline]
    pub fn external_notify(&self) {
        if !self
            .external_notifier
            .compare_and_swap(true, false, Ordering::SeqCst)
        {
            unreachable!("external_notifier must be true");
        }
        self.do_notify();
    }
}

pub struct Sender<T> {
    sender: crossbeam_channel::Sender<T>,
    state: Arc<State>,
}

impl<T> Clone for Sender<T> {
    #[inline]
    fn clone(&self) -> Sender<T> {
        self.state.sender_cnt.fetch_add(1, Ordering::AcqRel);
        Sender {
            sender: self.sender.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    #[inline]
    fn drop(&mut self) {
        self.state.sender_cnt.fetch_add(-1, Ordering::AcqRel);
        if self.state.is_sender_closed() {
            self.state.do_notify();
        }
    }
}

pub struct Receiver<T> {
    receiver: crossbeam_channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> Sender<T> {
    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if !self.state.is_receiver_closed() {
            self.sender.send(t);
            self.state.inc_send_and_maybe_notify();
            return Ok(());
        }
        Err(SendError(t))
    }

    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        if !self.state.is_receiver_closed() {
            crossbeam_channel::select! {
                send(self.sender, t) => {},
                default => return Err(TrySendError::Full(t)),
            }
            self.state.inc_send_and_maybe_notify();
            Ok(())
        } else {
            Err(TrySendError::Disconnected(t))
        }
    }

    #[inline]
    pub fn get_notifier(&self) -> Option<Arc<State>> {
        if !self
            .state
            .external_notifier
            .compare_and_swap(false, true, Ordering::SeqCst)
        {
            return Some(Arc::clone(&self.state));
        }
        None
    }
}

impl<T> Receiver<T> {
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.receiver.recv() {
            Some(t) => Ok(t),
            None => Err(RecvError),
        }
    }

    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.receiver.try_recv() {
            Some(t) => Ok(t),
            None => {
                if !self.state.is_sender_closed() {
                    return Err(TryRecvError::Empty);
                }
                self.receiver.try_recv().ok_or(TryRecvError::Disconnected)
            }
        }
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        crossbeam_channel::select! {
            recv(self.receiver, task) => match task {
                Some(t) => Ok(t),
                None => Err(RecvTimeoutError::Disconnected),
            }
            recv(crossbeam_channel::after(timeout)) => Err(RecvTimeoutError::Timeout),
        }
    }

    // For implement more receivers based on futures. Must be called in poll.
    // The return value indicates the the caller can give up the poll or not.
    #[inline]
    pub fn yield_poll(&self) -> bool {
        let recv_task = &self.state.recv_task;
        recv_task.swap(task::current(), Ordering::AcqRel).is_some()
    }
}

impl<T> Drop for Receiver<T> {
    #[inline]
    fn drop(&mut self) {
        self.state.receiver_cnt.fetch_add(-1, Ordering::AcqRel);
    }
}

#[inline]
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State::new());
    let (sender, receiver) = crossbeam_channel::unbounded();
    (
        Sender {
            sender,
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

#[inline]
pub fn batch_unbounded<T>() -> (Sender<T>, BatchReceiver<T>) {
    let (tx, rx) = unbounded();
    let rx = BatchReceiver {
        rx,
        buf: Vec::with_capacity(MAX_BATCH_SIZE),
    };
    (tx, rx)
}

#[inline]
pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State::new());
    let (sender, receiver) = crossbeam_channel::bounded(cap);
    (
        Sender {
            sender,
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

impl<T> Stream for Receiver<T> {
    type Error = ();
    type Item = T;

    fn poll(&mut self) -> Poll<Option<T>, ()> {
        match self.try_recv() {
            Ok(m) => Ok(Async::Ready(Some(m))),
            Err(TryRecvError::Empty) => {
                let t = self.state.recv_task.swap(task::current(), Ordering::AcqRel);
                if t.is_some() {
                    return Ok(Async::NotReady);
                }
                // Note: If there is a message or all the senders are dropped after
                // polling but before task is set, `t` should be None.
                self.poll()
            }
            Err(TryRecvError::Disconnected) => Ok(Async::Ready(None)),
        }
    }
}

pub struct BatchReceiver<T> {
    rx: Receiver<T>,
    buf: Vec<T>,
}

impl<T> Stream for BatchReceiver<T> {
    type Error = ();
    type Item = Vec<T>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, ()> {
        let finished = loop {
            match self.rx.try_recv() {
                Err(TryRecvError::Disconnected) => break true,
                Ok(m) => {
                    self.buf.push(m);
                    if self.buf.len() >= MAX_BATCH_SIZE {
                        break false;
                    }
                }
                Err(TryRecvError::Empty) => if self.rx.yield_poll() {
                    break false;
                },
            }
        };

        if self.buf.is_empty() && finished {
            return Ok(Async::Ready(None));
        } else if self.buf.is_empty() {
            return Ok(Async::NotReady);
        }
        let msgs = mem::replace(&mut self.buf, Vec::with_capacity(MAX_BATCH_SIZE));
        Ok(Async::Ready(Some(msgs)))
    }
}
