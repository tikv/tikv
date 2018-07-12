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

/*!

This module provides an implementation of mpsc channel based on
crossbeam_channel. Comparing to the crossbeam_channel, this implementation
supports closed detection and try operations.

*/

#![cfg_attr(feature = "cargo-clippy", allow(cast_ptr_alignment))]

use crossbeam::sync::AtomicOption;
use crossbeam_channel;
use futures::task::{self, Task};
use futures::{Async, Poll, Stream};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError};
use std::sync::Arc;
use std::time::Duration;

struct State {
    sender_cnt: AtomicIsize,
    receiver_cnt: AtomicIsize,
    // Because we don't support sending messages asychrounously, so
    // only recv_task needs to be kept.
    recv_task: AtomicOption<Task>,
}

impl State {
    fn new() -> State {
        State {
            sender_cnt: AtomicIsize::new(1),
            receiver_cnt: AtomicIsize::new(1),
            recv_task: AtomicOption::new(),
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
            self.notify();
        }
    }
}

pub struct Receiver<T> {
    receiver: crossbeam_channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> Sender<T> {
    #[inline]
    fn notify(&self) {
        let task = self.state.recv_task.take(Ordering::AcqRel);
        if let Some(t) = task {
            t.notify();
        }
    }

    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if !self.state.is_receiver_closed() {
            self.sender.send(t);
            self.notify();
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
            self.notify();
            Ok(())
        } else {
            Err(TrySendError::Disconnected(t))
        }
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

const CHECK_INTERVAL: usize = 8;

/// A sender of channel that limits the maximun pending messages count loosely.
pub struct LooseBoundedSender<T> {
    sender: Sender<T>,
    tried_cnt: usize,
    limit: usize,
}

impl<T> LooseBoundedSender<T> {
    /// Send a message regardless its capacity limit.
    #[inline]
    pub fn force_send(&mut self, t: T) -> Result<(), SendError<T>> {
        self.tried_cnt += 1;
        self.sender.send(t)
    }

    #[inline]
    pub fn try_send(&mut self, t: T) -> Result<(), TrySendError<T>> {
        if self.tried_cnt < CHECK_INTERVAL {
            self.force_send(t)
                .map_err(|SendError(t)| TrySendError::Disconnected(t))
        } else if self.sender.sender.len() < self.limit {
            self.tried_cnt = 0;
            self.force_send(t)
                .map_err(|SendError(t)| TrySendError::Disconnected(t))
        } else {
            Err(TrySendError::Full(t))
        }
    }
}

impl<T> Clone for LooseBoundedSender<T> {
    #[inline]
    fn clone(&self) -> LooseBoundedSender<T> {
        LooseBoundedSender {
            sender: self.sender.clone(),
            tried_cnt: self.tried_cnt,
            limit: self.limit,
        }
    }
}

pub fn loose_bounded<T>(cap: usize) -> (LooseBoundedSender<T>, Receiver<T>) {
    let (sender, receiver) = unbounded();
    (
        LooseBoundedSender {
            sender,
            tried_cnt: 0,
            limit: cap,
        },
        receiver,
    )
}

#[cfg(test)]
mod tests {
    use futures::sync::mpsc;
    use futures::{Future, Sink, Stream};
    use std::sync::mpsc::*;
    use std::thread;
    use std::time::*;

    #[test]
    fn test_bounded() {
        let (tx, rx) = super::bounded::<u64>(10);
        tx.try_send(1).unwrap();
        for i in 2..11 {
            tx.clone().send(i).unwrap();
        }
        assert_eq!(tx.try_send(11), Err(TrySendError::Full(11)));

        assert_eq!(rx.try_recv(), Ok(1));
        for i in 2..11 {
            assert_eq!(rx.recv(), Ok(i));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let timer = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        );
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);

        drop(rx);
        assert_eq!(tx.send(2), Err(SendError(2)));
        assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));

        let (tx, rx) = super::bounded::<u64>(10);
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Disconnected)
        );

        let (tx1, rx1) = super::bounded::<u64>(10);
        let (tx2, rx2) = super::bounded::<u64>(0);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx1.send(10).unwrap();
            thread::sleep(Duration::from_millis(100));
            assert_eq!(rx2.recv(), Ok(2));
        });
        let timer = Instant::now();
        assert_eq!(rx1.recv(), Ok(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);
        let timer = Instant::now();
        tx2.send(2).unwrap();
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(50), "{:?}", elapsed);
    }

    #[test]
    fn test_unbounded() {
        let (tx, rx) = super::unbounded::<u64>();
        tx.try_send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let timer = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        );
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);

        drop(rx);
        assert_eq!(tx.send(2), Err(SendError(2)));
        assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));

        let (tx, rx) = super::unbounded::<u64>();
        drop(tx);
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Disconnected)
        );

        let (tx, rx) = super::unbounded::<u64>();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.send(10).unwrap();
        });
        let timer = Instant::now();
        assert_eq!(rx.recv(), Ok(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);
    }

    #[test]
    fn test_notify() {
        let (tx1, rx1) = super::unbounded();
        let (tx2, rx2) = mpsc::unbounded();
        let mut rx2 = rx2.wait();
        let j = thread::spawn(move || {
            rx1.map_err(|_| ())
                .forward(tx2.sink_map_err(|_| ()))
                .wait()
                .unwrap();
        });
        tx1.send(2).unwrap();
        assert_eq!(rx2.next().unwrap(), Ok(2));
        for i in 0..100 {
            tx1.send(i).unwrap();
        }
        for i in 0..100 {
            assert_eq!(rx2.next().unwrap(), Ok(i));
        }
        thread::spawn(move || {
            for i in 100..10000 {
                tx1.send(i).unwrap();
            }
        });
        for i in 100..10000 {
            assert_eq!(rx2.next().unwrap(), Ok(i));
        }
        // j should automatically stop when tx1 is dropped.
        j.join().unwrap();
    }

    #[test]
    fn test_loose() {
        let (mut tx, rx) = super::loose_bounded(10);
        tx.try_send(1).unwrap();
        for i in 2..11 {
            tx.clone().try_send(i).unwrap();
        }
        for i in 1..super::CHECK_INTERVAL {
            tx.force_send(i).unwrap();
        }
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));
        tx.force_send(5).unwrap();
        assert_eq!(tx.try_send(6), Err(TrySendError::Full(6)));

        assert_eq!(rx.try_recv(), Ok(1));
        for i in 2..11 {
            assert_eq!(rx.recv(), Ok(i));
        }
        for i in 1..super::CHECK_INTERVAL {
            assert_eq!(rx.try_recv(), Ok(i));
        }
        assert_eq!(rx.try_recv(), Ok(5));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        let timer = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Timeout)
        );
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);

        tx.force_send(1).unwrap();
        drop(rx);
        assert_eq!(tx.force_send(2), Err(SendError(2)));
        assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));
        for _ in 0..super::CHECK_INTERVAL {
            assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));
        }

        let (mut tx, rx) = super::loose_bounded(10);
        tx.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Disconnected)
        );

        let (mut tx, rx) = super::loose_bounded(10);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.try_send(10).unwrap();
        });
        let timer = Instant::now();
        assert_eq!(rx.recv(), Ok(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);
    }
}
