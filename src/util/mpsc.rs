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

use crossbeam_channel;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError};
use std::sync::Arc;
use std::time::Duration;

struct State {
    sender_cnt: AtomicIsize,
    alive: AtomicBool,
}

impl State {
    fn new() -> State {
        State {
            sender_cnt: AtomicIsize::new(1),
            alive: AtomicBool::new(true),
        }
    }

    #[inline]
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
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
        let res = self.state.sender_cnt.fetch_add(-1, Ordering::AcqRel);
        if res == 1 {
            self.close();
        }
    }
}

pub struct Receiver<T> {
    receiver: crossbeam_channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> Sender<T> {
    #[inline]
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if self.state.is_alive() {
            self.sender.send(t);
            return Ok(());
        }
        Err(SendError(t))
    }

    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        if self.state.is_alive() {
            crossbeam_channel::select! {
                send(self.sender, t) => {},
                default => return Err(TrySendError::Full(t)),
            }
            Ok(())
        } else {
            Err(TrySendError::Disconnected(t))
        }
    }

    #[inline]
    pub fn close(&self) {
        self.state.alive.store(false, Ordering::Release);
    }

    #[inline]
    pub fn is_alive(&self) -> bool {
        self.state.is_alive()
    }
}

impl<T> Receiver<T> {
    #[inline]
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

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
                if self.state.is_alive() {
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
        self.state.alive.store(false, Ordering::Release);
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

const CHECK_INTERVAL: usize = 8;

/// A sender of channel that limits the maximun pending messages count loosely.
pub struct LooseBoundedSender<T> {
    sender: Sender<T>,
    tried_cnt: Cell<usize>,
    limit: usize,
}

impl<T> LooseBoundedSender<T> {
    #[inline]
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Send a message regardless its capacity limit.
    #[inline]
    pub fn force_send(&self, t: T) -> Result<(), SendError<T>> {
        self.tried_cnt.update(|t| t + 1);
        self.sender.send(t)
    }

    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        if self.tried_cnt.get() < CHECK_INTERVAL {
            self.force_send(t)
                .map_err(|SendError(t)| TrySendError::Disconnected(t))
        } else if self.sender.sender.len() < self.limit {
            self.tried_cnt.set(0);
            self.force_send(t)
                .map_err(|SendError(t)| TrySendError::Disconnected(t))
        } else {
            Err(TrySendError::Full(t))
        }
    }

    #[inline]
    pub fn close(&self) {
        self.sender.close();
    }

    #[inline]
    pub fn is_alive(&self) -> bool {
        self.sender.state.is_alive()
    }
}

impl<T> Clone for LooseBoundedSender<T> {
    #[inline]
    fn clone(&self) -> LooseBoundedSender<T> {
        LooseBoundedSender {
            sender: self.sender.clone(),
            tried_cnt: self.tried_cnt.clone(),
            limit: self.limit,
        }
    }
}

pub fn loose_bounded<T>(cap: usize) -> (LooseBoundedSender<T>, Receiver<T>) {
    let (sender, receiver) = unbounded();
    (
        LooseBoundedSender {
            sender,
            tried_cnt: Cell::new(0),
            limit: cap,
        },
        receiver,
    )
}

#[cfg(test)]
mod tests {
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
    fn test_loose() {
        let (tx, rx) = super::loose_bounded(10);
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

        let (tx, rx) = super::loose_bounded(10);
        tx.try_send(2).unwrap();
        drop(tx);
        assert_eq!(rx.recv(), Ok(2));
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)),
            Err(RecvTimeoutError::Disconnected)
        );

        let (tx, rx) = super::loose_bounded(10);
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
