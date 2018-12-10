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
pub mod batch;

use crossbeam::channel::{
    self, RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError,
};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct State {
    sender_cnt: AtomicIsize,
    connected: AtomicBool,
}

impl State {
    fn new() -> State {
        State {
            sender_cnt: AtomicIsize::new(1),
            connected: AtomicBool::new(true),
        }
    }

    #[inline]
    fn is_sender_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }
}

/// A sender that can be closed.
///
/// Closed means that sender can no longer send out any messages after closing.
/// However, receiver may still block at receiving.
///
/// Note that a receiver should reports error in such case.
/// However, to fully implement a close mechanism, like waking up waiting
/// receivers, requires a cost of performance. And the mechanism is unnecessary
/// for current usage.
///
/// TODO: use builtin close when crossbeam-rs/crossbeam#236 is resolved.
pub struct Sender<T> {
    sender: channel::Sender<T>,
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
            self.close_sender();
        }
    }
}

/// The receive end of a channel.
pub struct Receiver<T> {
    receiver: channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> Sender<T> {
    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Blocks the current thread until a message is sent or the channel is disconnected.
    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if self.state.is_sender_connected() {
            self.sender.send(t)
        } else {
            Err(SendError(t))
        }
    }

    /// Attempts to send a message into the channel without blocking.
    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        if self.state.is_sender_connected() {
            self.sender.try_send(t)
        } else {
            Err(TrySendError::Disconnected(t))
        }
    }

    /// Stop the sender from sending any further messages.
    #[inline]
    pub fn close_sender(&self) {
        self.state.connected.store(false, Ordering::Release);
    }

    /// Check if the sender is still connected.
    #[inline]
    pub fn is_sender_connected(&self) -> bool {
        self.state.is_sender_connected()
    }
}

impl<T> Receiver<T> {
    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Blocks the current thread until a message is received or
    /// the channel is empty and disconnected.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    /// Attempts to receive a message from the channel without blocking.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    /// Waits for a message to be received from the channel,
    /// but only for a limited time.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }
}

impl<T> Drop for Receiver<T> {
    #[inline]
    fn drop(&mut self) {
        self.state.connected.store(false, Ordering::Release);
    }
}

/// Create an unbounded channel.
#[inline]
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State::new());
    let (sender, receiver) = channel::unbounded();
    (
        Sender {
            sender,
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

/// Create a bounded channel.
#[inline]
pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State::new());
    let (sender, receiver) = channel::bounded(cap);
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
    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Send a message regardless its capacity limit.
    #[inline]
    pub fn force_send(&self, t: T) -> Result<(), SendError<T>> {
        let cnt = self.tried_cnt.get();
        self.tried_cnt.set(cnt + 1);
        self.sender.send(t)
    }

    /// Attempts to send a message into the channel without blocking.
    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        let cnt = self.tried_cnt.get();
        if cnt < CHECK_INTERVAL {
            self.tried_cnt.set(cnt + 1);
        } else if self.len() < self.limit {
            self.tried_cnt.set(1);
        } else {
            return Err(TrySendError::Full(t));
        }

        match self.sender.send(t) {
            Ok(()) => Ok(()),
            Err(SendError(t)) => Err(TrySendError::Disconnected(t)),
        }
    }

    /// Stop the sender from sending any further messages.
    #[inline]
    pub fn close_sender(&self) {
        self.sender.close_sender();
    }

    /// Check if the sender is still connected.
    #[inline]
    pub fn is_sender_connected(&self) -> bool {
        self.sender.state.is_sender_connected()
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

/// Create a loosely bounded channel with the given capacity.
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
    use crossbeam::channel::*;
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
        assert!(!tx.is_sender_connected());

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

        let (tx, rx) = super::bounded::<u64>(10);
        assert!(tx.is_empty());
        assert!(tx.is_sender_connected());
        assert_eq!(tx.len(), 0);
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        assert_eq!(tx.len(), 2);
        assert_eq!(rx.len(), 2);
        tx.close_sender();
        assert_eq!(tx.send(3), Err(SendError(3)));
        assert_eq!(tx.try_send(3), Err(TrySendError::Disconnected(3)));
        assert!(!tx.is_sender_connected());
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        // assert_eq!(rx.recv(), Err(RecvError));

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
        assert!(!tx.is_sender_connected());

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

        let (tx, rx) = super::unbounded::<u64>();
        assert!(tx.is_empty());
        assert!(tx.is_sender_connected());
        assert_eq!(tx.len(), 0);
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        assert_eq!(tx.len(), 2);
        assert_eq!(rx.len(), 2);
        tx.close_sender();
        assert_eq!(tx.send(3), Err(SendError(3)));
        assert_eq!(tx.try_send(3), Err(TrySendError::Disconnected(3)));
        assert!(!tx.is_sender_connected());
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        // assert_eq!(rx.recv(), Err(RecvError));
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
