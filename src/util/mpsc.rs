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

/*!

This module provides an implementation of mpsc channel based on
crossbeam_channel. Comparing to the crossbeam_channel, this implementation
supports closed detection and try operations.

*/

use crossbeam_channel;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError};
use std::sync::Arc;
use std::time::Duration;

struct ReplicaCount {
    sender: AtomicIsize,
    receiver: AtomicIsize,
}

impl ReplicaCount {
    #[inline]
    fn new() -> ReplicaCount {
        ReplicaCount {
            sender: AtomicIsize::new(1),
            receiver: AtomicIsize::new(1),
        }
    }

    #[inline]
    fn is_receiver_closed(&self) -> bool {
        self.receiver.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn is_sender_closed(&self) -> bool {
        self.sender.load(Ordering::Acquire) == 0
    }
}

pub struct Sender<T> {
    sender: crossbeam_channel::Sender<T>,
    replica_cnt: Arc<ReplicaCount>,
}

impl<T> Clone for Sender<T> {
    #[inline]
    fn clone(&self) -> Sender<T> {
        self.replica_cnt.sender.fetch_add(1, Ordering::AcqRel);
        Sender {
            sender: self.sender.clone(),
            replica_cnt: self.replica_cnt.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    #[inline]
    fn drop(&mut self) {
        self.replica_cnt.sender.fetch_add(-1, Ordering::AcqRel);
    }
}

pub struct Receiver<T> {
    receiver: crossbeam_channel::Receiver<T>,
    replica_cnt: Arc<ReplicaCount>,
}

impl<T> Sender<T> {
    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        if !self.replica_cnt.is_receiver_closed() {
            self.sender.send(t);
            return Ok(());
        }
        Err(SendError(t))
    }

    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        if !self.replica_cnt.is_receiver_closed() {
            return select! {
                send(self.sender, t) => Ok(()),
                default => return Err(TrySendError::Full(t)),
            };
        }
        Err(TrySendError::Disconnected(t))
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
        if !self.replica_cnt.is_sender_closed() {
            return match self.receiver.try_recv() {
                Some(t) => Ok(t),
                None => Err(TryRecvError::Empty),
            };
        }
        Err(TryRecvError::Disconnected)
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        select! {
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
        self.replica_cnt.receiver.fetch_add(-1, Ordering::AcqRel);
    }
}

#[inline]
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let replica_cnt = Arc::new(ReplicaCount::new());
    let (sender, receiver) = crossbeam_channel::unbounded();
    (
        Sender {
            sender,
            replica_cnt: replica_cnt.clone(),
        },
        Receiver {
            receiver,
            replica_cnt,
        },
    )
}

#[inline]
pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let replica_cnt = Arc::new(ReplicaCount::new());
    let (sender, receiver) = crossbeam_channel::bounded(cap);
    (
        Sender {
            sender,
            replica_cnt: replica_cnt.clone(),
        },
        Receiver {
            receiver,
            replica_cnt,
        },
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
        assert!((timer.elapsed().subsec_nanos() as i64 - 100_000_000).abs() < 1_000_000);

        drop(rx);
        assert_eq!(tx.send(2), Err(SendError(2)));
        assert_eq!(tx.try_send(2), Err(TrySendError::Disconnected(2)));

        let (tx, rx) = super::bounded::<u64>(10);
        drop(tx);
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
        assert!((timer.elapsed().subsec_nanos() as i64 - 100_000_000).abs() < 1_000_000);
        let timer = Instant::now();
        tx2.send(2).unwrap();
        assert!(timer.elapsed() > Duration::from_millis(50));
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
        assert!((timer.elapsed().subsec_nanos() as i64 - 100_000_000).abs() < 1_000_000);

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
        assert!((timer.elapsed().subsec_nanos() as i64 - 100_000_000).abs() < 1_000_000);
    }
}
