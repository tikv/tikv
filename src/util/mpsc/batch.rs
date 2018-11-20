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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam::sync::AtomicOption;
use crossbeam_channel;
pub use crossbeam_channel::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError};
use futures::task::{self, Task};
use futures::{Async, Poll, Stream};

struct State {
    recv_task: AtomicOption<Task>,
    notify_size: usize,
    // How many messages are sent without notify.
    old_pending: AtomicUsize,
    notifier_registered: AtomicBool,
}

impl State {
    fn new(notify_size: usize) -> State {
        State {
            recv_task: AtomicOption::new(),
            notify_size,
            old_pending: AtomicUsize::new(0),
            notifier_registered: AtomicBool::new(false),
        }
    }

    #[inline]
    fn try_notiry_post_send(&self) {
        let old_pending = self.old_pending.fetch_add(1, Ordering::AcqRel);
        if old_pending >= self.notify_size - 1 {
            self.notify();
        }
    }

    #[inline]
    fn notify(&self) {
        let task = self.recv_task.take(Ordering::AcqRel);
        if let Some(t) = task {
            self.old_pending.store(0, Ordering::AcqRel);
            t.notify();
        }
    }
}

/// `Notifier` is used to notify receiver whenever you want.
pub struct Notifier(Arc<State>);
impl Notifier {
    #[inline]
    pub fn notify(self) {
        drop(self);
    }
}

impl Drop for Notifier {
    #[inline]
    fn drop(&mut self) {
        let notifier_registered = &self.0.notifier_registered;
        if !notifier_registered.compare_and_swap(true, false, Ordering::AcqRel) {
            unreachable!("notifier_registered must be true");
        }
        self.0.notify();
    }
}

pub struct Sender<T> {
    sender: crossbeam_channel::Sender<T>,
    state: Arc<State>,
}

impl<T> Clone for Sender<T> {
    #[inline]
    fn clone(&self) -> Sender<T> {
        Sender {
            sender: self.sender.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Drop for Sender<T> {
    #[inline]
    fn drop(&mut self) {
        self.state.notify();
    }
}

pub struct Receiver<T> {
    receiver: crossbeam_channel::Receiver<T>,
    state: Arc<State>,
}

impl<T> Sender<T> {
    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        self.sender.send(t)?;
        self.state.try_notiry_post_send();
        Ok(())
    }

    #[inline]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(t)?;
        self.state.try_notiry_post_send();
        Ok(())
    }

    #[inline]
    pub fn get_notifier(&self) -> Option<Notifier> {
        let notifier_registered = &self.state.notifier_registered;
        if !notifier_registered.compare_and_swap(false, true, Ordering::AcqRel) {
            return Some(Notifier(Arc::clone(&self.state)));
        }
        None
    }
}

impl<T> Receiver<T> {
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    #[inline]
    fn yield_poll(&self) -> bool {
        let recv_task = &self.state.recv_task;
        recv_task.swap(task::current(), Ordering::AcqRel).is_some()
    }
}

/// Create a unbounded channel with a given `notify_size`, which means if there are more pending
/// messages in the channel than `notify_size`, the `Sender` will auto notify the `Receiver`.
///
/// ### Panics
/// if `notify_size` equals to 0.
#[inline]
pub fn unbounded<T>(notify_size: usize) -> (Sender<T>, Receiver<T>) {
    assert!(notify_size > 0);
    let state = Arc::new(State::new(notify_size));
    let (sender, receiver) = crossbeam_channel::unbounded();
    (
        Sender {
            sender,
            state: state.clone(),
        },
        Receiver { receiver, state },
    )
}

/// Create a bounded channel with a given `notify_size`, which means if there are more pending
/// messages in the channel than `notify_size`, the `Sender` will auto notify the `Receiver`.
///
/// ### Panics
/// if `notify_size` equals to 0.
#[inline]
pub fn bounded<T>(cap: usize, notify_size: usize) -> (Sender<T>, Receiver<T>) {
    assert!(notify_size > 0);
    let state = Arc::new(State::new(notify_size));
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
            Err(TryRecvError::Empty) => if self.yield_poll() {
                Ok(Async::NotReady)
            } else {
                // Note: If there is a message or all the senders are dropped after
                // polling but before task is set, `t` should be None.
                self.poll()
            },
            Err(TryRecvError::Disconnected) => Ok(Async::Ready(None)),
        }
    }
}

/// `BatchReceiver` is a `futures::Stream`, which returns a batched type.
pub struct BatchReceiver<T, E, I: Fn() -> E, C: Fn(&mut E, T)> {
    rx: Receiver<T>,
    max_batch_size: usize,
    elem: Option<E>,
    initializer: I,
    collector: C,
}

impl<T, E, I: Fn() -> E, C: Fn(&mut E, T)> BatchReceiver<T, E, I, C> {
    /// Create a new `BatchReceiver` with given `initializer` and `collector`. `initializer` is
    /// used to generate a initial value, and `collector` will collect every (at most
    /// `max_batch_size`) raw items into the batched value.
    pub fn new(rx: Receiver<T>, max_batch_size: usize, initializer: I, collector: C) -> Self {
        BatchReceiver {
            rx,
            max_batch_size,
            elem: None,
            initializer,
            collector,
        }
    }
}

impl<T, E, I: Fn() -> E, C: Fn(&mut E, T)> Stream for BatchReceiver<T, E, I, C> {
    type Error = ();
    type Item = E;

    fn poll(&mut self) -> Poll<Option<Self::Item>, ()> {
        let mut count = 0;
        let finished = loop {
            match self.rx.try_recv() {
                Ok(m) => {
                    (self.collector)(self.elem.get_or_insert_with(&self.initializer), m);
                    count += 1;
                    if count >= self.max_batch_size {
                        break false;
                    }
                }
                Err(TryRecvError::Disconnected) => break true,
                Err(TryRecvError::Empty) => if self.rx.yield_poll() {
                    break false;
                },
            }
        };

        if self.elem.is_none() && finished {
            return Ok(Async::Ready(None));
        } else if self.elem.is_none() {
            return Ok(Async::NotReady);
        }
        Ok(Async::Ready(self.elem.take()))
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time};

    use futures_cpupool::CpuPool;

    use super::*;

    #[test]
    fn test_batch_receiver() {
        let (tx, rx) = unbounded::<u64>(4);
        let rx = BatchReceiver::new(rx, 8, || Vec::with_capacity(4), |v, e| v.push(e));

        let msg_counter = Arc::new(AtomicUsize::new(0));
        let msg_counter1 = Arc::clone(&msg_counter);
        let pool = CpuPool::new(1);
        pool.spawn(rx.for_each(move |v| {
            msg_counter1.fetch_add(v.len(), Ordering::AcqRel);
            Ok(())
        })).forget();
        thread::sleep(time::Duration::from_millis(10));

        // Send without notify, the receiver can't get batched messages.
        assert!(tx.send(0).is_ok());
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::AcqRel), 0);

        // Send with notify.
        let notifier = tx.get_notifier().unwrap();
        assert!(tx.get_notifier().is_none());
        notifier.notify();
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::AcqRel), 1);

        // Auto notify with more sendings.
        for _ in 0..4 {
            assert!(tx.send(0).is_ok());
        }
        thread::sleep(time::Duration::from_millis(10));
        assert_eq!(msg_counter.load(Ordering::AcqRel), 5);
    }
}
