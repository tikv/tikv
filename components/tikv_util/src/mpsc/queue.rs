// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::channel::{RecvTimeoutError, SendError, TryRecvError, TrySendError};
use crossbeam::queue::SegQueue;
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use std::usize;

const CONNECTED_BIT: usize = 1;
const REF_BASE: usize = 1 << 1;

struct Channel<T> {
    state: AtomicUsize,
    queue: SegQueue<T>,
}

impl<T> Channel<T> {
    fn is_connected(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        state & CONNECTED_BIT != 0 && state != REF_BASE + CONNECTED_BIT
    }

    fn disconnect(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & CONNECTED_BIT != 0 {
                match self.state.compare_exchange_weak(
                    state,
                    state - CONNECTED_BIT,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(s) => state = s,
                }
            } else {
                return;
            }
        }
    }
}

unsafe fn drop_chan<T>(chan: NonNull<Channel<T>>) {
    atomic::fence(Ordering::Acquire);
    Box::from_raw(chan.as_ptr());
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
    chan: NonNull<Channel<T>>,
}

impl<T> Clone for Sender<T> {
    #[inline]
    fn clone(&self) -> Sender<T> {
        self.as_chan().state.fetch_add(REF_BASE, Ordering::Relaxed);
        Sender { chan: self.chan }
    }
}

impl<T> Drop for Sender<T> {
    #[inline]
    fn drop(&mut self) {
        let res = self.as_chan().state.fetch_sub(REF_BASE, Ordering::Release);
        if res != REF_BASE {
            return;
        }
        unsafe { drop_chan(self.chan) }
    }
}

/// The receive end of a channel.
pub struct Receiver<T> {
    chan: NonNull<Channel<T>>,
}

impl<T> Drop for Receiver<T> {
    #[inline]
    fn drop(&mut self) {
        let mut state = self.as_chan().state.load(Ordering::Acquire);
        loop {
            let new_state = (state & !CONNECTED_BIT) - REF_BASE;
            match self.as_chan().state.compare_exchange_weak(
                state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if new_state == 0 {
                        unsafe {
                            drop_chan(self.chan);
                        }
                    }
                    return;
                }
                Err(s) => state = s,
            }
        }
    }
}

impl<T> Sender<T> {
    #[inline]
    fn as_chan(&self) -> &Channel<T> {
        unsafe { self.chan.as_ref() }
    }

    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.as_chan().queue.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_chan().queue.is_empty()
    }

    /// Attempts to send a message into the channel without blocking.
    #[inline]
    pub fn send(&self, t: T) -> Result<(), SendError<T>> {
        let chan = self.as_chan();
        if chan.is_connected() {
            chan.queue.push(t);
            Ok(())
        } else {
            Err(SendError(t))
        }
    }

    /// Stop the sender from sending any further messages.
    #[inline]
    pub fn disconnect(&self) {
        self.as_chan().disconnect();
    }

    /// Check if the sender is still connected.
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.as_chan().is_connected()
    }
}

impl<T> Receiver<T> {
    #[inline]
    fn as_chan(&self) -> &Channel<T> {
        unsafe { self.chan.as_ref() }
    }

    /// Returns the number of messages in the channel.
    #[inline]
    pub fn len(&self) -> usize {
        self.as_chan().queue.len()
    }

    /// Returns true if the channel is empty.
    ///
    /// Note: Zero-capacity channels are always empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_chan().queue.is_empty()
    }

    /// Attempts to receive a message from the channel without blocking.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let chan = self.as_chan();
        match chan.queue.pop() {
            Ok(t) => Ok(t),
            Err(_) => {
                if chan.is_connected() {
                    Err(TryRecvError::Empty)
                } else {
                    Err(TryRecvError::Disconnected)
                }
            }
        }
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let timer = Instant::now();
        let chan = self.as_chan();
        while timer.elapsed() < timeout {
            match chan.queue.pop() {
                Ok(t) => return Ok(t),
                Err(_) => {
                    if chan.is_connected() {
                        thread::sleep(Duration::from_millis(1));
                    } else {
                        return Err(RecvTimeoutError::Disconnected);
                    }
                }
            }
        }
        Err(RecvTimeoutError::Timeout)
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Send for Receiver<T> {}

/// Create an unbounded channel.
#[inline]
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let chan = unsafe {
        NonNull::new_unchecked(Box::into_raw(Box::new(Channel {
            state: AtomicUsize::new((REF_BASE + REF_BASE) | CONNECTED_BIT),
            queue: SegQueue::new(),
        })))
    };
    (Sender { chan }, Receiver { chan })
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
    pub fn disconnect(&self) {
        self.sender.disconnect();
    }

    /// Check if the sender is still connected.
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.sender.is_connected()
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
    fn test_unbounded() {
        let (tx, rx) = super::unbounded::<u64>();
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
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
        assert!(!tx.is_connected());

        let (tx, rx) = super::unbounded::<u64>();
        drop(tx);
        assert!(!rx.as_chan().is_connected());
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
        while timer.elapsed() < Duration::from_secs(3) {
            if rx.try_recv() == Ok(10) {
                break;
            }
        }
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);
        assert!(elapsed < Duration::from_secs(3), "{:?}", elapsed);

        let (tx, rx) = super::unbounded::<u64>();
        assert!(tx.is_empty());
        assert!(tx.is_connected());
        assert_eq!(tx.len(), 0);
        assert!(rx.is_empty());
        assert_eq!(rx.len(), 0);
        tx.send(2).unwrap();
        assert_eq!(tx.len(), 1);
        assert_eq!(rx.len(), 1);
        tx.disconnect();
        assert_eq!(tx.send(3), Err(SendError(3)));
        assert!(!tx.is_connected());
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
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
            assert_eq!(rx.try_recv(), Ok(i));
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
        assert_eq!(rx.try_recv(), Ok(2));
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
        assert_eq!(rx.recv_timeout(Duration::from_millis(300)), Ok(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "{:?}", elapsed);
        assert!(elapsed < Duration::from_millis(300), "{:?}", elapsed);
    }
}
