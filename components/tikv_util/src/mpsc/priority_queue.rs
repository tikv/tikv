// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use crossbeam::channel::{RecvError, SendError, TryRecvError, TrySendError};
use crossbeam_skiplist::SkipMap;
use parking_lot_core::{
    park, unpark_all, unpark_one, SpinWait, DEFAULT_PARK_TOKEN, DEFAULT_UNPARK_TOKEN,
};

// Create a priority based channel. Sender can send message with priority of
// u64, and receiver will receive messages in ascending order of priority. For
// two messages of same priority, the receiving order follows FIFO.
pub fn unbounded<T: Send>() -> (Sender<T>, Receiver<T>) {
    let queue = Arc::new(PriorityQueue::new());
    let sender = Sender {
        inner: queue.clone(),
    };
    let receiver = Receiver { inner: queue };
    (sender, receiver)
}

struct Cell<T> {
    ptr: AtomicPtr<T>,
}

unsafe impl<T: Send> Send for Cell<T> {}
unsafe impl<T: Send> Sync for Cell<T> {}

impl<T> Cell<T> {
    fn new(value: T) -> Self {
        Self {
            ptr: AtomicPtr::new(Box::into_raw(Box::new(value))),
        }
    }

    fn take(&self) -> Option<T> {
        let p = self.ptr.swap(std::ptr::null_mut(), Ordering::SeqCst);
        if !p.is_null() {
            unsafe { Some(*Box::from_raw(p)) }
        } else {
            None
        }
    }
}

impl<T> Drop for Cell<T> {
    fn drop(&mut self) {
        self.take();
    }
}

#[derive(Default)]
struct PriorityQueue<T> {
    queue: SkipMap<MapKey, Cell<T>>,

    sequencer: AtomicU64,

    senders: AtomicUsize,
    receivers: AtomicUsize,
}

impl<T> PriorityQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: SkipMap::new(),
            sequencer: AtomicU64::new(0),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
        }
    }

    pub fn get_map_key(&self, pri: u64) -> MapKey {
        MapKey {
            priority: pri,
            sequence: self.sequencer.fetch_add(1, Ordering::Relaxed),
        }
    }

    fn is_disconnected(&self) -> bool {
        self.senders.load(Ordering::SeqCst) == 0
    }
}

// When derived `PartialOrd` on structs, it will produce a lexicographic
// ordering based on the top-to-bottom declaration order of the struct’s
// members.
#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct MapKey {
    priority: u64,
    sequence: u64,
}

pub struct Sender<T: Send> {
    inner: Arc<PriorityQueue<T>>,
}

impl<T: Send + 'static> Sender<T> {
    pub fn try_send(&self, msg: T, pri: u64) -> Result<(), TrySendError<T>> {
        self.send(msg, pri)
            .map_err(|SendError(msg)| TrySendError::Disconnected(msg))
    }

    pub fn send(&self, msg: T, pri: u64) -> Result<(), SendError<T>> {
        if self.inner.receivers.load(Ordering::Acquire) == 0 {
            return Err(SendError(msg));
        }
        self.inner
            .queue
            .insert(self.inner.get_map_key(pri), Cell::new(msg));
        let addr = &*self.inner as *const PriorityQueue<T> as usize;
        unsafe {
            unpark_one(addr, |_| DEFAULT_UNPARK_TOKEN);
        }
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.queue.len()
    }
}

impl<T: Send> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::AcqRel);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        let old = self.inner.senders.fetch_sub(1, Ordering::AcqRel);
        if old <= 1 {
            let addr = &*self.inner as *const PriorityQueue<T> as usize;
            unsafe {
                unpark_all(addr, DEFAULT_UNPARK_TOKEN);
            }
        }
    }
}

pub struct Receiver<T: Send> {
    inner: Arc<PriorityQueue<T>>,
}

impl<T: Send + 'static> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.inner.queue.pop_front() {
            Some(entry) => Ok(entry.value().take().unwrap()),
            None if self.inner.is_disconnected() => Err(TryRecvError::Disconnected),
            None => Err(TryRecvError::Empty),
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        let mut spin = SpinWait::new();
        loop {
            match self.try_recv() {
                Ok(msg) => return Ok(msg),
                Err(TryRecvError::Disconnected) => {
                    return Err(RecvError);
                }
                Err(TryRecvError::Empty) => {
                    if spin.spin() {
                        continue;
                    }
                    let addr = &*self.inner as *const PriorityQueue<T> as usize;
                    unsafe {
                        park(
                            addr,
                            || self.len() == 0 && !self.inner.is_disconnected(),
                            || {},
                            |_, _| {},
                            DEFAULT_PARK_TOKEN,
                            None,
                        );
                    }
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.inner.queue.len()
    }
}

impl<T: Send> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.inner.receivers.fetch_add(1, Ordering::AcqRel);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Send> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receivers.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicU64, thread, time::Duration};

    use crossbeam::channel::TrySendError;
    use rand::Rng;

    use super::*;

    #[test]
    fn test_priority() {
        let (tx, rx) = super::unbounded::<u64>();
        tx.try_send(1, 2).unwrap();
        tx.send(2, 1).unwrap();
        tx.send(3, 3).unwrap();

        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        drop(rx);
        assert_eq!(tx.send(2, 1), Err(SendError(2)));
        assert_eq!(tx.try_send(2, 1), Err(TrySendError::Disconnected(2)));

        let (tx, rx) = super::unbounded::<u64>();
        drop(tx);
        assert_eq!(rx.recv(), Err(RecvError));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));

        let (tx, rx) = super::unbounded::<u64>();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.send(10, 1).unwrap();
        });
        assert_eq!(rx.recv(), Ok(10));

        let (tx, rx) = super::unbounded::<u64>();
        assert_eq!(tx.len(), 0);
        assert_eq!(rx.len(), 0);
        tx.send(2, 1).unwrap();
        tx.send(3, 2).unwrap();
        assert_eq!(tx.len(), 2);
        assert_eq!(rx.len(), 2);
        drop(tx);
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(rx.recv(), Err(RecvError));
    }

    #[test]
    fn test_priority_multi_thread() {
        let (tx, rx) = super::unbounded::<u64>();

        let mut handlers = Vec::with_capacity(10);
        let expected_count = Arc::new(AtomicU64::new(0));
        let real_counter = Arc::new(AtomicU64::new(0));
        for _ in 0..10 {
            let sender = tx.clone();
            let expected_count = expected_count.clone();
            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let pri = rng.gen_range(0..1000);
                let mut cnt = 0;
                for i in 0..1000 {
                    sender.send(i, pri).unwrap();
                    cnt += i;
                }
                expected_count.fetch_add(cnt, Ordering::Relaxed);
            });
            handlers.push(handle);
        }
        for _i in 0..10 {
            let recv = rx.clone();
            let real_counter = real_counter.clone();
            let handle = thread::spawn(move || {
                let mut cnt = 0;
                while let Ok(v) = recv.recv() {
                    cnt += v;
                }
                real_counter.fetch_add(cnt, Ordering::Relaxed);
            });
            handlers.push(handle);
        }
        drop(tx);
        for h in handlers {
            h.join().unwrap();
        }
        assert_eq!(
            expected_count.load(Ordering::Relaxed),
            real_counter.load(Ordering::Relaxed)
        );
    }
}
