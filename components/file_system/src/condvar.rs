// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{future::FutureExt, pin_mut, select};
use parking_lot::{Condvar as ParkingLotCondvar, Mutex, MutexGuard};
use std::cell::Cell;
use std::ptr::NonNull;
use std::time::Duration;
use tokio::sync::Notify as TokioNotify;

struct DoublyLinkedNode<T> {
    prev: Cell<Option<NonNull<T>>>,
    next: Cell<Option<NonNull<T>>>,
}

impl<T> DoublyLinkedNode<T> {
    fn new() -> DoublyLinkedNode<T> {
        DoublyLinkedNode {
            prev: Cell::new(None),
            next: Cell::new(None),
        }
    }
}

enum CondvarNode {
    Sync(ParkingLotCondvar, DoublyLinkedNode<CondvarNode>),
    Async(TokioNotify, DoublyLinkedNode<CondvarNode>),
}

impl CondvarNode {
    pub fn new_sync() -> CondvarNode {
        CondvarNode::Sync(ParkingLotCondvar::new(), DoublyLinkedNode::new())
    }

    pub fn new_async() -> CondvarNode {
        CondvarNode::Async(TokioNotify::new(), DoublyLinkedNode::new())
    }

    pub fn notify(&self) {
        match *self {
            CondvarNode::Sync(ref condv, _) => {
                condv.notify_one();
            }
            CondvarNode::Async(ref not, _) => not.notify(),
        }
    }

    pub fn get_prev(&self) -> Option<NonNull<CondvarNode>> {
        match *self {
            CondvarNode::Sync(_, ref node) => node.prev.get(),
            CondvarNode::Async(_, ref node) => node.prev.get(),
        }
    }

    pub fn get_next(&self) -> Option<NonNull<CondvarNode>> {
        match *self {
            CondvarNode::Sync(_, ref node) => node.next.get(),
            CondvarNode::Async(_, ref node) => node.next.get(),
        }
    }

    pub fn set_prev(&self, prev: Option<NonNull<CondvarNode>>) {
        match *self {
            CondvarNode::Sync(_, ref node) => node.prev.set(prev),
            CondvarNode::Async(_, ref node) => node.prev.set(prev),
        }
    }

    pub fn set_next(&self, next: Option<NonNull<CondvarNode>>) {
        match *self {
            CondvarNode::Sync(_, ref node) => node.next.set(next),
            CondvarNode::Async(_, ref node) => node.next.set(next),
        }
    }
}

/// Un-prioritized conditional variable. Supports both synchronously or
/// asynchronously waiting on the same instance.
/// TODO: Prioritized waiting
/// Maintains multiple linked list for each priority. When notified, instead of
/// waking up all nodes at once, sort them to a seperate waiting queue and only
/// wake up the leader.
#[derive(Debug)]
pub struct Condvar {
    head: Cell<Option<NonNull<CondvarNode>>>,
    tail: Cell<Option<NonNull<CondvarNode>>>,
}

unsafe impl Send for Condvar {}
unsafe impl Sync for Condvar {}

impl Condvar {
    pub fn new() -> Condvar {
        Condvar {
            head: Cell::new(None),
            tail: Cell::new(None),
        }
    }

    #[inline]
    fn enqueue(&self, raw_node: &mut CondvarNode) {
        let node = unsafe { Some(NonNull::new_unchecked(raw_node)) };
        raw_node.set_prev(self.tail.get());
        if let Some(tail) = self.tail.get() {
            unsafe {
                tail.as_ref().set_next(node);
            }
        } else {
            self.head.set(node);
        }
        self.tail.set(node);
    }

    #[inline]
    fn dequeue(&self, raw_node: &mut CondvarNode) {
        let prev = raw_node.get_prev();
        let next = raw_node.get_next();
        if let Some(prev) = prev {
            unsafe {
                prev.as_ref().set_next(next);
            }
        } else {
            assert!(self.head.get().unwrap().as_ptr() == raw_node);
            self.head.set(next);
        }
        if let Some(next) = next {
            unsafe {
                next.as_ref().set_prev(prev);
            }
        } else {
            assert!(self.tail.get().unwrap().as_ptr() == raw_node);
            self.tail.set(prev);
        }
    }

    /// Notifies all waiters in queue as till now.
    pub fn notify_all(&self) {
        let mut ptr = self.head.get();
        while let Some(inner) = ptr {
            unsafe {
                let node = inner.as_ref();
                node.notify();
                ptr = node.get_next();
            }
        }
    }

    /// Synchronously waits on this condition variable for a notification,
    /// timing out after a specified duration.
    pub fn wait_timeout<'a, T>(
        &self,
        mut guard: MutexGuard<'a, T>,
        timeout: Duration,
    ) -> (MutexGuard<'a, T>, bool) {
        // mutable just to indulge NonNull
        let mut node = CondvarNode::new_sync();
        self.enqueue(&mut node);
        // alternative: std::thread::park_timeout suffers from spurious wake
        let res = match node {
            CondvarNode::Sync(ref condv, _) => condv.wait_for(&mut guard, timeout),
            _ => unreachable!(),
        };
        self.dequeue(&mut node);
        (guard, res.timed_out())
    }

    /// Asynchronously waits on this condition variable for a notification,
    /// timing out after a specified duration. Need to pass in additional
    /// reference of the original mutex to regain lock after wakeup.
    pub async fn async_wait_timeout<'a, 'b, T>(
        &self,
        mu: &'a Mutex<T>,
        guard: MutexGuard<'b, T>,
        timeout: Duration,
    ) -> (MutexGuard<'a, T>, bool) {
        let mut node = CondvarNode::new_async();
        self.enqueue(&mut node);
        std::mem::drop(guard);
        let timed_out = {
            let f = match node {
                CondvarNode::Async(ref not, _) => not.notified().fuse(),
                _ => unreachable!(),
            };
            pin_mut!(f);
            select! {
                _ = f => false,
                _ = tokio::time::delay_for(timeout).fuse() => true,
            }
        };
        let guard = mu.lock();
        self.dequeue(&mut node);
        (guard, timed_out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar as StdCondvar, Mutex as StdMutex};

    use tikv_util::time::Instant;

    use test::Bencher;

    #[test]
    fn test_condvar() {
        let long_timeout_millis = 1000 * 100;
        let short_timeout_millis = 5;
        let total_waits = 50;

        let mu = Arc::new(Mutex::new(()));
        let condv = Arc::new(Condvar::new());
        let mut threads = vec![];
        let enter_ticket = Arc::new(AtomicUsize::new(0));
        let exit_ticket = Arc::new(AtomicUsize::new(0));

        let begin = Instant::now_coarse();
        for i in 0..total_waits {
            let (mu, condv, enter, exit) = (
                mu.clone(),
                condv.clone(),
                enter_ticket.clone(),
                exit_ticket.clone(),
            );
            while enter.load(Ordering::Relaxed) != i {
                std::thread::yield_now();
            }
            let t = std::thread::spawn(move || {
                let guard = mu.lock();
                assert_eq!(enter.fetch_add(1, Ordering::Relaxed), i);
                if i % 3 == 0 {
                    let (_, timed_out) = condv.wait_timeout(
                        guard,
                        Duration::from_millis(short_timeout_millis * (i / 3) as u64),
                    );
                    assert_eq!(timed_out, true);
                    assert_eq!(exit.fetch_add(1, Ordering::Relaxed), i / 3);
                } else if i % 3 == 1 {
                    let mut rt = tokio::runtime::Runtime::new().unwrap();
                    let (_, timed_out) = rt.block_on(condv.async_wait_timeout(
                        &mu,
                        guard,
                        Duration::from_millis(long_timeout_millis),
                    ));
                    assert_eq!(timed_out, false);
                } else {
                    let (_, timed_out) =
                        condv.wait_timeout(guard, Duration::from_millis(long_timeout_millis));
                    assert_eq!(timed_out, false);
                }
            });
            threads.push(t);
        }
        while exit_ticket.load(Ordering::Relaxed) != (total_waits + 2) / 3
            || enter_ticket.load(Ordering::Relaxed) != total_waits
        {
            std::thread::yield_now();
        }
        {
            let _guard = mu.lock();
            condv.notify_all();
        }
        for t in threads {
            t.join().unwrap();
        }
        let end = Instant::now_coarse();
        assert!(end.duration_since(begin) < Duration::from_secs(short_timeout_millis * 2));
    }

    #[bench]
    #[ignore]
    fn bench_std_condvar(b: &mut Bencher) {
        let mu = StdMutex::new(());
        let condv = StdCondvar::new();
        b.iter(|| {
            let guard = mu.lock().unwrap();
            condv.wait_timeout(guard, Duration::from_millis(1))
        });
    }

    #[bench]
    #[ignore]
    fn bench_condvar_sync(b: &mut Bencher) {
        let mu = Mutex::new(());
        let condv = Condvar::new();
        b.iter(|| {
            let guard = mu.lock();
            condv.wait_timeout(guard, Duration::from_millis(1))
        });
    }

    #[bench]
    #[ignore]
    fn bench_condvar_async(b: &mut Bencher) {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let mu = Mutex::new(());
        let condv = Condvar::new();
        b.iter(|| {
            let guard = mu.lock();
            rt.block_on(condv.async_wait_timeout(&mu, guard, Duration::from_millis(1)))
        });
    }
}
