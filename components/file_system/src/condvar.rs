// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{future::FutureExt, pin_mut, select};
use std::cell::Cell;
use std::sync::Condvar as StdCondvar;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Semaphore as AsyncSemaphore;

trait LinkedNotifiable {
    fn notify(&self);
    fn get_next(&self) -> Option<*mut dyn LinkedNotifiable>;
    fn set_next(&self, next: Option<*mut dyn LinkedNotifiable>);
}

#[derive(Debug)]
struct SyncCondvarNode {
    condv: StdCondvar,
    next: Cell<Option<*mut dyn LinkedNotifiable>>,
}

impl SyncCondvarNode {
    pub fn new() -> SyncCondvarNode {
        SyncCondvarNode {
            condv: StdCondvar::new(),
            next: Cell::new(None),
        }
    }
}

impl LinkedNotifiable for SyncCondvarNode {
    fn notify(&self) {
        self.condv.notify_one();
    }
    fn get_next(&self) -> Option<*mut dyn LinkedNotifiable> {
        self.next.get()
    }
    fn set_next(&self, next: Option<*mut dyn LinkedNotifiable>) {
        self.next.set(next);
    }
}

#[derive(Debug)]
struct AsyncCondvarNode {
    sem: AsyncSemaphore,
    next: Cell<Option<*mut dyn LinkedNotifiable>>,
}

impl AsyncCondvarNode {
    pub fn new() -> AsyncCondvarNode {
        AsyncCondvarNode {
            sem: AsyncSemaphore::new(0),
            next: Cell::new(None),
        }
    }
}

impl LinkedNotifiable for AsyncCondvarNode {
    fn notify(&self) {
        self.sem.add_permits(1);
    }
    fn get_next(&self) -> Option<*mut dyn LinkedNotifiable> {
        self.next.get()
    }
    fn set_next(&self, next: Option<*mut dyn LinkedNotifiable>) {
        self.next.set(next);
    }
}

#[derive(Debug)]
pub struct Condvar {
    head: Cell<Option<*mut dyn LinkedNotifiable>>,
    tail: Cell<Option<*mut dyn LinkedNotifiable>>,
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

    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        timeout: Duration,
    ) -> (MutexGuard<'a, T>, bool) {
        let mut node = SyncCondvarNode::new();
        let raw_tail: *mut _ = &mut node;
        if let Some(tail) = self.tail.get() {
            unsafe {
                (*tail).set_next(Some(raw_tail));
            }
        } else {
            self.head.set(Some(raw_tail));
        }
        self.tail.set(Some(raw_tail));
        // alternative: std::thread::park_timeout suffers from spurious wake
        let (guard, res) = node.condv.wait_timeout(guard, timeout).unwrap();
        self.notify_before_me(raw_tail);
        (guard, res.timed_out())
    }

    pub async fn async_wait_timeout<'a, 'b, T>(
        &self,
        mu: &'a Mutex<T>,
        guard: MutexGuard<'b, T>,
        timeout: Duration,
    ) -> (MutexGuard<'a, T>, bool) {
        // it's safe to drop early because semaphore is state preserving
        std::mem::drop(guard);
        let mut node = AsyncCondvarNode::new();
        let raw_tail: *mut _ = &mut node;
        if let Some(tail) = self.tail.get() {
            unsafe {
                (*tail).set_next(Some(raw_tail));
            }
        } else {
            self.head.set(Some(raw_tail));
        }
        self.tail.set(Some(raw_tail));
        let f = node.sem.acquire().fuse();
        pin_mut!(f);
        let timed_out = select! {
            _ = f => false,
            _ = tokio::time::delay_for(timeout).fuse() => true,
        };
        let guard = mu.lock().unwrap();
        self.notify_before_me(raw_tail);
        (guard, timed_out)
    }

    fn notify_before_me(&self, me: *mut dyn LinkedNotifiable) {
        let mut ptr = self.head.get();
        loop {
            if let Some(inner) = ptr {
                unsafe {
                    let node = &(*inner);
                    if inner == me {
                        self.head.set(node.get_next());
                        if self.head.get().is_none() {
                            self.tail.set(None);
                        }
                        break;
                    } else {
                        node.notify();
                        ptr = node.get_next();
                    }
                }
            } else {
                self.head.set(None);
                self.tail.set(None);
                return;
            }
        }
    }

    fn notify_head(&self) {
        if let Some(head) = self.head.get() {
            unsafe {
                let node = &(*head);
                node.notify();
                self.head.set(node.get_next());
            }
            if self.head.get().is_none() {
                self.tail.set(None);
            }
        }
    }

    pub fn notify_all(&self) {
        self.notify_head();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn bench_std_condvar(b: &mut Bencher) {
        let mu = Mutex::new(());
        let condv = StdCondvar::new();
        b.iter(|| {
            let guard = mu.lock().unwrap();
            condv.wait_timeout(guard, Duration::from_millis(1))
        });
    }

    #[bench]
    fn bench_condvar_sync(b: &mut Bencher) {
        let mu = Mutex::new(());
        let condv = Condvar::new();
        b.iter(|| {
            let guard = mu.lock().unwrap();
            condv.wait_timeout(guard, Duration::from_millis(1))
        });
    }

    #[bench]
    fn bench_condvar_async(b: &mut Bencher) {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let mu = Mutex::new(());
        let condv = Condvar::new();
        b.iter(|| {
            let guard = mu.lock().unwrap();
            rt.block_on(condv.async_wait_timeout(&mu, guard, Duration::from_millis(1)))
        });
    }
}
