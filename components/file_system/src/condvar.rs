// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{future::FutureExt, pin_mut, select};
use std::cell::Cell;
use std::sync::Condvar as StdCondvar;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Semaphore as AsyncSemaphore;

trait Notifiable {
    fn notify(&self);
    fn get_next(&self) -> Option<*mut dyn Notifiable>;
    fn set_next(&self, next: Option<*mut dyn Notifiable>);
}

#[derive(Debug)]
struct SyncCondvarNode {
    condv: StdCondvar,
    next: Cell<Option<*mut dyn Notifiable>>,
}

impl SyncCondvarNode {
    pub fn new() -> SyncCondvarNode {
        SyncCondvarNode {
            condv: StdCondvar::new(),
            next: Cell::new(None),
        }
    }
}

impl Notifiable for SyncCondvarNode {
    fn notify(&self) {
        self.condv.notify_one();
    }
    fn get_next(&self) -> Option<*mut dyn Notifiable> {
        self.next.get()
    }
    fn set_next(&self, next: Option<*mut dyn Notifiable>) {
        self.next.set(next);
    }
}

#[derive(Debug)]
struct AsyncCondvarNode {
    sem: AsyncSemaphore,
    next: Cell<Option<*mut dyn Notifiable>>,
}

impl AsyncCondvarNode {
    pub fn new() -> AsyncCondvarNode {
        AsyncCondvarNode {
            sem: AsyncSemaphore::new(0),
            next: Cell::new(None),
        }
    }
}

impl Notifiable for AsyncCondvarNode {
    fn notify(&self) {
        self.sem.add_permits(1);
    }
    fn get_next(&self) -> Option<*mut dyn Notifiable> {
        self.next.get()
    }
    fn set_next(&self, next: Option<*mut dyn Notifiable>) {
        self.next.set(next);
    }
}

#[derive(Debug)]
pub struct Condvar {
    head: Cell<Option<*mut dyn Notifiable>>,
    tail: Cell<Option<*mut dyn Notifiable>>,
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
    ) -> MutexGuard<'a, T> {
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
        // Alternative: use std::thread::park_timeout
        let guard = node.condv.wait_timeout(guard, timeout).unwrap().0;
        self.notify_next();
        guard
    }

    pub async fn async_wait_timeout<'a, 'b, T>(
        &self,
        mu: &'a Mutex<T>,
        guard: MutexGuard<'b, T>,
        timeout: Duration,
    ) -> MutexGuard<'a, T> {
        // drop early
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
        select! {
            _ = f => (),
            _ = tokio::time::delay_for(timeout).fuse() => (),
        }
        let guard = mu.lock().unwrap();
        self.notify_next();
        guard
    }

    fn notify_next(&self) {
        if let Some(head) = self.head.get() {
            unsafe {
                let ref node = *head;
                node.notify();
                self.head.set(node.get_next());
            }
        }
        if self.head.get().is_none() {
            self.tail.set(None);
        }
    }

    pub fn notify_all(&self) {
        self.notify_next();
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
