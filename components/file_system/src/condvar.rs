// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{future::FutureExt, pin_mut, select};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::Condvar as StdCondvar;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Semaphore as AsyncSemaphore;

enum LinkedNotifiable {
    Sync(StdCondvar, Cell<Option<NonNull<LinkedNotifiable>>>),
    Async(AsyncSemaphore, Cell<Option<NonNull<LinkedNotifiable>>>),
}

impl LinkedNotifiable {
    pub fn new_sync() -> LinkedNotifiable {
        LinkedNotifiable::Sync(StdCondvar::new(), Cell::new(None))
    }
    pub fn new_async() -> LinkedNotifiable {
        LinkedNotifiable::Async(AsyncSemaphore::new(0), Cell::new(None))
    }

    pub fn notify(&self) {
        match self {
            &LinkedNotifiable::Sync(ref condv, _) => condv.notify_one(),
            &LinkedNotifiable::Async(ref sem, _) => sem.add_permits(1),
        }
    }
    pub fn get_next(&self) -> Option<NonNull<LinkedNotifiable>> {
        match self {
            &LinkedNotifiable::Sync(_, ref ptr) => ptr.get(),
            &LinkedNotifiable::Async(_, ref ptr) => ptr.get(),
        }
    }
    pub fn set_next(&self, next: Option<NonNull<LinkedNotifiable>>) {
        match self {
            &LinkedNotifiable::Sync(_, ref ptr) => ptr.set(next),
            &LinkedNotifiable::Async(_, ref ptr) => ptr.set(next),
        }
    }
}

#[derive(Debug)]
pub struct Condvar {
    head: Cell<Option<NonNull<LinkedNotifiable>>>,
    tail: Cell<Option<NonNull<LinkedNotifiable>>>,
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
        let mut node = LinkedNotifiable::new_sync();
        let raw_node_ptr: *mut _ = &mut node;
        let node_ptr = unsafe { NonNull::new_unchecked(raw_node_ptr) };
        if let Some(tail) = self.tail.get() {
            unsafe {
                tail.as_ref().set_next(Some(node_ptr.clone()));
            }
        } else {
            self.head.set(Some(node_ptr.clone()));
        }
        self.tail.set(Some(node_ptr));
        // alternative: std::thread::park_timeout suffers from spurious wake
        let (guard, res) = match node {
            LinkedNotifiable::Sync(ref condv, _) => condv.wait_timeout(guard, timeout).unwrap(),
            _ => unreachable!(),
        };
        // let (guard, res) = node.condv.wait_timeout(guard, timeout).unwrap();
        self.notify_before_me(raw_node_ptr);
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
        let mut node = LinkedNotifiable::new_async();
        let raw_node_ptr: *mut _ = &mut node;
        let node_ptr = unsafe { NonNull::new_unchecked(raw_node_ptr) };
        if let Some(tail) = self.tail.get() {
            unsafe {
                tail.as_ref().set_next(Some(node_ptr));
            }
        } else {
            self.head.set(Some(node_ptr));
        }
        self.tail.set(Some(node_ptr));
        let f = match node {
            LinkedNotifiable::Async(ref sem, _) => sem.acquire().fuse(),
            _ => unreachable!(),
        };
        // let f = node.sem.acquire().fuse();
        pin_mut!(f);
        let timed_out = select! {
            _ = f => false,
            _ = tokio::time::delay_for(timeout).fuse() => true,
        };
        let guard = mu.lock().unwrap();
        self.notify_before_me(raw_node_ptr);
        (guard, timed_out)
    }

    fn notify_before_me(&self, me: *mut LinkedNotifiable) {
        let mut ptr = self.head.get();
        loop {
            if let Some(inner) = ptr {
                unsafe {
                    let node = inner.as_ref();
                    if inner.as_ptr() == me {
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
                let node = head.as_ref();
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
