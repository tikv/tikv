// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::stats::TaskStats;

use crossbeam::deque::Injector;
use futures::future::{BoxFuture, FutureExt};
use rand::prelude::*;

use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

pub struct Task {
    task: UnsafeCell<BoxFuture<'static, ()>>,
    injectors: Arc<[Injector<ArcTask>]>,
    status: AtomicU8,
    // this token's total elapsed time
    stats: Arc<TaskStats>,
    nice: u8,
    _token: u64,
}

#[derive(Clone)]
pub struct ArcTask(Arc<Task>);

const WAITING: u8 = 0; // --> POLLING
const POLLING: u8 = 1; // --> WAITING, REPOLL, or COMPLETE
const REPOLL: u8 = 2; // --> POLLING
const COMPLETE: u8 = 3; // No transitions out

unsafe impl Send for Task {}
unsafe impl Sync for Task {}

impl ArcTask {
    pub fn new<F>(
        future: F,
        injectors: Arc<[Injector<ArcTask>]>,
        // parker: Arc<Parker>,
        stats: Arc<TaskStats>,
        nice: u8,
        token: u64,
    ) -> ArcTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let future = Arc::new(Task {
            task: UnsafeCell::new(future.boxed()),
            injectors,
            // parker,
            status: AtomicU8::new(WAITING),
            stats,
            nice,
            _token: token,
        });
        let future: *const Task = Arc::into_raw(future) as *const Task;
        unsafe { task(future) }
    }

    unsafe fn poll(self) {
        self.0.status.store(POLLING, Ordering::SeqCst);
        let waker = ManuallyDrop::new(waker(&*self.0));
        let mut cx = Context::from_waker(&waker);
        loop {
            if let Poll::Ready(_) = (&mut *self.0.task.get()).poll_unpin(&mut cx) {
                break self.0.status.store(COMPLETE, Ordering::SeqCst);
            }
            match self.0.status.compare_exchange(
                POLLING,
                WAITING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(_) => self.0.status.store(POLLING, Ordering::SeqCst),
            }
        }
    }
}

unsafe fn waker(task: *const Task) -> Waker {
    Waker::from_raw(RawWaker::new(
        task as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    ))
}

unsafe fn clone_raw(this: *const ()) -> RawWaker {
    let task = clone_task(this as *const Task);
    RawWaker::new(
        Arc::into_raw(task.0) as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    )
}

unsafe fn drop_raw(this: *const ()) {
    drop(task(this as *const Task))
}

unsafe fn wake_raw(this: *const ()) {
    let task = task(this as *const Task);
    let mut status = task.0.status.load(Ordering::SeqCst);
    loop {
        match status {
            WAITING => {
                match task.0.status.compare_exchange(
                    WAITING,
                    POLLING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        let index = thread_rng().gen::<usize>() % 3;
                        task.0.injectors[index].push(clone_task(&*task.0));
                        // task.0.parker.notify_one();
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task.0.status.compare_exchange(
                    POLLING,
                    REPOLL,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

unsafe fn wake_ref_raw(this: *const ()) {
    let task = ManuallyDrop::new(task(this as *const Task));
    let mut status = task.0.status.load(Ordering::SeqCst);
    loop {
        match status {
            WAITING => {
                match task.0.status.compare_exchange(
                    WAITING,
                    POLLING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        let index = thread_rng().gen::<usize>() % 3;
                        task.0.injectors[index].push(clone_task(&*task.0));
                        // task.0.parker.notify_one();
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task.0.status.compare_exchange(
                    POLLING,
                    REPOLL,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

unsafe fn task(future: *const Task) -> ArcTask {
    ArcTask(Arc::from_raw(future))
}

unsafe fn clone_task(future: *const Task) -> ArcTask {
    let task = task(future);
    std::mem::forget(task.clone());
    task
}
