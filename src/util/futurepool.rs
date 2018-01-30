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

/// This mod implemented a wrapped future pool that supports `on_tick()` which is driven by
/// tasks and is invoked no less than the specific interval.

use std::fmt;
use std::cell::{Cell, RefCell, RefMut};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use futures::Future;
use futures_cpupool::{self as cpupool, CpuFuture, CpuPool};

use util;
use util::time::Instant;
use util::collections::HashMap;

pub trait Context: fmt::Debug + Send {
    /// Will be invoked periodically (no less than specified interval).
    /// When there is no task, it will NOT be invoked.
    fn on_tick(&mut self) {}
}

/// A delegator to wrap `Context` to provide `on_tick` feature.
#[derive(Debug)]
struct ContextDelegator<T: Context> {
    tick_interval: Duration,
    inner: RefCell<T>,
    last_tick: Cell<Option<Instant>>,
}

impl<T: Context> ContextDelegator<T> {
    fn new(context: T, tick_interval: Duration) -> ContextDelegator<T> {
        ContextDelegator {
            tick_interval,
            inner: RefCell::new(context),
            last_tick: Cell::new(None),
        }
    }

    fn get_context(&self) -> RefMut<T> {
        self.inner.borrow_mut()
    }

    fn on_task_finish(&self) {
        let now = Instant::now_coarse();
        let last_tick = self.last_tick.get();
        if last_tick.is_none() {
            // set last_tick when the first future is resolved
            self.last_tick.set(Some(now));
            return;
        }
        if now.duration_since(last_tick.unwrap()) < self.tick_interval {
            return;
        }
        self.last_tick.set(Some(now));
        self.get_context().on_tick();
    }
}

/// Each `ContextDelegator` instance is invoked individually for each thread.
/// It is never accessed concurrently so that we mark it as Sync.
unsafe impl<T: Context> Sync for ContextDelegator<T> {}

#[derive(Debug)]
pub struct ContextDelegators<T: Context> {
    delegators: Arc<HashMap<thread::ThreadId, ContextDelegator<T>>>,
}

impl<T: Context> Clone for ContextDelegators<T> {
    fn clone(&self) -> Self {
        ContextDelegators::<T> {
            delegators: Arc::clone(&self.delegators),
        }
    }
}

impl<T: Context> ContextDelegators<T> {
    fn new(delegators: HashMap<thread::ThreadId, ContextDelegator<T>>) -> Self {
        ContextDelegators::<T> {
            delegators: Arc::new(delegators),
        }
    }

    fn get_current_thread_delegator(&self) -> &ContextDelegator<T> {
        let thread_id = thread::current().id();
        if let Some(delegator) = self.delegators.get(&thread_id) {
            delegator
        } else {
            unreachable!();
        }
    }
}

/// A future thread pool that supports `on_tick` for each thread.
#[derive(Debug)]
pub struct FuturePool<T: Context + 'static> {
    pool: CpuPool,
    context_delegators: ContextDelegators<T>,
}

impl<T: Context + 'static> Clone for FuturePool<T> {
    fn clone(&self) -> FuturePool<T> {
        FuturePool::<T> {
            pool: self.pool.clone(),
            context_delegators: self.context_delegators.clone(),
        }
    }
}

impl<T: Context + 'static> util::AssertSend for FuturePool<T> {}
impl<T: Context + 'static> util::AssertSync for FuturePool<T> {}

impl<T: Context + 'static> FuturePool<T> {
    pub fn new<F>(
        pool_size: usize,
        stack_size: usize,
        name_prefix: &str,
        tick_interval: Duration,
        context_factory: F,
    ) -> FuturePool<T>
    where
        F: Send + 'static + Fn() -> T,
    {
        let (tx, rx) = mpsc::sync_channel(pool_size);
        let pool = cpupool::Builder::new()
            .pool_size(pool_size)
            .stack_size(stack_size)
            .name_prefix(name_prefix)
            .after_start(move || {
                // We only need to know each thread's id and we can build context later
                // by invoking `context_factory` in a !Sync way.
                let thread_id = thread::current().id();
                tx.send(thread_id).unwrap();
            })
            .create();
        let contexts = (0..pool_size)
            .map(|_| {
                let thread_id = rx.recv().unwrap();
                let context = context_factory();
                let context_delegator = ContextDelegator::new(context, tick_interval);
                (thread_id, context_delegator)
            })
            .collect();
        FuturePool {
            pool,
            context_delegators: ContextDelegators::new(contexts),
        }
    }

    pub fn spawn<F>(&self, f: F) -> CpuFuture<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        // TODO: Support busy check
        let delegators = self.context_delegators.clone();
        let f = f.then(move |r| {
            let delegator = delegators.get_current_thread_delegator();
            delegator.on_task_finish();
            r
        });
        self.pool.spawn(f)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use std::sync::mpsc::{channel, Sender};
    use futures::future;

    pub use super::*;

    fn spawn_long_time_future_and_wait<T: Context>(pool: &FuturePool<T>, future_duration_ms: u64) {
        pool.spawn(future::lazy(move || {
            thread::sleep(Duration::from_millis(future_duration_ms));
            future::ok::<(), ()>(())
        })).wait()
            .unwrap();
    }

    #[test]
    fn test_tick() {
        struct MyContext {
            tx: Sender<i32>,
            sn: i32,
        }
        impl fmt::Debug for MyContext {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "MyContext")
            }
        }
        impl Context for MyContext {
            fn on_tick(&mut self) {
                self.tx.send(self.sn).unwrap();
                self.sn += 1;
            }
        }

        let (tx, rx) = channel();

        let pool = FuturePool::new(
            1,
            1024000,
            "test-pool",
            Duration::from_millis(50),
            move || MyContext {
                tx: tx.clone(),
                sn: 0,
            },
        );
        assert!(rx.try_recv().is_err());

        // Tick is not emitted since there is no task
        thread::sleep(Duration::from_millis(100));
        assert!(rx.try_recv().is_err());

        // Tick is not emitted immediately for the first future
        spawn_long_time_future_and_wait(&pool, 10);
        assert!(rx.try_recv().is_err());

        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        assert!(rx.try_recv().is_err());

        // Even if there are tasks, tick is not emitted until next task arrives
        thread::sleep(Duration::from_millis(100));
        assert!(rx.try_recv().is_err());

        // Next task arrives && long time passed, tick is emitted
        spawn_long_time_future_and_wait(&pool, 100);
        assert_eq!(rx.try_recv().unwrap(), 0);
        assert!(rx.try_recv().is_err());

        // Tick is not emitted if there is no task
        thread::sleep(Duration::from_millis(100));
        assert!(rx.try_recv().is_err());

        // Tick is emitted since long enough time has passed
        spawn_long_time_future_and_wait(&pool, 1);
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());
    }
}
