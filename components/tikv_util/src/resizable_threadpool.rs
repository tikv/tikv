// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use futures::Future;
use tokio::{
    io::Result as TokioResult,
    runtime::{Builder, Handle, Runtime},
    time::interval,
};

pub struct RcRuntime {
    inner: Option<Runtime>,
    task_count: Arc<AtomicUsize>,
}

impl RcRuntime {
    pub fn new(runtime: Runtime) -> Self {
        Self {
            inner: Some(runtime),
            task_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn handle(&self) -> Option<Handle> {
        self.inner.as_ref().map(|runtime| runtime.handle().clone())
    }
}

impl Drop for RcRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.inner.take() {
            // Safely take ownership
            runtime.shutdown_background();
        }
    }
}

#[derive(Clone)]
pub struct RcRuntimeHandle {
    name: String,
    inner: Arc<Mutex<Option<RcRuntime>>>,
}

impl PartialEq for RcRuntimeHandle {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for RcRuntimeHandle {}

impl Hash for RcRuntimeHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl RcRuntimeHandle {
    pub fn new(name: &str) -> Self {
        RcRuntimeHandle {
            name: name.to_owned(),
            inner: Arc::new(Mutex::new(None)),
        }
    }

    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let task_count: Arc<AtomicUsize>;
        let handle: Handle;

        {
            let lock = self.inner.lock().unwrap();
            if let Some(rc_runtime) = lock.as_ref() {
                task_count = rc_runtime.task_count.clone();
                handle = rc_runtime.handle().expect("Failed to get runtime handle");
                task_count.fetch_add(1, Ordering::SeqCst);
            } else {
                panic!("RcRuntime is not set");
            }
        }

        handle.spawn(async move {
            fut.await;
            task_count.fetch_sub(1, Ordering::SeqCst);
        });
    }

    pub fn block_on<Fut>(&self, fut: Fut) -> Fut::Output
    where
        Fut: Future,
    {
        let task_count: Arc<AtomicUsize>;
        let handle: Handle;

        {
            let lock = self.inner.lock().unwrap();
            if let Some(rc_runtime) = lock.as_ref() {
                task_count = rc_runtime.task_count.clone();
                handle = rc_runtime.handle().expect("Failed to get runtime handle");
                task_count.fetch_add(1, Ordering::SeqCst);
            } else {
                panic!("RcRuntime is not set");
            }
        }

        handle.block_on(async move {
            let output = fut.await;
            task_count.fetch_sub(1, Ordering::SeqCst);
            output
        })
    }
}

pub struct ResizableRuntime {
    pub size: usize,
    count: usize,
    thread_name: String,
    pool: RcRuntimeHandle,
    pools: Arc<Mutex<HashSet<RcRuntimeHandle>>>,
    keeper: Runtime,
    replace_pool_rule: Box<dyn Fn(usize, &str) -> TokioResult<Runtime> + Send + Sync>,
    after_adjust: Box<dyn Fn(usize) + Send + Sync>,
}

impl ResizableRuntime {
    pub fn new(
        thread_size: usize,
        thread_name: &str,
        replace_pool_rule: Box<dyn Fn(usize, &str) -> TokioResult<Runtime> + Send + Sync>,
        after_adjust: Box<dyn Fn(usize) + Send + Sync>,
    ) -> Self {
        let keeper = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("rtkp")
            .enable_all()
            .build()
            .expect("Failed to create runtime-keeper");

        let mut ret = ResizableRuntime {
            size: 0,
            count: 0,
            thread_name: thread_name.to_owned(),
            pool: RcRuntimeHandle {
                name: thread_name.to_owned() + "-init",
                inner: Arc::new(Mutex::new(None)),
            },
            pools: Arc::new(Mutex::new(HashSet::new())),
            keeper,
            replace_pool_rule,
            after_adjust,
        };

        ret.adjust_with(thread_size);
        ret.start_clean_loop();
        ret
    }

    fn start_clean_loop(&self) {
        let pools_clone = self.pools.clone();
        self.keeper.spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                let mut pools = pools_clone.lock().unwrap();
                pools.retain(|handle| {
                    if let Some(rc_runtime) = handle.inner.lock().unwrap().as_ref() {
                        rc_runtime.task_count.load(Ordering::SeqCst) > 0
                    } else {
                        false
                    }
                });
            }
        });
    }

    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.pool.spawn(fut);
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        self.pool.block_on(f)
    }

    pub fn handle(&self) -> RcRuntimeHandle {
        self.pool.clone()
    }

    pub fn adjust_with(&mut self, new_size: usize) -> usize {
        if self.size == new_size {
            return new_size;
        }

        self.count += 1;
        let thread_name = self.thread_name.to_string()
            + "-"
            + &self.count.to_string()
            + "-"
            + &new_size.to_string();
        let new_pool = (self.replace_pool_rule)(new_size, thread_name.as_str())
            .expect("failed to create tokio runtime for backup worker.");

        self.pools.lock().unwrap().insert(self.pool.clone());
        *self.pool.inner.lock().unwrap() = Some(RcRuntime {
            inner: Some(new_pool),
            task_count: Arc::new(AtomicUsize::new(0)),
        });
        self.pool.name = thread_name;

        self.size = new_size;
        (self.after_adjust)(new_size);

        new_size
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread::{self, sleep},
    };

    use super::*;

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn replace_pool_rule(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count)
            .thread_name(thread_name)
            .enable_all()
            .build()
            .unwrap();
        Ok(rt)
    }

    #[test]
    fn test_adjust_thread_num() {
        let after_adjust = |new_size: usize| {
            COUNTER.store(new_size, Ordering::SeqCst);
        };
        let mut threads = ResizableRuntime::new(
            4,
            "test",
            Box::new(replace_pool_rule),
            Box::new(after_adjust),
        );
        let handle = threads.handle();
        assert_eq!(COUNTER.load(Ordering::SeqCst), 4);
        handle.block_on(async {
            assert_eq!(COUNTER.load(Ordering::SeqCst), 4);
        });
        assert!(!threads.pools.lock().unwrap().is_empty());

        // The old pool should be added into the pools
        threads.adjust_with(8);
        assert!(!threads.pools.lock().unwrap().is_empty());

        // The old pool should be cleaned after 10s
        sleep(Duration::from_secs(12));
        assert!(threads.pools.lock().unwrap().is_empty());
        handle.block_on(async {
            assert_eq!(COUNTER.load(Ordering::SeqCst), 8);
        });
    }

    #[test]
    fn test_multi_tasks() {
        let threads =
            ResizableRuntime::new(32, "test", Box::new(replace_pool_rule), Box::new(|_| {}));
        let handle = threads.handle();

        let handles: Vec<_> = (0..32)
            .map(|i| {
                let runtime_handle = handle.clone();
                thread::spawn(move || {
                    if i % 2 == 0 {
                        runtime_handle.block_on(async move {
                            println!("Thread {} sleeping", i);
                            sleep(Duration::from_secs(10));
                            println!("Thread {} finished sleeping", i);
                        });
                    } else {
                        runtime_handle.spawn(async move {
                            println!("Thread {} sleeping", i);
                            sleep(Duration::from_secs(10));
                            println!("Thread {} finished sleeping", i);
                        })
                    }
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }
}
