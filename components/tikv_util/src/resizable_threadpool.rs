// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex, Weak};

use futures::Future;
use tokio::{
    io::Result as TokioResult,
    runtime::{Builder, Runtime},
};
use tokio_util::task::task_tracker::TaskTracker;

struct DeamonRuntime {
    inner: Option<Runtime>,
    tracker: TaskTracker,
}

impl DeamonRuntime {
    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.inner
            .as_ref()
            .unwrap()
            .spawn(self.tracker.track_future(fut));
    }
}

impl Drop for DeamonRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.inner.take() {
            runtime.shutdown_background();
        }
    }
}

#[derive(Clone)]
pub struct DeamonRuntimeHandle {
    inner: Weak<Mutex<DeamonRuntime>>,
}

impl DeamonRuntimeHandle {
    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let runtime = match self.inner.upgrade() {
            Some(runtime) => runtime,
            None => {
                error!("Daemon runtime has been dropped. Task will be ignored.");
                return;
            }
        };

        let (handle, tracker) = {
            let lock_guard = runtime.lock().unwrap();
            let inner = lock_guard
                .inner
                .as_ref()
                .expect("Runtime inner should exist");
            (inner.handle().clone(), lock_guard.tracker.clone())
        };

        handle.spawn(tracker.track_future(fut));
    }

    pub fn block_on<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let runtime = match self.inner.upgrade() {
            Some(runtime) => runtime,
            None => {
                error!("Daemon runtime has been dropped. Task will be ignored.");
                return;
            }
        };

        let (handle, tracker) = {
            let lock_guard = runtime.lock().unwrap();
            let inner = lock_guard
                .inner
                .as_ref()
                .expect("Runtime inner should exist");
            (inner.handle().clone(), lock_guard.tracker.clone())
        };

        handle.block_on(tracker.track_future(fut));
    }
}

pub struct ResizableRuntime {
    size: usize,
    count: usize,
    thread_name: String,
    gc_runtime: DeamonRuntime,
    current_runtime: Arc<Mutex<DeamonRuntime>>,
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
        let init_name = format!("{}-v0", thread_name);
        let keeper = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("rtkp")
            .enable_all()
            .build()
            .expect("Failed to create runtime-keeper");
        let new_runtime = (replace_pool_rule)(thread_size, &init_name)
            .unwrap_or_else(|_| panic!("failed to create tokio runtime {}", thread_name));

        ResizableRuntime {
            size: thread_size,
            count: 0,
            thread_name: thread_name.to_owned(),
            gc_runtime: DeamonRuntime {
                inner: Some(keeper),
                tracker: TaskTracker::new(),
            },
            current_runtime: Arc::new(Mutex::new(DeamonRuntime {
                inner: Some(new_runtime),
                tracker: TaskTracker::new(),
            })),
            replace_pool_rule,
            after_adjust,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handle = self.handle();
        handle.spawn(fut);
    }

    pub fn block_on<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handle = self.handle();
        handle.block_on(fut);
    }

    pub fn handle(&self) -> DeamonRuntimeHandle {
        DeamonRuntimeHandle {
            inner: Arc::downgrade(&self.current_runtime),
        }
    }

    // TODO: after tokio supports adjusting thread pool size(https://github.com/tokio-rs/tokio/issues/3329),
    //   adapt it.
    pub fn adjust_with(&mut self, new_size: usize) {
        if self.size == new_size {
            return;
        }

        let thread_name = format!("{}-v{}", self.thread_name, self.count + 1);
        let new_runtime = (self.replace_pool_rule)(new_size, &thread_name)
            .unwrap_or_else(|_| panic!("failed to create tokio runtime {}", thread_name));

        let old_runtime: DeamonRuntime;
        {
            let mut runtime_guard = self.current_runtime.lock().unwrap();
            if self.size == new_size {
                return;
            }

            old_runtime = std::mem::replace(
                &mut *runtime_guard,
                DeamonRuntime {
                    inner: Some(new_runtime),
                    tracker: TaskTracker::new(),
                },
            );
            self.size = new_size;
            self.count += 1;
        }

        info!(
            "Resizing thread pool";
            "thread_name" => &thread_name,
            "new_size" => new_size
        );
        self.gc_runtime.spawn(async move {
            old_runtime.tracker.close();
            old_runtime.tracker.wait().await;
            drop(old_runtime);
        });
        (self.after_adjust)(new_size);
    }
}

#[cfg(test)]
mod test {
    use std::{
        future,
        sync::atomic::{AtomicUsize, Ordering},
        thread::{self, sleep},
        time::Duration,
    };

    use super::*;
    use crate::time::Instant;

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
        static COUNTER: AtomicUsize = AtomicUsize::new(4);
        let after_adjust = |new_size: usize| {
            COUNTER.store(new_size, Ordering::SeqCst);
        };
        let mut threads = ResizableRuntime::new(
            COUNTER.load(Ordering::SeqCst),
            "test",
            Box::new(replace_pool_rule),
            Box::new(after_adjust),
        );
        assert_eq!(COUNTER.load(Ordering::SeqCst), threads.size());

        let handle = threads.handle();
        handle.block_on(async {
            COUNTER.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(COUNTER.load(Ordering::SeqCst), threads.size() + 1);

        threads.adjust_with(8);
        assert!(!threads.gc_runtime.tracker.is_empty());
        assert_eq!(COUNTER.load(Ordering::SeqCst), threads.size());

        sleep(Duration::from_secs(1));
        assert!(threads.gc_runtime.tracker.is_empty());

        // New task should be scheduled to the new runtime
        handle.block_on(async {
            COUNTER.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(COUNTER.load(Ordering::SeqCst), threads.size() + 1);
    }

    #[test]
    fn test_infinite_loop() {
        static COUNTER: AtomicUsize = AtomicUsize::new(4);
        let after_adjust = |new_size: usize| {
            COUNTER.store(new_size, Ordering::SeqCst);
        };
        let mut threads = ResizableRuntime::new(
            COUNTER.load(Ordering::SeqCst),
            "test",
            Box::new(replace_pool_rule),
            Box::new(after_adjust),
        );
        assert_eq!(COUNTER.load(Ordering::SeqCst), threads.size());

        let handle = threads.handle();
        // infinite loop should not be cleaned
        handle.spawn(async {
            future::pending::<()>().await;
        });

        threads.adjust_with(8);
        sleep(Duration::from_secs(1));
        assert!(!threads.gc_runtime.tracker.is_empty());
        assert_eq!(COUNTER.load(Ordering::SeqCst), threads.size());
    }

    #[test]
    fn test_drop() {
        let start = Instant::now();
        let threads =
            ResizableRuntime::new(4, "test", Box::new(replace_pool_rule), Box::new(|_| {}));
        let handle = threads.handle();
        let handle_clone = handle.clone();
        handle.spawn(async {
            future::pending::<()>().await;
        });
        let thread = thread::spawn(move || {
            handle_clone.block_on(async {
                future::pending::<()>().await;
            });
        });
        drop(threads);
        handle.spawn(async {
            future::pending::<()>().await;
        });
        handle.block_on(async {
            future::pending::<()>().await;
        });
        thread.join().unwrap();

        assert!(Instant::now() - start < Duration::from_secs(5));
    }

    #[test]
    fn test_multi_tasks() {
        let threads = Arc::new(ResizableRuntime::new(
            8,
            "test",
            Box::new(replace_pool_rule),
            Box::new(|_| {}),
        ));
        let handle = threads.handle();

        let handles: Vec<_> = (0..2000)
            .map(|i| {
                let runtime_handle = handle.clone();
                thread::spawn(move || {
                    if i % 2 == 0 {
                        runtime_handle.block_on(async move {
                            sleep(Duration::from_millis(500));
                            println!("Thread {} finished", i);
                        });
                    } else {
                        runtime_handle.spawn(async move {
                            sleep(Duration::from_millis(500));
                            println!("Thread {} finished", i);
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
