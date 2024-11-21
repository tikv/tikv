// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

use futures::Future;
use tokio::{
    io::Result as TokioResult,
    runtime::{Builder, Runtime},
    time::interval,
};

struct DeamonRuntime {
    inner: Option<Runtime>,
    task_count: Arc<AtomicUsize>,
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
    inner: Weak<DeamonRuntime>,
}

impl DeamonRuntimeHandle {
    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
        {
            let rc_runtime = match self.inner.upgrade() {
                Some(runtime) => runtime,
                None => return,
            };
        
            let inner = match rc_runtime.inner.as_ref() {
                Some(inner) => inner,
                None => return,
            };
        
            let handle = inner.handle().clone();
            let task_count = rc_runtime.task_count.clone();
        
            task_count.fetch_add(1, Ordering::SeqCst);
            
            handle.spawn(async move {
                fut.await;
                task_count.fetch_sub(1, Ordering::SeqCst);
            });
        }

    pub fn block_on<Fut>(&self, fut: Fut) -> Option<Fut::Output>
    where
        Fut: Future,
    {
        let (handle, task_count) = {
            let rc_runtime = self.inner.upgrade()?;
            let inner = rc_runtime.inner.as_ref()?;
            let handle = inner.handle().clone();
            let task_count = rc_runtime.task_count.clone();
            (handle, task_count)
        };    
    
        task_count.fetch_add(1, Ordering::SeqCst);
    
        Some(handle.block_on(async move {
            let output = fut.await;
            task_count.fetch_sub(1, Ordering::SeqCst);
            output
        }))
    }
}

pub struct ResizableRuntime {
    pub size: usize,
    count: usize,
    thread_name: String,
    current_runtime: Arc<DeamonRuntime>,
    used_runtime: Arc<Mutex<Vec<Arc<DeamonRuntime>>>>,
    keeper: Option<Runtime>,
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
        let init_thread_name = format!("{}-v0-{}", thread_name, thread_size);
        let current_runtime = replace_pool_rule(thread_size, init_thread_name.as_str()).expect(
            "failed to create tokio runtime for backup worker.");
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
            current_runtime: Arc::new(DeamonRuntime {
                inner: Some(current_runtime),
                task_count: Arc::new(AtomicUsize::new(0)),
            }),
            used_runtime: Arc::new(Mutex::new(Vec::new())),
            keeper: Some(keeper),
            replace_pool_rule,
            after_adjust,
        };

        ret.adjust_with(thread_size);
        ret.start_clean_loop();
        ret
    }

    fn start_clean_loop(&self) {
        let pools_clone = Arc::downgrade(&self.used_runtime);
        let handle = self.keeper.as_ref().unwrap().handle().clone();
        handle.spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                if let Some(pools) = pools_clone.upgrade() {
                    pools.lock().unwrap().retain(|handle| {
                        handle.task_count.load(Ordering::SeqCst) > 0
                    });
                }
            }
        });
    }

    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handle = self.handle();
        handle.spawn(fut);
    }

    pub fn block_on<Fut>(&self, fut: Fut) -> Option<Fut::Output>
    where
        Fut: Future,
    {
        let handle = self.handle();
        handle.block_on(fut)
    }

    pub fn handle(&self) -> DeamonRuntimeHandle {
        DeamonRuntimeHandle {
            inner: Arc::downgrade(&self.current_runtime),
        }
    }

    pub fn adjust_with(&mut self, new_size: usize) -> usize {
        if self.size == new_size {
            return new_size;
        }

        {
            let mut used_runtime_guard = self.used_runtime.lock().unwrap();

            self.count += 1;
            let thread_name = format!("{}-{}-{}", self.thread_name, self.count, new_size,);
            let new_pool = (self.replace_pool_rule)(new_size, thread_name.as_str())
                .expect("failed to create tokio runtime for backup worker.");

            used_runtime_guard.push(self.current_runtime.clone());

            self.current_runtime = Arc::new(DeamonRuntime {
                inner: Some(new_pool),
                task_count: Arc::new(AtomicUsize::new(0)),
            });

            info!(
                "Resizing thread pool";
                "thread_name" => thread_name.as_str(),
                "new_size" => new_size
            );
        }

        self.size = new_size;
        (self.after_adjust)(new_size);

        new_size
    }
}

impl Drop for ResizableRuntime {
    fn drop(&mut self) {
        if let Some(keeper) = self.keeper.take() {
            keeper.shutdown_background();
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        thread::{self, sleep},
    };

    use crate::time::Instant;

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
        assert!(!threads.used_runtime.lock().unwrap().is_empty());

        // The old pool should be added into the pools
        threads.adjust_with(8);
        assert!(!threads.used_runtime.lock().unwrap().is_empty());

        // The old pool should be cleaned after 10s
        sleep(Duration::from_secs(12));
        assert!(threads.used_runtime.lock().unwrap().is_empty());
        handle.block_on(async {
            assert_eq!(COUNTER.load(Ordering::SeqCst), 8);
        });
    }

    #[test]
    fn test_infinite_loop() {
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
        // infinite loop should not be cleaned
        handle.spawn(async {
            loop {
                sleep(Duration::from_secs(10));
            }
        });

        // The old pool should be added into the pools
        threads.adjust_with(8);
        assert!(!threads.used_runtime.lock().unwrap().is_empty());

        // The old pool should be cleaned after 10s
        sleep(Duration::from_secs(12));
        assert!(!threads.used_runtime.lock().unwrap().is_empty());
        handle.block_on(async {
            assert_eq!(COUNTER.load(Ordering::SeqCst), 8);
        });
    }

    #[test]
    fn test_drop() {
        let start = Instant::now();
        let threads = ResizableRuntime::new(
            4,
            "test",
            Box::new(replace_pool_rule),
            Box::new(|_| {}),
        );
        let handle = threads.handle();
        let handle_clone = handle.clone();
        handle.spawn(async {
            sleep(Duration::from_secs(10));
        });
        let thread = thread::spawn(move ||{
            handle_clone.block_on(async{
                sleep(Duration::from_secs(10));
            });
        });
        drop(threads);
        handle.spawn(async{
            sleep(Duration::from_secs(10));
        });
        handle.block_on(async{
            sleep(Duration::from_secs(10));
        });
        thread.join().unwrap();
        assert!(Instant::now() - start < Duration::from_secs(10));
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
