// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}, Mutex}, collections::HashSet, time::Duration, hash::{Hasher, Hash}};

use futures::Future;
use tokio::{
    io::Result as TokioResult,
    runtime::{Handle, Runtime, Builder},
    time::interval,
};

#[derive(Clone)]
pub struct RuntimeHandle {
    name: String,
    inner: Arc<Mutex<Option<Runtime>>>,  
    task_count: Arc<AtomicUsize>,
}

impl PartialEq for RuntimeHandle {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for RuntimeHandle {}

impl Hash for RuntimeHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl RuntimeHandle {
    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handle = {
            let inner = self.inner.lock().unwrap();
            inner.as_ref().map(|runtime| runtime.handle().clone())
        };

        self.task_count.fetch_add(1, Ordering::SeqCst);
        let task_count = self.task_count.clone();

        if let Some(handle) = handle {
            handle.spawn(async move {
                fut.await;
                task_count.fetch_sub(1, Ordering::SeqCst);
            });
        } else {
            panic!("runtime is not running");
        }
    }

    pub fn block_on<Fut>(&self, fut: Fut) -> Fut::Output
    where
        Fut: Future,
    {
        let handle = {
            let inner = self.inner.lock().unwrap();
            inner.as_ref().map(|runtime| runtime.handle().clone())
        };

        self.task_count.fetch_add(1, Ordering::SeqCst);
        let task_count = self.task_count.clone();
        if let Some(handle) = handle {
            handle.block_on( {
                async move {
                    let res = fut.await;
                    task_count.fetch_sub(1, Ordering::SeqCst);
                    res
                }
            })
        } else {
            panic!("runtime is not running");
        }
    }
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(runtime) = inner.take() {
            runtime.shutdown_background();
        }
    }
}

pub struct ResizableRuntime {
    pub size: usize,
    count: usize,
    thread_name: String,
    pool: Arc<RwLock<Option<RuntimeHandle>>>,
    pools: Arc<Mutex<HashSet<RuntimeHandle>>>,
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
        .thread_name("management-runtime")
        .enable_all()
        .build()
        .expect("Failed to create management runtime");

        let keeper_handle = keeper.handle().clone();
        let pools = Arc::new(Mutex::new(HashSet::new()));
        
        let mut ret = ResizableRuntime {
            size: 0,
            count: 0,
            thread_name: thread_name.to_owned(),
            pool: Arc::new(RwLock::new(None)),
            pools: pools.clone(),
            keeper: keeper,
            replace_pool_rule: replace_pool_rule,
            after_adjust: after_adjust,
        };

        ret.adjust_with(thread_size);
        
        let pools_clone = pools.clone();
        keeper_handle.spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                let to_clean = {
                    let pools_guard = pools_clone.lock().unwrap();
                    pools_guard
                        .iter()
                        .filter(|p| p.task_count.load(Ordering::SeqCst) == 0)
                        .cloned()
                        .collect::<Vec<_>>()
                };

                for p in to_clean {
                    pools_clone.lock().unwrap().remove(&p);
                    drop(p);
                }
            }
        });
        ret
    }

    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.pool.read().unwrap().as_ref().unwrap().spawn(fut)
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        self.pool.read().unwrap().as_ref().unwrap().block_on(f)
    }

    pub fn handle(&self) -> RuntimeHandle {
        self.pool.read().unwrap().as_ref().unwrap().clone()
    }

    pub fn adjust_with(&mut self, new_size: usize) -> usize {
        if self.size == new_size {
            return new_size;
        }

        self.count += 1;
        let thread_name = self.thread_name.to_string() +"-" + &self.count.to_string();
        let new_pool = (self.replace_pool_rule)(new_size, thread_name.as_str())
            .expect("failed to create tokio runtime for backup worker.");

        //insert the old pool to keeper
        if let Some(pool) = self.pool.write().unwrap().take(){
            self.pools.lock().unwrap().insert(pool);
        }
        *self.pool.write().unwrap() = Some(RuntimeHandle {
            name: thread_name,
            inner: Arc::new(Mutex::new(Some(new_pool))),
            task_count: Arc::new(AtomicUsize::new(0)),
        });

        self.size = new_size;
        (self.after_adjust)(new_size);

        new_size
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn test_adjust_thread_num() {
        let replace_pool_rule = |thread_count: usize, thread_name: &str| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(thread_count)
                .thread_name(thread_name)
                .enable_all()
                .build()
                .unwrap();
            Ok(rt)
        };
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
        threads.adjust_with(8);
        handle.block_on(async {
            assert_eq!(COUNTER.load(Ordering::SeqCst), 8);
        });
    }
}
