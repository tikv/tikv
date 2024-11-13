// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use futures::Future;
use tokio::{
    io::Result as TokioResult,
    runtime::{Handle, Runtime},
};

#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<RwLock<Option<Handle>>>,
}

pub struct ResizableRuntime {
    pub size: usize,
    thread_name: String,
    pool: RuntimeHandle,
    all_pools: Vec<Runtime>,
    replace_pool_rule: Box<dyn Fn(usize, &str) -> TokioResult<Runtime> + Send + Sync>,
    after_adjust: Box<dyn Fn(usize) + Send + Sync>,
}

impl RuntimeHandle {
    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handle = {
            let inner = self.inner.read().unwrap();
            inner.as_ref().cloned() // Clone the handle to avoid holding the lock
        };
        
        if let Some(handle) = handle {
            handle.spawn(fut);
        } else {
            panic!("runtime is not running");
        }
    }

    pub fn block_on<Fut>(&self, fut: Fut) -> Fut::Output
    where
        Fut: Future,
    {
        let handle = {
            let inner = self.inner.read().unwrap();
            inner.as_ref().cloned() // Clone the handle to avoid holding the lock
        };
        
        if let Some(handle) = handle {
            handle.block_on(fut)
        } else {
            panic!("runtime is not running");
        }
    }
}

impl ResizableRuntime {
    pub fn new(
        thread_size: usize,
        thread_name: &str,
        replace_pool_rule: Box<dyn Fn(usize, &str) -> TokioResult<Runtime> + Send + Sync>,
        after_adjust: Box<dyn Fn(usize) + Send + Sync>,
    ) -> Self {
        let mut ret = ResizableRuntime {
            size: 0,
            thread_name: thread_name.to_owned(),
            pool: RuntimeHandle {
                inner: Arc::new(RwLock::new(None)),
            },
            all_pools: Vec::new(),
            replace_pool_rule,
            after_adjust,
        };

        ret.adjust_with(thread_size);
        ret
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

    pub fn handle(&self) -> RuntimeHandle {
        self.pool.clone()
    }

    pub fn adjust_with(&mut self, new_size: usize) -> usize {
        if self.size == new_size {
            return new_size;
        }

        let new_pool = (self.replace_pool_rule)(new_size, &self.thread_name)
            .expect("failed to create tokio runtime for backup worker.");
        let handle = new_pool.handle().clone();
        self.all_pools.push(new_pool);

        {
            let mut pool_guard = self.pool.inner.write().unwrap();
            *pool_guard = Some(handle);
        }

        self.size = new_size;
        (self.after_adjust)(new_size);

        new_size
    }
}

impl Drop for ResizableRuntime {
    fn drop(&mut self) {
        for runtime in self.all_pools.drain(..) {
            runtime.shutdown_background();
        }
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
