// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex, RwLock};

use futures::Future;
use tokio::{
    io::Result as TokioResult,
    runtime::{Handle, Runtime},
};

pub struct ResizableRuntime {
    pub size: usize,
    thread_name: String,
    pool: Arc<Mutex<Option<Handle>>>, // Use Arc<Mutex> for thread-safe updates
    all_pools: Vec<Runtime>,          // Keep track of all pools
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
        let mut ret = ResizableRuntime {
            size: 0,
            thread_name: thread_name.to_owned(),
            pool: Arc::new(Mutex::new(None)),
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
        let pool_guard = self.pool.lock().unwrap();
        if let Some(pool) = pool_guard.as_ref() {
            pool.spawn(fut);
        } else {
            panic!("ResizableRuntime: please call adjust_with() before spawn()");
        }
    }

    pub fn adjust_with(&mut self, new_size: usize) {
        if self.size == new_size {
            return;
        }

        let new_pool = (self.replace_pool_rule)(new_size, &self.thread_name)
            .expect("failed to create tokio runtime for backup worker.");
        let handle = new_pool.handle().clone();
        self.all_pools.push(new_pool);

        {
            let mut pool_guard = self.pool.lock().unwrap();
            *pool_guard = Some(handle);
        }

        self.size = new_size;
        (self.after_adjust)(new_size);
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        let pool_guard = self.pool.lock().unwrap();
        if let Some(pool) = pool_guard.as_ref() {
            pool.block_on(f)
        } else {
            panic!("ResizableRuntime: please call adjust_with() before block_on()");
        }
    }
}

impl Drop for ResizableRuntime {
    fn drop(&mut self) {
        self.all_pools.clear(); // Drop all previously used pools
    }
}

#[derive(Clone)]
pub struct ResizableRuntimeHandle {
    inner: Arc<RwLock<ResizableRuntime>>,
}

impl ResizableRuntimeHandle {
    pub fn new(runtime: ResizableRuntime) -> Self {
        ResizableRuntimeHandle {
            inner: Arc::new(RwLock::new(runtime)),
        }
    }

    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let inner = self.inner.read().unwrap();
        inner.spawn(fut);
    }

    pub fn adjust_with(&self, new_size: usize) {
        let mut inner = self.inner.write().unwrap();
        inner.adjust_with(new_size);
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        let inner = self.inner.read().unwrap();
        inner.block_on(f)
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
        assert_eq!(COUNTER.load(Ordering::SeqCst), 4);
        threads.adjust_with(8);
        assert_eq!(COUNTER.load(Ordering::SeqCst), 8);
    }
}
