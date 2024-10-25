// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use futures::Future;
use tokio::{io::Result as TokioResult, runtime::Runtime};

/// DaemonRuntime is a "background" runtime, which contains "daemon" tasks:
/// any task spawn into it would run until finish even the runtime isn't
/// referenced.
pub struct DaemonRuntime(Option<Runtime>);

impl DaemonRuntime {
    /// spawn a daemon task to the runtime.
    pub fn spawn(self: &Arc<Self>, f: impl Future<Output = ()> + Send + 'static) {
        let wkr = self.clone();
        self.0.as_ref().unwrap().spawn(async move {
            f.await;
            drop(wkr)
        });
    }

    /// create a daemon runtime from some runtime.
    pub fn from_runtime(rt: Runtime) -> Arc<Self> {
        Arc::new(Self(Some(rt)))
    }
}

impl Drop for DaemonRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

pub struct ResizableRuntime {
    pub size: usize,
    thread_name: String,
    pool: Option<Arc<DaemonRuntime>>,
    replace_pool_rule: Box<dyn Fn(usize, &str) -> TokioResult<Runtime> + Send + Sync>,
    after_adjust: Box<dyn Fn(usize) + Send + Sync>,
}

impl ResizableRuntime {
    pub fn new(
        thread_name: &str,
        replace_pool_rule: Box<dyn Fn(usize, &str) -> TokioResult<Runtime> + Send + Sync>,
        after_adjust: Box<dyn Fn(usize) + Send + Sync>,
    ) -> Self {
        ResizableRuntime {
            size: 0,
            thread_name: thread_name.to_owned(),
            pool: None,
            replace_pool_rule,
            after_adjust,
        }
    }

    pub fn spawn<Fut>(&self, fut: Fut)
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.pool
            .as_ref()
            .expect("ResizableRuntime: please call adjust_with() before spawn()")
            .spawn(fut);
    }

    /// Lazily adjust the thread pool's size
    pub fn adjust_with(&mut self, new_size: usize) {
        if self.size == new_size {
            return;
        }
        // TODO: after tokio supports adjusting thread pool size(https://github.com/tokio-rs/tokio/issues/3329),
        //   adapt it.
        let pool = (self.replace_pool_rule)(new_size, &self.thread_name)
            .expect("failed to create tokio runtime for backup worker.");
        self.pool = Some(DaemonRuntime::from_runtime(pool));
        self.size = new_size;
        (self.after_adjust)(new_size);
    }

    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future,
    {
        self.pool
            .as_ref()
            .expect("ResizableRuntime: please call adjust_with() before block_on()")
            .0
            .as_ref()
            .unwrap()
            .block_on(f)
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
        let mut threads =
            ResizableRuntime::new("test", Box::new(replace_pool_rule), Box::new(after_adjust));
        threads.adjust_with(4);
        assert_eq!(COUNTER.load(Ordering::SeqCst), 4);
        threads.adjust_with(8);
        assert_eq!(COUNTER.load(Ordering::SeqCst), 8);
    }
}
