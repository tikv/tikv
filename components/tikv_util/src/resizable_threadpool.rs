// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

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
        // it is safe because all tasks should be finished.
        self.0.take().unwrap().shutdown_background()
    }
}
pub struct ResizableRuntime
{
    pub size: usize,
    thread_name: String,
    pool: Option<Arc<DaemonRuntime>>,
    after_adjust: fn(usize), 
    replace_pool_rule: fn(usize,&str) -> TokioResult<Runtime>,
}

impl ResizableRuntime
{
    pub fn new(thread_name: &str, after_adjust: fn(usize), replace_pool_rule: fn(usize, &str) -> TokioResult<Runtime>) -> Self {
        ResizableRuntime {
            size: 0,
            thread_name: thread_name.to_owned(),
            pool: None,
            after_adjust,
            replace_pool_rule,
        }
    }

    pub fn spawn<Func>(&self, func: Func)
    where
        Func: Future<Output = ()> + Send + 'static,
    {
        self.pool
            .as_ref()
            .expect("ResizableRuntime: please call adjust_with() before spawn()")
            .spawn(func);
    }

    /// Lazily adjust the thread pool's size
    pub fn adjust_with(&mut self, new_size: usize)
    {
        // TODO: after tokio supports adjusting thread pool size(https://github.com/tokio-rs/tokio/issues/3329),
        //   adapt it.
        let pool = (self.replace_pool_rule)(self.size,&self.thread_name).expect("failed to create tokio runtime for backup worker.");
        self.pool = Some(DaemonRuntime::from_runtime(pool));
        self.size = new_size;
        (self.after_adjust)(new_size);
    }
}
