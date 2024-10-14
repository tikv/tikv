// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::Future;
use tikv_util::{error, sys::thread::ThreadBuildWrapper};
use tokio::{io::Result as TokioResult, runtime::Runtime};
use txn_types::{Key, TimeStamp};

use crate::Result;

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
pub struct ResizableRuntime {
    pub(crate) size: usize,
    workers: Option<Arc<DaemonRuntime>>,
}

impl ResizableRuntime {
    pub fn new() -> Self {
        ResizableRuntime {
            size: 0,
            workers: None,
        }
    }

    pub fn spawn<F>(&self, func: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.workers
            .as_ref()
            .expect("ResizableRuntime: please call adjust_with() before spawn()")
            .spawn(func);
    }

    /// Lazily adjust the thread pool's size
    pub fn adjust_with<F>(&mut self, new_size: usize, after_adjust: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        // TODO: after tokio supports adjusting thread pool size(https://github.com/tokio-rs/tokio/issues/3329),
        //   adapt it.
        let workers = create_tokio_runtime(new_size, "bkwkr")
            .expect("failed to create tokio runtime for backup worker.");
        
        self.workers = Some(DaemonRuntime::from_runtime(workers));
        self.size = new_size;
        after_adjust();
    }
}

/// Create a standard tokio runtime.
/// (which allows io and time reactor, involve thread memory accessor),
/// and set the I/O type to export.
pub fn create_tokio_runtime<F1, F2>(
    thread_count: usize,
    thread_name: &str,
    start_hook: Option<F1>,
    stop_hook: Option<F2>,
) -> TokioResult<Runtime>
where
    F1: Fn() + Send + Sync + 'static,
    F2: Fn() + Send + Sync + 'static,
{
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        .enable_io()
        .enable_time()
        .with_sys_and_custom_hooks(
            start_hook.unwrap_or_else(|| || {}),  // Default to no-op
            stop_hook.unwrap_or_else(|| || {})    // Default to no-op
        )
        .worker_threads(thread_count)
        .build()
}
