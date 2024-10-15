// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::sys::thread::ThreadBuildWrapper;
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
    workers: Option<Arc<DaemonRuntime>>,
    thread_name: String,
    after_adjust: fn(usize), 
    io_type: fn(),
}

impl ResizableRuntime
{
    pub fn new(thread_name: &str, after_adjust: fn(usize), io_type: fn()) -> Self {
        ResizableRuntime {
            size: 0,
            thread_name: thread_name.to_string(),
            workers: None,
            after_adjust,
            io_type,
        }
    }

    pub fn spawn<Func>(&self, func: Func)
    where
        Func: Future<Output = ()> + Send + 'static,
    {
        self.workers
            .as_ref()
            .expect("ResizableRuntime: please call adjust_with() before spawn()")
            .spawn(func);
    }

    /// Lazily adjust the thread pool's size
    pub fn adjust_with(&mut self, new_size: usize)
    {
        // TODO: after tokio supports adjusting thread pool size(https://github.com/tokio-rs/tokio/issues/3329),
        //   adapt it.
        let workers = create_tokio_runtime(new_size, &self.thread_name, self.io_type)
            .expect("failed to create tokio runtime for backup worker.");
        
        self.workers = Some(DaemonRuntime::from_runtime(workers));
        self.size = new_size;
        (self.after_adjust)(new_size);
    }
}

/// Create a standard tokio runtime.
/// (which allows io and time reactor, involve thread memory accessor),
pub fn create_tokio_runtime(
    thread_count: usize,
    thread_name: &str,
    io_type: fn(),
) -> TokioResult<Runtime>
    {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        .enable_io()
        .enable_time()
        .with_sys_and_custom_hooks(
            move || {
                io_type();
            },
            || {}
        )
        .worker_threads(thread_count)
        .build()
}
