// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

/*!

Currently we does not support collecting CPU usage of threads for systems
other than Linux. PRs are welcome!

*/

use super::{ThreadBuildWrapper, TokioThreadBuildWrapper};
use std::{io, thread};

pub fn monitor_threads<S: Into<String>>(_: S) -> io::Result<()> {
    Ok(())
}

impl ThreadBuildWrapper for thread::Builder {
    fn spawn_wrapper<F, T>(self, f: F) -> io::Result<thread::JoinHandle<T>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        self.spawn(f)
    }
}

impl TokioThreadBuildWrapper for tokio_threadpool::Builder {
    fn after_start_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.after_start(f)
    }
}
