// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

/*!

Currently we does not support collecting CPU usage of threads for systems
other than Linux. PRs are welcome!

*/

use std::io::{self, Result};

use collections::HashMap;
use futures::Future;

use super::ThreadBuildWrapper;
use crate::{
    metrics::StdThreadBuildWrapper,
    yatp_pool::{PoolTicker, YatpPoolBuilder},
};

pub fn monitor_threads<S: Into<String>>(_: S) -> io::Result<()> {
    Ok(())
}

pub struct ThreadInfoStatistics {}

impl ThreadInfoStatistics {
    pub fn new() -> Self {
        ThreadInfoStatistics {}
    }

    pub fn record(&mut self) {}

    pub fn get_cpu_usages(&self) -> HashMap<String, u64> {
        HashMap::default()
    }

    pub fn get_read_io_rates(&self) -> HashMap<String, u64> {
        HashMap::default()
    }

    pub fn get_write_io_rates(&self) -> HashMap<String, u64> {
        HashMap::default()
    }
}

impl Default for ThreadInfoStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl StdThreadBuildWrapper for std::thread::Builder {
    fn spawn_wrapper<F, T>(self, f: F) -> Result<std::thread::JoinHandle<T>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.spawn(|| f())
    }
}

impl<T: PoolTicker> ThreadBuildWrapper for YatpPoolBuilder<T> {
    fn after_start_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        // #[allow(clippy::disallowed_methods)]
        self.after_start(move || {
            f();
        })
    }

    fn before_stop_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        // #[allow(clippy::disallowed_methods)]
        self.before_stop(move || {
            f();
        })
    }
}

impl ThreadBuildWrapper for tokio::runtime::Builder {
    fn after_start_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.on_thread_start(move || {
            f();
        })
    }

    fn before_stop_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.on_thread_stop(move || {
            f();
        })
    }
}

impl ThreadBuildWrapper for futures::executor::ThreadPoolBuilder {
    fn after_start_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.after_start(move |_| {
            f();
        })
    }

    fn before_stop_wrapper<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        #[allow(clippy::disallowed_methods)]
        self.before_stop(move |_| {
            f();
        })
    }
}
