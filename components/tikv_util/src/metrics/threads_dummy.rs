// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

/*!

Currently we does not support collecting CPU usage of threads for systems
other than Linux. PRs are welcome!

*/

use super::ThreadSpawnWrapper;
use std::{io, thread};

pub fn monitor_threads<S: Into<String>>(_: S) -> io::Result<()> {
    Ok(())
}

impl ThreadSpawnWrapper for thread::Builder {
    fn spawn_wrapper<F, T>(self, f: F) -> io::Result<thread::JoinHandle<T>>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        self.spawn(f)
    }
}
