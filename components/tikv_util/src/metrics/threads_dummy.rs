// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

/*!

Currently we does not support collecting CPU usage of threads for systems
other than Linux. PRs are welcome!

*/

use std::io;

use collections::HashMap;

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
