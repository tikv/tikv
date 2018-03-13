// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{Error, ErrorKind, Result};
use std::sync::Mutex;
use prometheus::{self, proto, Collector, Desc, Gauge, Opts};
use libc::{self, pid_t};
use procinfo::pid;

fn to_err(s: String) -> Error {
    Error::new(ErrorKind::Other, s)
}

/// monitor current process's memory usage.
pub fn monitor_memory<S: Into<String>>(namespace: S) -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let mc = MemoryColletcor::new(pid, namespace);
    prometheus::register(Box::new(mc)).map_err(|e| to_err(format!("{:?}", e)))?;

    Ok(())
}

struct MemoryColletcor {
    pid: pid_t,
    desc: Desc,
    memory_total: Mutex<Gauge>,
}

impl MemoryColletcor {
    fn new<S: Into<String>>(pid: pid_t, namespace: S) -> MemoryColletcor {
        let memory_total = Gauge::with_opts(
            Opts::new(
                "tikv_memory_usage_seconds_total",
                "Total memory usage in tikv",
            ).namespace(namespace),
        ).unwrap();
        let desc = memory_total.desc()[0].clone();

        MemoryColletcor {
            pid: pid,
            desc: desc,
            memory_total: Mutex::new(memory_total),
        }
    }

    #[cfg(test)]
    pub fn get_memory_total(&self) -> u64 {
        let memory_total = self.memory_total.lock().unwrap();
        memory_total.get() as u64
    }
}

impl Collector for MemoryColletcor {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let memory_total = self.memory_total.lock().unwrap();
        if let Ok(status) = pid::status(self.pid) {
            memory_total.set(status.vm_rss as f64); // get memory usage size by KB
        }
        memory_total.collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libc;

    #[test]
    fn test_memory_collector() {
        let pid = unsafe { libc::getpid() };
        let mc = MemoryColletcor::new(pid, "test");

        let _ = mc.collect();
        assert!(mc.get_memory_total() > 0);
    }
}
