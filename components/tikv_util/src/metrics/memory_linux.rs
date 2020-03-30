// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This module is a subset of rust-prometheus's process collector, without the fd collector
//! to avoid memory fragmentation issues when open fd is large.

use std::fs;
use std::io::Read;
use std::sync::Mutex;

use libc;
use procinfo::pid as pid_info;

use counter::Counter;
use desc::Desc;
use errors::{Error, Result};
use gauge::Gauge;
use libc::pid_t;
use metrics::{Collector, Opts};
use proto;

/// Monitors threads of the current process.
pub fn monitor_memory<S: Into<String>>(namespace: S) -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let tc = MemoryCollector::new(pid, namespace);
    prometheus::register(Box::new(tc)).map_err(|e| to_io_err(format!("{:?}", e)))
}

/// A collector to collect memory metrics.
pub struct MemoryCollector {
    pid: pid_t,
    descs: Vec<Desc>,
    vsize: Gauge,
    rss: Gauge,
}

impl MemoryCollector {
    /// Create a `ProcessCollector` with the given process id and namespace.
    pub fn new<S: Into<String>>(pid: pid_t, namespace: S) -> ProcessCollector {
        let namespace = namespace.into();
        let mut descs = Vec::new();

        let vsize = Gauge::with_opts(
            Opts::new(
                "memory_virtual_memory_bytes",
                "Virtual memory size in bytes.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(vsize.desc().into_iter().cloned());

        let rss = Gauge::with_opts(
            Opts::new(
                "memory_resident_memory_bytes",
                "Resident memory size in bytes.",
            )
            .namespace(namespace.clone()),
        )
        .unwrap();
        descs.extend(rss.desc().into_iter().cloned());

        ProcessCollector {
            pid,
            descs,
            vsize,
            rss,
        }
    }
}

impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        if let Ok(statm) = pid_info::statm(self.pid) {
            self.vsize.set(statm.size as f64 * *PAGESIZE);
            self.rss.set(statm.resident as f64 * *PAGESIZE);
        }

        let mut mfs = Vec::with_capacity(2);
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs
    }
}

lazy_static! {
    static ref PAGESIZE: f64 = { unsafe { libc::sysconf(libc::_SC_PAGESIZE) as f64 } };
}
