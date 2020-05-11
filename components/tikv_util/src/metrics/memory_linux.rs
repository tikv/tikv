// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This module is a subset of rust-prometheus's process collector, without the fd collector
//! to avoid memory fragmentation issues when open fd is large.

use std::io::{Error, ErrorKind, Result};

use libc;
use procinfo::pid as pid_info;

use prometheus::core::{Collector, Desc};
use prometheus::{proto, Gauge, Opts};

/// Monitors memory of the current process.
pub fn monitor_memory() -> Result<()> {
    let pid = unsafe { libc::getpid() };
    let tc = MemoryCollector::new(pid);
    prometheus::register(Box::new(tc)).map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

/// A collector to collect memory metrics.
pub struct MemoryCollector {
    pid: libc::pid_t,
    descs: Vec<Desc>,
    vsize: Gauge,
    rss: Gauge,
}

impl MemoryCollector {
    pub fn new(pid: libc::pid_t) -> Self {
        let mut descs = Vec::new();

        let vsize = Gauge::with_opts(Opts::new(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
        ))
        .unwrap();
        descs.extend(vsize.desc().into_iter().cloned());

        let rss = Gauge::with_opts(Opts::new(
            "process_resident_memory_bytes",
            "Resident memory size in bytes.",
        ))
        .unwrap();
        descs.extend(rss.desc().into_iter().cloned());

        Self {
            pid,
            descs,
            vsize,
            rss,
        }
    }
}

impl Collector for MemoryCollector {
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
