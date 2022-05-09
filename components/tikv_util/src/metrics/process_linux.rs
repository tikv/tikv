// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This module is a subset of rust-prometheus's process collector, without the fd collector
//! to avoid memory fragmentation issues when open fd is large.

use std::io::{Error, ErrorKind, Result};

use prometheus::{
    core::{Collector, Desc},
    proto, IntCounter, IntGauge, Opts,
};

use crate::sys::thread;

/// Monitors current process.
pub fn monitor_process() -> Result<()> {
    let pc = ProcessCollector::new();
    prometheus::register(Box::new(pc)).map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

/// A collector to collect process metrics.
pub struct ProcessCollector {
    descs: Vec<Desc>,
    cpu_total: IntCounter,
    vsize: IntGauge,
    rss: IntGauge,
    start_time: IntGauge,
}

impl ProcessCollector {
    pub fn new() -> Self {
        let mut descs = Vec::new();

        let cpu_total = IntCounter::with_opts(Opts::new(
            "process_cpu_seconds_total",
            "Total user and system CPU time spent in \
                 seconds.",
        ))
        .unwrap();
        descs.extend(cpu_total.desc().into_iter().cloned());

        let vsize = IntGauge::with_opts(Opts::new(
            "process_virtual_memory_bytes",
            "Virtual memory size in bytes.",
        ))
        .unwrap();
        descs.extend(vsize.desc().into_iter().cloned());

        let rss = IntGauge::with_opts(Opts::new(
            "process_resident_memory_bytes",
            "Resident memory size in bytes.",
        ))
        .unwrap();
        descs.extend(rss.desc().into_iter().cloned());

        let start_time = IntGauge::with_opts(Opts::new(
            "process_start_time_seconds",
            "Start time of the process since unix epoch \
                 in seconds.",
        ))
        .unwrap();
        descs.extend(start_time.desc().into_iter().cloned());
        // proc_start_time init once because it is immutable
        if let Ok(boot_time) = procfs::boot_time_secs() {
            if let Ok(p) = procfs::process::Process::myself() {
                start_time
                    .set(p.stat.starttime as i64 / thread::ticks_per_second() + boot_time as i64);
            }
        }

        Self {
            descs,
            cpu_total,
            vsize,
            rss,
            start_time,
        }
    }
}

impl Collector for ProcessCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let p = match procfs::process::Process::myself() {
            Ok(p) => p,
            Err(..) => {
                // we can't construct a Process object, so there's no stats to gather
                return Vec::new();
            }
        };

        // memory
        self.vsize.set(p.stat.vsize as i64);
        self.rss.set(p.stat.rss * *PAGESIZE);

        // cpu
        let cpu_total_mfs = {
            let total = (p.stat.utime + p.stat.stime) / thread::ticks_per_second() as u64;
            let past = self.cpu_total.get();
            self.cpu_total.inc_by(total - past);

            self.cpu_total.collect()
        };

        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(4);
        mfs.extend(cpu_total_mfs);
        mfs.extend(self.vsize.collect());
        mfs.extend(self.rss.collect());
        mfs.extend(self.start_time.collect());
        mfs
    }
}

lazy_static::lazy_static! {
    // getconf PAGESIZE
    static ref PAGESIZE: i64 = {
        unsafe {
            libc::sysconf(libc::_SC_PAGESIZE)
        }
    };
}
