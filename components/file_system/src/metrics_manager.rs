// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use strum::{EnumCount, IntoEnumIterator};
use tikv_util::time::Instant;

use crate::{
    io_stats::fetch_io_bytes,
    metrics::{tls_flush, IO_BYTES_VEC},
    IoBytes, IoOp, IoRateLimiterStatistics, IoType,
};

pub enum BytesFetcher {
    /// Fetch IO statistics from IO rate limiter, which records passed-through
    /// IOs in atomic counters.
    FromRateLimiter(Arc<IoRateLimiterStatistics>),
    /// Fetch IO statistics from OS I/O stats collector.
    FromIoStatsCollector(),
}

impl BytesFetcher {
    fn fetch(&self) -> [IoBytes; IoType::COUNT] {
        match *self {
            BytesFetcher::FromRateLimiter(ref stats) => {
                let mut bytes: [IoBytes; IoType::COUNT] = Default::default();
                for t in IoType::iter() {
                    bytes[t as usize].read = stats.fetch(t, IoOp::Read) as u64;
                    bytes[t as usize].write = stats.fetch(t, IoOp::Write) as u64;
                }
                bytes
            }
            BytesFetcher::FromIoStatsCollector() => fetch_io_bytes(),
        }
    }
}

pub struct MetricsManager {
    fetcher: BytesFetcher,
    last_fetch: [IoBytes; IoType::COUNT],
}

impl MetricsManager {
    pub fn new(fetcher: BytesFetcher) -> Self {
        MetricsManager {
            fetcher,
            last_fetch: Default::default(),
        }
    }

    pub fn flush(&mut self, _now: Instant) {
        tls_flush();
        let latest = self.fetcher.fetch();
        for t in IoType::iter() {
            let delta_bytes = latest[t as usize] - self.last_fetch[t as usize];
            IO_BYTES_VEC
                .with_label_values(&[t.as_str(), "read"])
                .inc_by(delta_bytes.read);
            IO_BYTES_VEC
                .with_label_values(&[t.as_str(), "write"])
                .inc_by(delta_bytes.write);
            self.last_fetch[t as usize] = latest[t as usize];
        }
    }
}
