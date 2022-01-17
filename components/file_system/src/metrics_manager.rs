// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::{tls_flush, IO_BYTES_VEC};
use crate::tracing::fetch_io_bytes;
use crate::IOBytes;
use crate::IORateLimiterStatistics;
use crate::{IOOp, IOType};

use std::sync::Arc;

use strum::EnumCount;
use strum::IntoEnumIterator;
use tikv_util::time::Instant;

pub enum BytesFetcher {
    /// Fetch IO statistics from IO rate limiter, which records passed-through IOs in atomic counters.
    FromRateLimiter(Arc<IORateLimiterStatistics>),
    /// Fetch IO statistics from OS I/O tracer.
    FromIOTracer(),
}

impl BytesFetcher {
    fn fetch(&self, bytes_vec: &mut [IOBytes]) {
        match *self {
            BytesFetcher::FromRateLimiter(ref stats) => {
                for t in IOType::iter() {
                    bytes_vec[t as usize].read = stats.fetch(t, IOOp::Read) as u64;
                    bytes_vec[t as usize].write = stats.fetch(t, IOOp::Write) as u64;
                }
            }
            BytesFetcher::FromIOTracer() => fetch_io_bytes(bytes_vec, false /*allow_cache*/),
        }
    }
}

pub struct MetricsManager {
    fetcher: BytesFetcher,
    last_fetch: [IOBytes; IOType::COUNT],
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
        let mut latest: [IOBytes; IOType::COUNT] = Default::default();
        self.fetcher.fetch(&mut latest);
        for t in IOType::iter() {
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
