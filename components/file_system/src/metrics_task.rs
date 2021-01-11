// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tikv_util::IntervalRunnable;

use crate::iosnoop::flush_io_metrics;
use crate::metrics::IO_BYTES_VEC;
use crate::BytesRecorder;
use crate::{IOOp, IOType};

pub struct MetricsTask {
    recorder: Option<Arc<BytesRecorder>>,
    last_fetch: [(i64, i64); IOType::VARIANT_COUNT],
}

impl MetricsTask {
    pub fn new(recorder: Option<Arc<BytesRecorder>>) -> Self {
        MetricsTask {
            recorder,
            last_fetch: [(0, 0); IOType::VARIANT_COUNT],
        }
    }
}

macro_rules! flush_io_bytes {
    ($recorder:expr, $metrics:ident, $io_type:expr, $last_fetch:expr) => {
        let read = $recorder.fetch($io_type, IOOp::Read) as i64;
        let write = $recorder.fetch($io_type, IOOp::Write) as i64;
        let (last_read, last_write) = $last_fetch;
        IO_BYTES_VEC.$metrics.read.inc_by(read - last_read);
        IO_BYTES_VEC.$metrics.write.inc_by(write - last_write);
        $last_fetch = (read, write);
    };
}

impl IntervalRunnable for MetricsTask {
    fn on_tick(&mut self) {
        flush_io_metrics();
        if let Some(recorder) = &self.recorder {
            flush_io_bytes!(
                recorder,
                other,
                IOType::Other,
                self.last_fetch[IOType::Other as usize]
            );
            flush_io_bytes!(
                recorder,
                foreground_read,
                IOType::ForegroundRead,
                self.last_fetch[IOType::ForegroundRead as usize]
            );
            flush_io_bytes!(
                recorder,
                foreground_write,
                IOType::ForegroundWrite,
                self.last_fetch[IOType::ForegroundWrite as usize]
            );
            flush_io_bytes!(
                recorder,
                flush,
                IOType::Flush,
                self.last_fetch[IOType::Flush as usize]
            );
            flush_io_bytes!(
                recorder,
                compaction,
                IOType::Compaction,
                self.last_fetch[IOType::Compaction as usize]
            );
            flush_io_bytes!(
                recorder,
                replication,
                IOType::Replication,
                self.last_fetch[IOType::Replication as usize]
            );
            flush_io_bytes!(
                recorder,
                load_balance,
                IOType::LoadBalance,
                self.last_fetch[IOType::LoadBalance as usize]
            );
            flush_io_bytes!(
                recorder,
                import,
                IOType::Import,
                self.last_fetch[IOType::Import as usize]
            );
            flush_io_bytes!(
                recorder,
                export,
                IOType::Export,
                self.last_fetch[IOType::Export as usize]
            );
        }
    }
}
