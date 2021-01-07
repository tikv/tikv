// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tikv_util::IntervalRunnable;

use crate::iosnoop::flush_io_metrics;
use crate::metrics::IO_BYTES_VEC;
use crate::BytesRecorder;
use crate::{IOOp, IOType};

pub struct MetricsTask {
    recorder: Option<Arc<BytesRecorder>>,
}

impl MetricsTask {
    pub fn new(recorder: Option<Arc<BytesRecorder>>) -> Self {
        MetricsTask { recorder }
    }
}

impl IntervalRunnable for MetricsTask {
    fn on_tick(&mut self) {
        flush_io_metrics();
        if let Some(recorder) = &self.recorder {
            IO_BYTES_VEC
                .other
                .read
                .inc_by(recorder.fetch(IOType::Other, IOOp::Read) as i64);
            IO_BYTES_VEC
                .other
                .write
                .inc_by(recorder.fetch(IOType::Other, IOOp::Write) as i64);
            IO_BYTES_VEC
                .read
                .read
                .inc_by(recorder.fetch(IOType::Read, IOOp::Read) as i64);
            IO_BYTES_VEC
                .read
                .write
                .inc_by(recorder.fetch(IOType::Read, IOOp::Write) as i64);
            IO_BYTES_VEC
                .write
                .read
                .inc_by(recorder.fetch(IOType::Write, IOOp::Read) as i64);
            IO_BYTES_VEC
                .write
                .write
                .inc_by(recorder.fetch(IOType::Write, IOOp::Write) as i64);
            IO_BYTES_VEC
                .coprocessor
                .read
                .inc_by(recorder.fetch(IOType::Coprocessor, IOOp::Read) as i64);
            IO_BYTES_VEC
                .coprocessor
                .write
                .inc_by(recorder.fetch(IOType::Coprocessor, IOOp::Write) as i64);
            IO_BYTES_VEC
                .flush
                .read
                .inc_by(recorder.fetch(IOType::Flush, IOOp::Read) as i64);
            IO_BYTES_VEC
                .flush
                .write
                .inc_by(recorder.fetch(IOType::Flush, IOOp::Write) as i64);
            IO_BYTES_VEC
                .compaction
                .read
                .inc_by(recorder.fetch(IOType::Compaction, IOOp::Read) as i64);
            IO_BYTES_VEC
                .compaction
                .write
                .inc_by(recorder.fetch(IOType::Compaction, IOOp::Write) as i64);
            IO_BYTES_VEC
                .replication
                .read
                .inc_by(recorder.fetch(IOType::Replication, IOOp::Read) as i64);
            IO_BYTES_VEC
                .replication
                .write
                .inc_by(recorder.fetch(IOType::Replication, IOOp::Write) as i64);
            IO_BYTES_VEC
                .load_balance
                .read
                .inc_by(recorder.fetch(IOType::LoadBalance, IOOp::Read) as i64);
            IO_BYTES_VEC
                .load_balance
                .write
                .inc_by(recorder.fetch(IOType::LoadBalance, IOOp::Write) as i64);
            IO_BYTES_VEC
                .import
                .read
                .inc_by(recorder.fetch(IOType::Import, IOOp::Read) as i64);
            IO_BYTES_VEC
                .import
                .write
                .inc_by(recorder.fetch(IOType::Import, IOOp::Write) as i64);
            IO_BYTES_VEC
                .export
                .read
                .inc_by(recorder.fetch(IOType::Export, IOOp::Read) as i64);
            IO_BYTES_VEC
                .export
                .write
                .inc_by(recorder.fetch(IOType::Export, IOOp::Write) as i64);
        }
    }
}
