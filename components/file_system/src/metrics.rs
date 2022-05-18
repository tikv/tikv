// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;

use prometheus::{local::*, *};
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum IOType {
        other,
        foreground_read,
        foreground_write,
        flush,
        compaction,
        level_zero_compaction,
        replication,
        load_balance,
        gc,
        import,
        export,
    }

    pub label_enum IOOp {
        read,
        write,
    }

    pub label_enum IOPriority {
        low,
        medium,
        high,
    }

    pub struct IOLatencyVec : Histogram {
        "type" => IOType,
        "op" => IOOp,
    }

    pub struct IOBytesVec : IntCounter {
        "type" => IOType,
        "op" => IOOp,
    }

    pub struct IOPriorityIntGaugeVec : IntGauge {
        "type" => IOPriority,
    }
}

lazy_static! {
    pub static ref IO_BYTES_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_io_bytes",
        "Bytes of disk tikv io",
        &["type", "op"]
    ).unwrap();

    pub static ref IO_LATENCY_MICROS_VEC: IOLatencyVec =
        register_static_histogram_vec!(
            IOLatencyVec,
            "tikv_io_latency_micros",
            "Duration of disk tikv io.",
            &["type", "op"],
            exponential_buckets(1.0, 2.0, 22).unwrap() // max 4s
        ).unwrap();

    pub static ref RATE_LIMITER_REQUEST_WAIT_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_rate_limiter_request_wait_duration_seconds",
        "Bucketed histogram of IO rate limiter request wait duration",
        &["type"],
        exponential_buckets(0.001, 1.8, 20).unwrap()
    )
    .unwrap();

    pub static ref RATE_LIMITER_MAX_BYTES_PER_SEC: IOPriorityIntGaugeVec = register_static_int_gauge_vec!(
        IOPriorityIntGaugeVec,
        "tikv_rate_limiter_max_bytes_per_sec",
        "Maximum IO bytes per second",
        &["type"]
    ).unwrap();
}

pub struct FileSystemLocalMetrics {
    rate_limiter_request_wait_duration: LocalHistogramVec,
}

thread_local! {
    static TLS_FILE_SYSTEM_METRICS: RefCell<FileSystemLocalMetrics> = RefCell::new(
        FileSystemLocalMetrics {
            rate_limiter_request_wait_duration: RATE_LIMITER_REQUEST_WAIT_DURATION.local(),
        }
   );
}

pub fn tls_flush() {
    TLS_FILE_SYSTEM_METRICS.with(|m| {
        let m = m.borrow();
        m.rate_limiter_request_wait_duration.flush();
    });
}

#[inline]
pub fn tls_collect_rate_limiter_request_wait(priority: &str, duration: std::time::Duration) {
    TLS_FILE_SYSTEM_METRICS.with(|m| {
        m.borrow_mut()
            .rate_limiter_request_wait_duration
            .with_label_values(&[priority])
            .observe(tikv_util::time::duration_to_sec(duration))
    });
}
