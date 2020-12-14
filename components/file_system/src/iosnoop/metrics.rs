// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum IOType {
        other,
        read,
        write,
        coprocessor,
        flush,
        compaction,
        replication,
        loadbalance,
        import,
        export,
    }

    pub label_enum IOOp {
        read,
        write,
    }

    pub struct IOLatencyVec : LocalHistogram {
        "type" => IOType,
        "op" => IOOp,
    }

    pub struct IOBytesVec : LocalIntCounter {
        "type" => IOType,
        "op" => IOOp,
    }
}

lazy_static! {
    pub static ref IO_BYTES_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_io_bytes",
        "Bytes of disk tikv io",
        &["type", "op"]
    ).unwrap();
    pub static ref IO_BYTES : IOBytesVec =
        auto_flush_from!(IO_BYTES_VEC, IOBytesVec);

    pub static ref IO_LATENCY_MICROS_VEC: HistogramVec =
        register_histogram_vec!(
            "tikv_io_latency_micros",
            "Duration of disk tikv io.",
            &["type", "op"],
            exponential_buckets(1.0, 2.0, 22).unwrap() // max 4s
        ).unwrap();
    pub static ref IO_LATENCY_MICROS: IOLatencyVec =
        auto_flush_from!(IO_LATENCY_MICROS_VEC, IOLatencyVec);
}
