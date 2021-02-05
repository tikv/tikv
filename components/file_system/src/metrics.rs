// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum IOType {
        other,
        foreground_read,
        foreground_write,
        flush,
        compaction,
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

    pub struct IOLatencyVec : Histogram {
        "type" => IOType,
        "op" => IOOp,
    }

    pub struct IOBytesVec : IntCounter {
        "type" => IOType,
        "op" => IOOp,
    }
}

lazy_static! {
    pub static ref IO_BYTES_VEC: IOBytesVec = register_static_int_counter_vec!(
        IOBytesVec,
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
}
