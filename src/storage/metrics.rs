// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum CommandKind {
        prewrite,
        acquire_pessimistic_lock,
        commit,
        cleanup,
        rollback,
        pessimistic_rollback,
        txn_heart_beat,
        check_txn_status,
        scan_lock,
        resolve_lock,
        resolve_lock_lite,
        delete_range,
        pause,
        key_mvcc,
        start_ts_mvcc,
        raw_get,
        raw_batch_get,
        raw_scan,
        raw_batch_scan,
        raw_put,
        raw_batch_put,
        raw_delete,
        raw_delete_range,
        raw_batch_delete,
    }

    pub label_enum CommandStageKind {
        new,
        snapshot,
        async_snapshot_err,
        snapshot_ok,
        snapshot_err,
        read_finish,
        next_cmd,
        lock_wait,
        process,
        prepare_write_err,
        write,
        write_finish,
        async_write_err,
        error,
    }

    pub label_enum CommandPriority {
        low,
        normal,
        high,
    }

    pub struct SchedDurationVec: Histogram {
        "type" => CommandKind,
    }

    pub struct KvCommandCounterVec: IntCounter {
        "type" => CommandKind,
    }

    pub struct SchedStageCounterVec: IntCounter {
        "type" => CommandKind,
        "stage" => CommandStageKind,
    }

    pub struct SchedLatchDurationVec: Histogram {
        "type" => CommandKind,
    }

    pub struct KvCommandKeysWrittenVec: Histogram {
        "type" => CommandKind,
    }

    pub struct SchedTooBusyVec: IntCounter {
        "type" => CommandKind,
    }

    pub struct SchedCommandPriCounterVec: IntCounter {
        "priority" => CommandPriority,
    }
}

lazy_static! {
    pub static ref KV_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_storage_command_total",
        "Total number of commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref KV_COMMAND_COUNTER_VEC_STATIC: KvCommandCounterVec =
        KvCommandCounterVec::from(&KV_COMMAND_COUNTER_VEC);
    pub static ref SCHED_STAGE_COUNTER_VEC: SchedStageCounterVec = {
        register_static_int_counter_vec!(
            SchedStageCounterVec,
            "tikv_scheduler_stage_total",
            "Total number of commands on each stage.",
            &["type", "stage"]
        )
        .unwrap()
    };
    pub static ref SCHED_WRITING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_writing_bytes",
        "Total number of writing kv."
    )
    .unwrap();
    pub static ref SCHED_CONTEX_GAUGE: IntGauge = register_int_gauge!(
        "tikv_scheduler_contex_total",
        "Total number of pending commands."
    )
    .unwrap();
    pub static ref SCHED_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_command_duration_seconds",
        "Bucketed histogram of command execution",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_HISTOGRAM_VEC_STATIC: SchedDurationVec =
        SchedDurationVec::from(&SCHED_HISTOGRAM_VEC);
    pub static ref SCHED_LATCH_HISTOGRAM_VEC: SchedLatchDurationVec =
        register_static_histogram_vec!(
            SchedLatchDurationVec,
            "tikv_scheduler_latch_wait_duration_seconds",
            "Bucketed histogram of latch wait",
            &["type"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        )
        .unwrap();
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_processing_read_duration_seconds",
        "Bucketed histogram of processing read duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_PROCESSING_WRITE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_processing_write_duration_seconds",
        "Bucketed histogram of processing write duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER_VEC: SchedTooBusyVec = register_static_int_counter_vec!(
        SchedTooBusyVec,
        "tikv_scheduler_too_busy_total",
        "Total count of scheduler too busy",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC: SchedCommandPriCounterVec =
        SchedCommandPriCounterVec::from(&SCHED_COMMANDS_PRI_COUNTER_VEC);
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_commands_pri_total",
        "Total count of different priority commands",
        &["priority"]
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_scheduler_kv_command_key_read",
        "Bucketed histogram of keys read of a kv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "tikv_scheduler_kv_scan_details",
        "Bucketed counter of kv keys scan details for each cf",
        &["req", "cf", "tag"]
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM_VEC: KvCommandKeysWrittenVec =
        register_static_histogram_vec!(
            KvCommandKeysWrittenVec,
            "tikv_scheduler_kv_command_key_write",
            "Bucketed histogram of keys write of a kv command",
            &["type"],
            exponential_buckets(1.0, 2.0, 21).unwrap()
        )
        .unwrap();
    pub static ref REQUEST_EXCEED_BOUND: IntCounter = register_int_counter!(
        "tikv_request_exceed_bound",
        "Counter of request exceed bound"
    )
    .unwrap();
}
