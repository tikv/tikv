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
        scan_lock,
        resolve_lock,
        resolve_lock_lite,
        gc,
        unsafe_destroy_range,
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
    pub static ref KV_GC_EMPTY_RANGE_COUNTER: IntCounter = register_int_counter!(
        "tikv_storage_gc_empty_range_total",
        "Total number of empty range found by gc"
    )
    .unwrap();
    pub static ref KV_GC_SKIPPED_COUNTER: IntCounter = register_int_counter!(
        "tikv_storage_gc_skipped_counter",
        "Total number of gc command skipped owing to optimization"
    )
    .unwrap();
    pub static ref GC_TASK_DURATION_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "tikv_gcworker_gc_task_duration_vec",
        "Duration of gc tasks execution",
        &["task"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref GC_GCTASK_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_gcworker_gc_tasks_vec",
        "Counter of gc tasks processed by gc_worker",
        &["task"]
    )
    .unwrap();
    pub static ref GC_GCTASK_FAIL_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_gcworker_gc_task_fail_vec",
        "Counter of gc tasks that is failed",
        &["task"]
    )
    .unwrap();
    pub static ref GC_TOO_BUSY_COUNTER: IntCounter = register_int_counter!(
        "tikv_gc_worker_too_busy",
        "Counter of occurrence of gc_worker being too busy"
    )
    .unwrap();
    pub static ref GC_KEYS_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "tikv_gcworker_gc_keys",
        "Counter of keys affected during gc",
        &["cf", "tag"]
    )
    .unwrap();
    pub static ref AUTO_GC_STATUS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_gcworker_autogc_status",
        "State of the auto gc manager",
        &["state"]
    )
    .unwrap();
    pub static ref AUTO_GC_SAFE_POINT_GAUGE: IntGauge = register_int_gauge!(
        "tikv_gcworker_autogc_safe_point",
        "Safe point used for auto gc"
    )
    .unwrap();
    pub static ref AUTO_GC_PROCESSED_REGIONS_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "tikv_gcworker_autogc_processed_regions",
        "Processed regions by auto gc",
        &["type"]
    )
    .unwrap();
    pub static ref REQUEST_EXCEED_BOUND: IntCounter = register_int_counter!(
        "tikv_request_exceed_bound",
        "Counter of request exceed bound"
    )
    .unwrap();
}
