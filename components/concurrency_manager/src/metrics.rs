// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! Metrics for KeyHandle memory leak detection and monitoring.

use lazy_static::lazy_static;
use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};

lazy_static! {
    /// Gauge for current KeyHandle instances, labeled by key type (data/meta)
    pub static ref KEYHANDLE_CURRENT: IntGaugeVec = register_int_gauge_vec!(
        "tikv_keyhandle_current",
        "Current number of KeyHandle instances in memory",
        &["type"]
    )
    .unwrap();

    /// Counter for KeyHandle creation events, labeled by key type (data/meta)
    pub static ref KEYHANDLE_CREATED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "tikv_keyhandle_created_total",
        "Total number of KeyHandle instances created",
        &["type"]
    )
    .unwrap();

    /// Counter for KeyHandle destruction events, labeled by key type (data/meta)
    pub static ref KEYHANDLE_RELEASED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "tikv_keyhandle_released_total",
        "Total number of KeyHandle instances released",
        &["type"]
    )
    .unwrap();

    /// Counter for KeyHandleGuard acquisition events, labeled by key type (data/meta)
    pub static ref KEYHANDLE_GUARD_ACQUIRED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "tikv_keyhandle_guard_acquired_total",
        "Total number of KeyHandleGuard instances acquired",
        &["type"]
    )
    .unwrap();

    /// Counter for KeyHandleGuard release events, labeled by key type (data/meta)
    pub static ref KEYHANDLE_GUARD_RELEASED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "tikv_keyhandle_guard_released_total",
        "Total number of KeyHandleGuard instances released",
        &["type"]
    )
    .unwrap();

    /// Counter for key-path lifecycle checkpoints, labeled by path and checkpoint
    pub static ref KEYHANDLE_CHECKPOINT_TOTAL: IntCounterVec = register_int_counter_vec!(
        "tikv_keyhandle_checkpoint_total",
        "Total number of KeyHandle lifecycle checkpoints reached",
        &["path", "checkpoint"]
    )
    .unwrap();

    // Pre-created checkpoint counters for performance optimization
    pub static ref CHECKPOINT_PREWRITE_START: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["prewrite", "start"]);
    pub static ref CHECKPOINT_PREWRITE_BEFORE_MUTATIONS: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["prewrite", "before_mutations"]);
    pub static ref CHECKPOINT_PREWRITE_AFTER_MUTATIONS: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["prewrite", "after_mutations"]);
    pub static ref CHECKPOINT_PREWRITE_BEFORE_TAKE_GUARDS: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["prewrite", "before_take_guards"]);
    pub static ref CHECKPOINT_PREWRITE_GUARDS_TAKEN: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["prewrite", "guards_taken"]);
    pub static ref CHECKPOINT_PREWRITE_ASYNC_BEFORE_LOCK_KEY: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["prewrite", "async_before_lock_key"]);
    pub static ref CHECKPOINT_PREWRITE_ASYNC_AFTER_LOCK_KEY: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["prewrite", "async_after_lock_key"]);
    pub static ref CHECKPOINT_SCHEDULER_GUARDS_TO_WRITE: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "guards_to_write"]);
    pub static ref CHECKPOINT_SCHEDULER_NO_WRITE_NEEDED: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "no_write_needed"]);
    pub static ref CHECKPOINT_SCHEDULER_IN_MEMORY_PESSIMISTIC_LOCK: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "in_memory_pessimistic_lock"]);
    pub static ref CHECKPOINT_SCHEDULER_BEFORE_ASYNC_WRITE: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "before_async_write"]);
    pub static ref CHECKPOINT_SCHEDULER_ASYNC_WRITE_LOOP_START: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "async_write_loop_start"]);
    pub static ref CHECKPOINT_SCHEDULER_ASYNC_WRITE_COMMITTED: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "async_write_committed"]);
    pub static ref CHECKPOINT_SCHEDULER_ASYNC_WRITE_FINISHED: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "async_write_finished"]);
    pub static ref CHECKPOINT_SCHEDULER_ASYNC_WRITE_LOOP_EXIT_NO_FINISHED: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "async_write_loop_exit_no_finished"]);
    pub static ref CHECKPOINT_SCHEDULER_EARLY_RESPONSE_ASYNC_APPLY: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "early_response_async_apply"]);
    pub static ref CHECKPOINT_SCHEDULER_EARLY_RESPONSE_PIPELINED: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "early_response_pipelined"]);
    pub static ref CHECKPOINT_SCHEDULER_WRITE_FINISHED: IntCounter =
        KEYHANDLE_CHECKPOINT_TOTAL.with_label_values(&["scheduler", "write_finished"]);

    /// Gauge for current lock table size
    pub static ref LOCK_TABLE_SIZE: IntGauge = register_int_gauge!(
        "tikv_lock_table_size",
        "Current number of entries in the lock table"
    )
    .unwrap();

    /// Counter for lock table insert operations
    pub static ref LOCK_TABLE_INSERTS_TOTAL: IntCounter = register_int_counter!(
        "tikv_lock_table_inserts_total",
        "Total number of lock table insert operations"
    )
    .unwrap();

    /// Counter for lock table removal operations
    pub static ref LOCK_TABLE_REMOVALS_TOTAL: IntCounter = register_int_counter!(
        "tikv_lock_table_removals_total",
        "Total number of lock table removal operations"
    )
    .unwrap();

    /// Counter for lock table reuse operations (existing KeyHandle found)
    pub static ref LOCK_TABLE_REUSE_TOTAL: IntCounter = register_int_counter!(
        "tikv_lock_table_reuse_total",
        "Total number of times an existing KeyHandle was reused"
    )
    .unwrap();

    pub static ref LOCK_TABLE_UPGRADE_FAIL: IntCounter = register_int_counter!(
        "tikv_lock_table_upgrade_fail",
        "Total number of times an existing KeyHandle was not upgraded"
    )
    .unwrap();

    /// Counter for tracking when registry reaches capacity limit
    pub static ref TRACKED_ARC_CAPACITY_EXCEEDED: IntCounter = register_int_counter!(
        "tikv_tracked_arc_capacity_exceeded_total",
        "Total number of times TrackedArc registry capacity was exceeded"
    ).unwrap();

    /// Counter for tracking when record_access is called for unregistered KeyHandles
    pub static ref TRACKED_ARC_RECORD_NOT_FOUND: IntCounter = register_int_counter!(
        "tikv_tracked_arc_record_not_found_total",
        "Total number of times record_access was called for unregistered KeyHandles"
    ).unwrap();
}

/// Determine the key type for metrics labeling
pub fn get_key_type(key: &[u8]) -> &'static str {
    if key.starts_with(b"m") {
        "meta"
    } else {
        "data"
    }
}
