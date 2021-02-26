// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod applied_lock_collector;
mod compaction_filter;
mod config;
mod gc_manager;
mod gc_worker;

// TODO: Use separated error type for GCWorker instead.
pub use crate::storage::{Callback, Error, ErrorInner, Result};
pub use compaction_filter::WriteCompactionFilterFactory;
use compaction_filter::{
    is_compaction_filter_allowed, CompactionFilterInitializer, GC_COMPACTION_FILTER_ORPHAN_VERSIONS,
};
pub use config::{GcConfig, GcWorkerConfigManager, DEFAULT_GC_BATCH_KEYS};
pub use gc_manager::AutoGcConfig;
pub use gc_worker::{sync_gc, GcSafePointProvider, GcTask, GcWorker, GC_MAX_EXECUTING_TASKS};

#[cfg(any(test, feature = "failpoints"))]
pub use compaction_filter::test_utils::{gc_by_compact, TestGCRunner};
