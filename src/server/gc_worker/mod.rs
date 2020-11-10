// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod applied_lock_collector;
mod compaction_filter;
mod config;
mod gc_runner;
mod gc_worker;

// TODO: Use separated error type for GCWorker instead.
pub use crate::storage::{Callback, Error, ErrorInner, Result};
pub use compaction_filter::WriteCompactionFilterFactory;
use compaction_filter::{is_compaction_filter_allowd, CompactionFilterInitializer};
pub use config::{GcConfig, GcWorkerConfigManager, DEFAULT_GC_BATCH_KEYS};
pub use gc_runner::{AutoGcConfig, GcSafePointProvider, GcTask};
pub use gc_worker::{GcWorker, GC_MAX_EXECUTING_TASKS};

#[cfg(test)]
pub use compaction_filter::tests::gc_by_compact;

pub use gc_runner::{MockRegionInfoProvider, MockSafePointProvider};
