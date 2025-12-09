//! Centralized definitions of TiKV thread name prefixes.
//!
//! TiKV spawns many worker threads across subsystems (raftstore, scheduler,
//! CDC, backup, PD workers, etc.) via `std::thread::Builder`. Historically
//! these names were scattered as ad-hoc string literals, making it harder to:
//!   - audit all TiKV-owned threads,
//!   - keep naming consistent,
//!   - update names globally,
//!   - recognize threads easily in tools like `top`, GDB/LLDB, `perf`, etc.,
//!   - learn the DB internals for developers who are just getting started.
//!
//! This file gathers all TiKV-controlled thread name **prefixes** in a single
//! place. Components should reference these constants when spawning threads.
//!
//! Threads created by external libraries (e.g., RocksDB, gRPC, Prometheus) are
//! excluded since TiKV cannot rename them.
//!
//! Constants are ordered by when TiKV creates these threads during server
//! startup.

pub const ARCHIVE_WORKER_THREAD_PREFIX: &str = "archive-worker";

pub const SLOGGER_THREAD_PREFIX: &str = "slogger";

pub const TIME_MONITOR_THREAD_PREFIX: &str = "time-monitor";

pub const GRPC_SERVER_THREAD_PREFIX: &str = "grpc-server";

pub const PD_MONITOR_THREAD_PREFIX: &str = "pdmonitor";

pub const TSO_WORKER_THREAD_PREFIX: &str = "tso-worker";

pub const TIMER_THREAD_PREFIX: &str = "timer";

pub const BACKTRACE_LOADER_THREAD_PREFIX: &str = "backtrace-loader";

pub const BACKGROUND_WORKER_THREAD_PREFIX: &str = "background";

pub const CHECK_LEADER_THREAD_PREFIX: &str = "check-leader";

pub const REGION_COLLECTOR_WORKER_THREAD_PREFIX: &str = "region-collector-worker";

pub const SST_RECOVERY_THREAD_PREFIX: &str = "sst-recovery";

pub const FLOW_CHECKER_THREAD_PREFIX: &str = "flow-checker";

pub const GC_WORKER_THREAD_PREFIX: &str = "gc-worker";

pub const CDC_THREAD_PREFIX: &str = "cdc";

pub const PD_WORKER_THREAD_PREFIX: &str = "pd-worker";

pub const UNIFIED_READ_POOL_THREAD_PREFIX: &str = "unified-read-pool";

pub const DEBUGGER_THREAD_PREFIX: &str = "debugger";

pub const RESOURCE_METERING_RECORDER_THREAD_PREFIX: &str = "resource-metering-recorder";

pub const RESOURCE_METERING_SINGLE_TARGET_DATA_SINK_THREAD_PREFIX: &str =
    "resource-metering-single-target-data-sink";

pub const SCHEDULE_WORKER_POOL_THREAD_PREFIX: &str = "sched-worker-pool";

pub const SCHEDULE_WORKER_HIGH_PRIORITY_THREAD_PREFIX: &str = "sched-worker-high";

pub const SCHEDULE_WORKER_PRIORITY_THREAD_PREFIX: &str = "sched-worker-priority";

pub const RESOLVED_TS_WORKER_THREAD_PREFIX: &str = "resolved-ts-worker";

pub const STATS_THREAD_PREFIX: &str = "transport-stats";

pub const SNAP_HANDLER_THREAD_PREFIX: &str = "snap-handler";

pub const RAFT_STREAM_THREAD_PREFIX: &str = "raft-stream";

pub const BACKUP_STREAM_THREAD_PREFIX: &str = "backup-stream";

pub const LOG_BACKUP_SCAN_THREAD_PREFIX: &str = "log-backup-scan";

pub const SST_IMPORT_MISC_THREAD_PREFIX: &str = "sst-import-misc";

pub const PURGE_WORKER_THREAD_PREFIX: &str = "purge-worker";

pub const CHECKPOINT_WORKER_THREAD_PREFIX: &str = "checkpoint-worker";

pub const ASYNC_READ_WORKER_THREAD_PREFIX: &str = "async-read-worker";

pub const STORE_BACKGROUND_WORKER_THREAD_PREFIX: &str = "store-bg";

pub const TABLET_WORKER_THREAD_PREFIX: &str = "tablet-worker";

pub const TABLET_HIGH_PRIORITY_WORKER_THREAD_PREFIX: &str = "tablet-high";

pub const TABLET_BACKGROUND_WORKER_THREAD_PREFIX: &str = "tablet-bg";

pub const SNAP_GENERATOR_THREAD_PREFIX: &str = "snap-generator";

pub const CLEANUP_WORKER_THREAD_PREFIX: &str = "cleanup-worker";

pub const REGION_WORKER_THREAD_PREFIX: &str = "region-worker";

pub const RAFTLOG_FETCH_WORKER_THREAD_PREFIX: &str = "raftlog-fetch-worker";

pub const REFRESH_CONFIG_WORKER_THREAD_PREFIX: &str = "refresh-config-worker";

pub const STORE_WRITER_THREAD_PREFIX: &str = "store-writer";

pub const STEADY_TIMER_THREAD_PREFIX: &str = "steady-timer";

pub const RAFTSTORE_THREAD_PREFIX: &str = "raftstore";

pub const RAFTSTORE_V2_THREAD_PREFIX: &str = "rs";

pub const APPLY_WORKER_THREAD_PREFIX: &str = "apply";

pub const STATS_MONITOR_THREAD_PREFIX: &str = "stats-monitor";

pub const GC_MANAGER_THREAD_PREFIX: &str = "gc-manager";

pub const COMPACTION_RUNNER_THREAD_PREFIX: &str = "compaction-runner";

pub const CDC_WORKER_THREAD_PREFIX: &str = "cdc-worker";

pub const TSO_THREAD_PREFIX: &str = "tso";

pub const ADVANCED_TS_THREAD_PREFIX: &str = "advanced-ts";

pub const RESOLVED_TS_SCANNER_THREAD_PREFIX: &str = "resolved-ts-scanner";

pub const SNAP_BROADCAST_BACKUP_PREPARE_THREAD_PREFIX: &str = "snap-br-backup-prepare";

pub const RUNTIME_KEEPER_THREAD_PREFIX: &str = "runtime-keeper";

pub const IMPORT_SST_WORKER_THREAD_PREFIX: &str = "import-sst-worker";

pub const WAITER_MANAGER_THREAD_PREFIX: &str = "waiter-manager";

pub const DEADLOCK_DETECTOR_THREAD_PREFIX: &str = "deadlock-detector";

pub const DEADLOCK_CLIENT_THREAD_PREFIX: &str = "deadlock-client";

pub const BACKUP_WORKER_THREAD_PREFIX: &str = "backup-worker";

pub const BACKUP_IO_THREAD_PREFIX: &str = "backup-io";

pub const SNAP_SENDER_THREAD_PREFIX: &str = "snap-sender";

pub const TABLET_SNAP_SENDER_THREAD_PREFIX: &str = "tablet-snap-sender";

pub const STATUS_SERVER_THREAD_PREFIX: &str = "status-server";
