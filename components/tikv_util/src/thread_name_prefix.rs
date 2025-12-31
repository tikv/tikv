// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

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
//!
//! Note on length: On Linux, the thread name (`comm`) is limited to 16 bytes.
//! We intentionally keep these prefixes short and stable so that the final
//! thread names (usually `"<prefix>-<N>"`, e.g. `"raftstore-1"`) remain
//! identifiable after truncation.

pub const ARCHIVE_WORKER_THREAD: &str = "archive-worker";

pub const SLOGGER_THREAD: &str = "slogger";

pub const TIME_MONITOR_THREAD: &str = "time-monitor";

pub const GRPC_SERVER_THREAD: &str = "grpc-server";

pub const PD_MONITOR_THREAD: &str = "pdmonitor";

pub const TSO_WORKER_THREAD: &str = "tso-worker";

pub const TIMER_THREAD: &str = "timer";

pub const BACKTRACE_LOADER_THREAD: &str = "backtrace-loader";

pub const BACKGROUND_WORKER_THREAD: &str = "background";

pub const CHECK_LEADER_THREAD: &str = "check-leader";

pub const REGION_COLLECTOR_WORKER_THREAD: &str = "region-collect";

pub const SST_RECOVERY_THREAD: &str = "sst-recovery";

pub const FLOW_CHECKER_THREAD: &str = "flow-checker";

pub const GC_WORKER_THREAD: &str = "gc-worker";

pub const CDC_THREAD: &str = "cdc";

pub const PD_WORKER_THREAD: &str = "pd-worker";

pub const UNIFIED_READ_POOL_THREAD: &str = "unified-read";

pub const DEBUGGER_THREAD: &str = "debugger";

pub const RESOURCE_METERING_RECORDER_THREAD: &str = "res-meter-rec";

pub const RESOURCE_METERING_SINGLE_TARGET_THREAD: &str = "res-meter-sink";

pub const SCHEDULE_WORKER_POOL_THREAD: &str = "sched-pool";

pub const SCHEDULE_WORKER_HIGH_PRI_THREAD: &str = "sched-high";

pub const SCHEDULE_WORKER_PRIORITY_THREAD: &str = "sched-pri";

pub const RESOLVED_TS_WORKER_THREAD: &str = "resolved-ts";

pub const TRANSPORT_STATS_THREAD: &str = "trans-stats";

pub const SNAP_HANDLER_THREAD: &str = "snap-handler";

pub const RAFT_STREAM_THREAD: &str = "raft-stream";

pub const BACKUP_STREAM_THREAD: &str = "backup-stream";

pub const LOG_BACKUP_SCAN_THREAD: &str = "log-backup-scan";

pub const SST_IMPORT_MISC_THREAD: &str = "sst-import-misc";

pub const PURGE_WORKER_THREAD: &str = "purge-worker";

pub const CHECKPOINT_WORKER_THREAD: &str = "checkpoint";

pub const ASYNC_READ_WORKER_THREAD: &str = "async-read";

pub const STORE_BACKGROUND_WORKER_THREAD: &str = "store-bg";

pub const TABLET_WORKER_THREAD: &str = "tablet-worker";

pub const TABLET_HIGH_PRIORITY_WORKER_THREAD: &str = "tablet-high";

pub const TABLET_BACKGROUND_WORKER_THREAD: &str = "tablet-bg";

pub const SNAP_GENERATOR_THREAD: &str = "snap-generator";

pub const CLEANUP_WORKER_THREAD: &str = "cleanup-worker";

pub const REGION_WORKER_THREAD: &str = "region-worker";

pub const RAFTLOG_FETCH_WORKER_THREAD: &str = "raftlog-fetch-worker";

pub const REFRESH_CONFIG_WORKER_THREAD: &str = "refresh-cfg";

pub const STORE_WRITER_THREAD: &str = "store-writer";

pub const STEADY_TIMER_THREAD: &str = "steady-timer";

pub const RAFTSTORE_THREAD: &str = "raftstore";

pub const RAFTSTORE_V2_THREAD: &str = "rs";

pub const APPLY_WORKER_THREAD: &str = "apply";

pub const STATS_MONITOR_THREAD: &str = "stats-monitor";

pub const GC_MANAGER_THREAD: &str = "gc-manager";

pub const COMPACTION_RUNNER_THREAD: &str = "compaction";

pub const CDC_WORKER_THREAD: &str = "cdcwkr";

pub const TSO_THREAD: &str = "tso";

pub const ADVANCED_TS_THREAD: &str = "advanced-ts";

pub const RESOLVED_TS_SCANNER_THREAD: &str = "rts-scan";

pub const SNAP_BROADCAST_THREAD: &str = "snap-broadcast";

pub const RUNTIME_KEEPER_THREAD: &str = "runtime-keeper";

pub const IMPORT_SST_WORKER_THREAD: &str = "impwkr";

pub const WAITER_MANAGER_THREAD: &str = "waiter-manager";

pub const DEADLOCK_DETECTOR_THREAD: &str = "deadlock-det";

pub const DEADLOCK_CLIENT_THREAD: &str = "deadlock-cli";

pub const BACKUP_WORKER_THREAD: &str = "backup-worker";

pub const BACKUP_IO_THREAD: &str = "backup-io";

pub const SNAP_SENDER_THREAD: &str = "snap-sender";

pub const TABLET_SNAP_SENDER_THREAD: &str = "tablet-snap";

pub const STATUS_SERVER_THREAD: &str = "status-server";
