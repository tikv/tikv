// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, io::Read, iter::FromIterator, path::PathBuf};

use batch_system::Config as BatchSystemConfig;
use causal_ts::Config as CausalTsConfig;
use collections::{HashMap, HashSet};
use encryption::{EncryptionConfig, FileConfig, MasterKeyConfig};
use engine_rocks::{
    config::{BlobRunMode, CompressionType, LogLevel},
    raw::{
        CompactionPriority, DBCompactionStyle, DBCompressionType, DBRateLimiterMode, DBRecoveryMode,
    },
};
use engine_traits::PerfLevel;
use file_system::{IOPriority, IORateLimitMode};
use kvproto::encryptionpb::EncryptionMethod;
use pd_client::Config as PdConfig;
use raft_log_engine::{ReadableSize as RaftEngineReadableSize, RecoveryMode};
use raftstore::{
    coprocessor::{Config as CopConfig, ConsistencyCheckMethod},
    store::Config as RaftstoreConfig,
};
use security::SecurityConfig;
use slog::Level;
use tikv::{
    config::*,
    import::Config as ImportConfig,
    server::{
        config::GrpcCompressionType, gc_worker::GcConfig,
        lock_manager::Config as PessimisticTxnConfig, Config as ServerConfig,
    },
    storage::config::{
        BlockCacheConfig, Config as StorageConfig, FlowControlConfig, IORateLimitConfig,
    },
};
use tikv_util::config::{LogFormat, ReadableDuration, ReadableSize};

mod dynamic;
mod test_config_client;

#[test]
fn test_toml_serde() {
    let value = TiKvConfig::default();
    let dump = toml::to_string_pretty(&value).unwrap();
    let load = toml::from_str(&dump).unwrap();
    assert_eq!(value, load);
}

// Read a file in project directory. It is similar to `include_str!`,
// but `include_str!` a large string literal increases compile time.
// See more: https://github.com/rust-lang/rust/issues/39352
fn read_file_in_project_dir(path: &str) -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push(path);
    let mut f = File::open(p).unwrap();
    let mut buffer = String::new();
    f.read_to_string(&mut buffer).unwrap();
    buffer
}

#[test]
fn test_serde_custom_tikv_config() {
    let mut value = TiKvConfig::default();
    value.log_rotation_timespan = ReadableDuration::days(1);
    value.log.level = Level::Critical;
    value.log.file.filename = "foo".to_owned();
    value.log.format = LogFormat::Json;
    value.log.file.max_size = 1;
    value.log.file.max_backups = 2;
    value.log.file.max_days = 3;
    value.slow_log_file = "slow_foo".to_owned();
    value.slow_log_threshold = ReadableDuration::secs(1);
    value.abort_on_panic = true;
    value.memory_usage_limit = Some(ReadableSize::gb(10));
    value.memory_usage_high_water = 0.65;
    value.server = ServerConfig {
        cluster_id: 0, // KEEP IT ZERO, it is skipped by serde.
        addr: "example.com:443".to_owned(),
        labels: HashMap::from_iter([("a".to_owned(), "b".to_owned())]),
        advertise_addr: "example.com:443".to_owned(),
        status_addr: "example.com:443".to_owned(),
        advertise_status_addr: "example.com:443".to_owned(),
        status_thread_pool_size: 1,
        max_grpc_send_msg_len: 6 * (1 << 20),
        raft_client_grpc_send_msg_buffer: 1234 * 1024,
        raft_client_queue_size: 1234,
        raft_msg_max_batch_size: 123,
        concurrent_send_snap_limit: 4,
        concurrent_recv_snap_limit: 4,
        grpc_compression_type: GrpcCompressionType::Gzip,
        grpc_concurrency: 123,
        grpc_concurrent_stream: 1_234,
        grpc_memory_pool_quota: ReadableSize(123_456),
        grpc_raft_conn_num: 123,
        grpc_stream_initial_window_size: ReadableSize(12_345),
        grpc_keepalive_time: ReadableDuration::secs(3),
        grpc_keepalive_timeout: ReadableDuration::secs(60),
        end_point_concurrency: None,
        end_point_max_tasks: None,
        end_point_stack_size: None,
        end_point_recursion_limit: 100,
        end_point_stream_channel_size: 16,
        end_point_batch_row_limit: 64,
        end_point_stream_batch_row_limit: 4096,
        end_point_enable_batch_if_possible: true,
        end_point_request_max_handle_duration: ReadableDuration::secs(12),
        end_point_max_concurrency: 10,
        end_point_perf_level: PerfLevel::EnableTime,
        snap_max_write_bytes_per_sec: ReadableSize::mb(10),
        snap_max_total_size: ReadableSize::gb(10),
        stats_concurrency: 10,
        heavy_load_threshold: 25,
        heavy_load_wait_duration: Some(ReadableDuration::millis(2)),
        enable_request_batch: false,
        background_thread_count: 999,
        raft_client_backoff_step: ReadableDuration::secs(1),
        end_point_slow_log_threshold: ReadableDuration::secs(1),
        forward_max_connections_per_address: 5,
        reject_messages_on_memory_ratio: 0.8,
    };
    value.readpool = ReadPoolConfig {
        unified: UnifiedReadPoolConfig {
            min_thread_count: 5,
            max_thread_count: 10,
            stack_size: ReadableSize::mb(20),
            max_tasks_per_worker: 2200,
        },
        storage: StorageReadPoolConfig {
            use_unified_pool: Some(true),
            high_concurrency: 1,
            normal_concurrency: 3,
            low_concurrency: 7,
            max_tasks_per_worker_high: 1000,
            max_tasks_per_worker_normal: 1500,
            max_tasks_per_worker_low: 2500,
            stack_size: ReadableSize::mb(20),
        },
        coprocessor: CoprReadPoolConfig {
            use_unified_pool: Some(false),
            high_concurrency: 2,
            normal_concurrency: 4,
            low_concurrency: 6,
            max_tasks_per_worker_high: 2000,
            max_tasks_per_worker_normal: 1000,
            max_tasks_per_worker_low: 3000,
            stack_size: ReadableSize::mb(12),
        },
    };
    value.metric = MetricConfig {
        interval: ReadableDuration::secs(15),
        address: "".to_string(),
        job: "tikv_1".to_owned(),
    };
    let mut apply_batch_system = BatchSystemConfig::default();
    apply_batch_system.max_batch_size = Some(22);
    apply_batch_system.pool_size = 4;
    apply_batch_system.reschedule_duration = ReadableDuration::secs(3);
    let mut store_batch_system = BatchSystemConfig::default();
    store_batch_system.max_batch_size = Some(21);
    store_batch_system.pool_size = 3;
    store_batch_system.reschedule_duration = ReadableDuration::secs(2);
    value.raft_store = RaftstoreConfig {
        prevote: false,
        raftdb_path: "/var".to_owned(),
        capacity: ReadableSize(123),
        raft_base_tick_interval: ReadableDuration::secs(12),
        raft_heartbeat_ticks: 1,
        raft_election_timeout_ticks: 12,
        raft_min_election_timeout_ticks: 14,
        raft_max_election_timeout_ticks: 20,
        raft_max_size_per_msg: ReadableSize::mb(12),
        raft_max_inflight_msgs: 123,
        raft_entry_max_size: ReadableSize::mb(12),
        raft_log_compact_sync_interval: ReadableDuration::secs(12),
        raft_log_gc_tick_interval: ReadableDuration::secs(12),
        raft_log_gc_threshold: 12,
        raft_log_gc_count_limit: Some(12),
        raft_log_gc_size_limit: Some(ReadableSize::kb(1)),
        raft_log_reserve_max_ticks: 100,
        raft_engine_purge_interval: ReadableDuration::minutes(20),
        raft_entry_cache_life_time: ReadableDuration::secs(12),
        raft_reject_transfer_leader_duration: ReadableDuration::secs(3),
        split_region_check_tick_interval: ReadableDuration::secs(12),
        region_split_check_diff: Some(ReadableSize::mb(20)),
        region_compact_check_interval: ReadableDuration::secs(12),
        clean_stale_peer_delay: ReadableDuration::secs(0),
        region_compact_check_step: 1_234,
        region_compact_min_tombstones: 999,
        region_compact_tombstones_percent: 33,
        pd_heartbeat_tick_interval: ReadableDuration::minutes(12),
        pd_store_heartbeat_tick_interval: ReadableDuration::secs(12),
        notify_capacity: 12_345,
        snap_mgr_gc_tick_interval: ReadableDuration::minutes(12),
        snap_gc_timeout: ReadableDuration::hours(12),
        messages_per_tick: 12_345,
        max_peer_down_duration: ReadableDuration::minutes(12),
        max_leader_missing_duration: ReadableDuration::hours(12),
        abnormal_leader_missing_duration: ReadableDuration::hours(6),
        peer_stale_state_check_interval: ReadableDuration::hours(2),
        leader_transfer_max_log_lag: 123,
        snap_apply_batch_size: ReadableSize::mb(12),
        lock_cf_compact_interval: ReadableDuration::minutes(12),
        lock_cf_compact_bytes_threshold: ReadableSize::mb(123),
        consistency_check_interval: ReadableDuration::secs(12),
        report_region_flow_interval: ReadableDuration::minutes(12),
        raft_store_max_leader_lease: ReadableDuration::secs(12),
        right_derive_when_split: false,
        allow_remove_leader: true,
        merge_max_log_gap: 3,
        merge_check_tick_interval: ReadableDuration::secs(11),
        use_delete_range: true,
        snap_generator_pool_size: 2,
        cleanup_import_sst_interval: ReadableDuration::minutes(12),
        region_max_size: ReadableSize(0),
        region_split_size: ReadableSize(0),
        local_read_batch_size: 33,
        apply_batch_system,
        store_batch_system,
        store_io_pool_size: 5,
        store_io_notify_capacity: 123456,
        future_poll_size: 2,
        hibernate_regions: false,
        dev_assert: true,
        apply_yield_duration: ReadableDuration::millis(333),
        perf_level: PerfLevel::Disable,
        evict_cache_on_memory_ratio: 0.8,
        cmd_batch: false,
        cmd_batch_concurrent_ready_max_count: 123,
        raft_write_size_limit: ReadableSize::mb(34),
        waterfall_metrics: true,
        io_reschedule_concurrent_max_count: 1234,
        io_reschedule_hotpot_duration: ReadableDuration::secs(4321),
        inspect_interval: ReadableDuration::millis(444),
        report_min_resolved_ts_interval: ReadableDuration::millis(233),
        raft_msg_flush_interval: ReadableDuration::micros(250),
        check_leader_lease_interval: ReadableDuration::millis(123),
        renew_leader_lease_advance_duration: ReadableDuration::millis(456),
        reactive_memory_lock_tick_interval: ReadableDuration::millis(566),
        reactive_memory_lock_timeout_tick: 8,
        report_region_buckets_tick_interval: ReadableDuration::secs(1234),
        max_snapshot_file_raw_size: ReadableSize::gb(10),
    };
    value.pd = PdConfig::new(vec!["example.com:443".to_owned()]);
    let titan_cf_config = TitanCfConfig {
        min_blob_size: ReadableSize(2018),
        blob_file_compression: CompressionType::Zstd,
        blob_cache_size: ReadableSize::gb(12),
        min_gc_batch_size: ReadableSize::kb(12),
        max_gc_batch_size: ReadableSize::mb(12),
        discardable_ratio: 0.00156,
        sample_ratio: 0.982,
        merge_small_file_threshold: ReadableSize::kb(21),
        blob_run_mode: BlobRunMode::Fallback,
        level_merge: true,
        range_merge: true,
        max_sorted_runs: 100,
        gc_merge_rewrite: true,
    };
    let titan_db_config = TitanDBConfig {
        enabled: true,
        dirname: "bar".to_owned(),
        disable_gc: false,
        max_background_gc: 9,
        purge_obsolete_files_period: ReadableDuration::secs(1),
    };
    value.rocksdb = DbConfig {
        wal_recovery_mode: DBRecoveryMode::AbsoluteConsistency,
        wal_dir: "/var".to_owned(),
        wal_ttl_seconds: 1,
        wal_size_limit: ReadableSize::kb(1),
        max_total_wal_size: ReadableSize::gb(1),
        max_background_jobs: 12,
        max_background_flushes: 4,
        max_manifest_file_size: ReadableSize::mb(12),
        create_if_missing: false,
        max_open_files: 12_345,
        enable_statistics: false,
        stats_dump_period: ReadableDuration::minutes(12),
        compaction_readahead_size: ReadableSize::kb(1),
        info_log_max_size: ReadableSize::kb(1),
        info_log_roll_time: ReadableDuration::secs(12),
        info_log_keep_log_file_num: 1000,
        info_log_dir: "/var".to_owned(),
        info_log_level: LogLevel::Info,
        rate_bytes_per_sec: ReadableSize::kb(1),
        rate_limiter_refill_period: ReadableDuration::millis(10),
        rate_limiter_mode: DBRateLimiterMode::AllIo,
        auto_tuned: None,
        rate_limiter_auto_tuned: false,
        bytes_per_sync: ReadableSize::mb(1),
        wal_bytes_per_sync: ReadableSize::kb(32),
        max_sub_compactions: 12,
        writable_file_max_buffer_size: ReadableSize::mb(12),
        use_direct_io_for_flush_and_compaction: true,
        enable_pipelined_write: false,
        enable_multi_batch_write: true,
        enable_unordered_write: true,
        defaultcf: DefaultCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            disable_write_stall: true,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(12),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: true,
            titan: titan_cf_config.clone(),
            prop_size_index_distance: 4000000,
            prop_keys_index_distance: 40000,
            enable_doubly_skiplist: false,
            enable_compaction_guard: false,
            compaction_guard_min_output_file_size: ReadableSize::mb(12),
            compaction_guard_max_output_file_size: ReadableSize::mb(34),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 1024,
            bottommost_zstd_compression_sample_size: 1024,
        },
        writecf: WriteCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            disable_write_stall: true,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(12),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: true,
            titan: TitanCfConfig {
                min_blob_size: ReadableSize(1024), // default value
                blob_file_compression: CompressionType::Lz4,
                blob_cache_size: ReadableSize::mb(0),
                min_gc_batch_size: ReadableSize::mb(16),
                max_gc_batch_size: ReadableSize::mb(64),
                discardable_ratio: 0.5,
                sample_ratio: 0.1,
                merge_small_file_threshold: ReadableSize::mb(8),
                blob_run_mode: BlobRunMode::ReadOnly,
                level_merge: false,
                range_merge: true,
                max_sorted_runs: 20,
                gc_merge_rewrite: false,
            },
            prop_size_index_distance: 4000000,
            prop_keys_index_distance: 40000,
            enable_doubly_skiplist: true,
            enable_compaction_guard: false,
            compaction_guard_min_output_file_size: ReadableSize::mb(12),
            compaction_guard_max_output_file_size: ReadableSize::mb(34),
            bottommost_level_compression: DBCompressionType::Zstd,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        },
        lockcf: LockCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            disable_write_stall: true,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(12),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: true,
            titan: TitanCfConfig {
                min_blob_size: ReadableSize(1024), // default value
                blob_file_compression: CompressionType::Lz4,
                blob_cache_size: ReadableSize::mb(0),
                min_gc_batch_size: ReadableSize::mb(16),
                max_gc_batch_size: ReadableSize::mb(64),
                discardable_ratio: 0.5,
                sample_ratio: 0.1,
                merge_small_file_threshold: ReadableSize::mb(8),
                blob_run_mode: BlobRunMode::ReadOnly, // default value
                level_merge: false,
                range_merge: true,
                max_sorted_runs: 20,
                gc_merge_rewrite: false,
            },
            prop_size_index_distance: 4000000,
            prop_keys_index_distance: 40000,
            enable_doubly_skiplist: true,
            enable_compaction_guard: true,
            compaction_guard_min_output_file_size: ReadableSize::mb(12),
            compaction_guard_max_output_file_size: ReadableSize::mb(34),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        },
        raftcf: RaftCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            disable_write_stall: true,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(12),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: true,
            titan: TitanCfConfig {
                min_blob_size: ReadableSize(1024), // default value
                blob_file_compression: CompressionType::Lz4,
                blob_cache_size: ReadableSize::mb(0),
                min_gc_batch_size: ReadableSize::mb(16),
                max_gc_batch_size: ReadableSize::mb(64),
                discardable_ratio: 0.5,
                sample_ratio: 0.1,
                merge_small_file_threshold: ReadableSize::mb(8),
                blob_run_mode: BlobRunMode::ReadOnly, // default value
                level_merge: false,
                range_merge: true,
                max_sorted_runs: 20,
                gc_merge_rewrite: false,
            },
            prop_size_index_distance: 4000000,
            prop_keys_index_distance: 40000,
            enable_doubly_skiplist: true,
            enable_compaction_guard: true,
            compaction_guard_min_output_file_size: ReadableSize::mb(12),
            compaction_guard_max_output_file_size: ReadableSize::mb(34),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        },
        titan: titan_db_config.clone(),
    };
    value.raftdb = RaftDbConfig {
        info_log_level: LogLevel::Info,
        wal_recovery_mode: DBRecoveryMode::SkipAnyCorruptedRecords,
        wal_dir: "/var".to_owned(),
        wal_ttl_seconds: 1,
        wal_size_limit: ReadableSize::kb(12),
        max_total_wal_size: ReadableSize::gb(1),
        max_background_jobs: 12,
        max_background_flushes: 4,
        max_manifest_file_size: ReadableSize::mb(12),
        create_if_missing: false,
        max_open_files: 12_345,
        enable_statistics: false,
        stats_dump_period: ReadableDuration::minutes(12),
        compaction_readahead_size: ReadableSize::kb(1),
        info_log_max_size: ReadableSize::kb(1),
        info_log_roll_time: ReadableDuration::secs(1),
        info_log_keep_log_file_num: 1000,
        info_log_dir: "/var".to_owned(),
        max_sub_compactions: 12,
        writable_file_max_buffer_size: ReadableSize::mb(12),
        use_direct_io_for_flush_and_compaction: true,
        enable_pipelined_write: false,
        enable_unordered_write: false,
        allow_concurrent_memtable_write: false,
        bytes_per_sync: ReadableSize::mb(1),
        wal_bytes_per_sync: ReadableSize::kb(32),
        defaultcf: RaftDefaultCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            disable_write_stall: true,
            soft_pending_compaction_bytes_limit: ReadableSize::gb(12),
            hard_pending_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: true,
            titan: titan_cf_config,
            prop_size_index_distance: 4000000,
            prop_keys_index_distance: 40000,
            enable_doubly_skiplist: true,
            enable_compaction_guard: true,
            compaction_guard_min_output_file_size: ReadableSize::mb(12),
            compaction_guard_max_output_file_size: ReadableSize::mb(34),
            bottommost_level_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        },
        titan: titan_db_config,
    };
    value.raft_engine.enable = false;
    let raft_engine_config = value.raft_engine.mut_config();
    raft_engine_config.dir = "test-dir".to_owned();
    raft_engine_config.batch_compression_threshold.0 = ReadableSize::kb(1).0;
    raft_engine_config.bytes_per_sync.0 = ReadableSize::kb(64).0;
    raft_engine_config.target_file_size.0 = ReadableSize::mb(1).0;
    raft_engine_config.purge_threshold.0 = ReadableSize::gb(1).0;
    raft_engine_config.recovery_mode = RecoveryMode::TolerateTailCorruption;
    raft_engine_config.recovery_read_block_size.0 = ReadableSize::kb(1).0;
    raft_engine_config.recovery_threads = 2;
    raft_engine_config.memory_limit = Some(RaftEngineReadableSize::gb(1));
    value.storage = StorageConfig {
        data_dir: "/var".to_owned(),
        gc_ratio_threshold: 1.2,
        max_key_size: 4096,
        scheduler_concurrency: 123,
        scheduler_worker_pool_size: 1,
        scheduler_pending_write_threshold: ReadableSize::kb(123),
        reserve_space: ReadableSize::gb(10),
        enable_async_apply_prewrite: true,
        api_version: 1,
        enable_ttl: true,
        ttl_check_poll_interval: ReadableDuration::hours(0),
        flow_control: FlowControlConfig {
            enable: false,
            l0_files_threshold: 10,
            memtables_threshold: 10,
            soft_pending_compaction_bytes_limit: ReadableSize(1),
            hard_pending_compaction_bytes_limit: ReadableSize(1),
        },
        block_cache: BlockCacheConfig {
            shared: true,
            capacity: Some(ReadableSize::gb(40)),
            num_shard_bits: 10,
            strict_capacity_limit: true,
            high_pri_pool_ratio: 0.8,
            memory_allocator: Some(String::from("nodump")),
        },
        io_rate_limit: IORateLimitConfig {
            max_bytes_per_sec: ReadableSize::mb(1000),
            mode: IORateLimitMode::AllIo,
            strict: true,
            foreground_read_priority: IOPriority::Low,
            foreground_write_priority: IOPriority::Low,
            flush_priority: IOPriority::Low,
            level_zero_compaction_priority: IOPriority::Low,
            compaction_priority: IOPriority::High,
            replication_priority: IOPriority::Low,
            load_balance_priority: IOPriority::Low,
            gc_priority: IOPriority::High,
            import_priority: IOPriority::High,
            export_priority: IOPriority::High,
            other_priority: IOPriority::Low,
        },
        background_error_recovery_window: ReadableDuration::hours(1),
    };
    value.coprocessor = CopConfig {
        split_region_on_table: false,
        batch_split_limit: 1,
        region_max_size: Some(ReadableSize::mb(12)),
        region_split_size: ReadableSize::mb(12),
        region_max_keys: Some(100000),
        region_split_keys: Some(100000),
        consistency_check_method: ConsistencyCheckMethod::Raw,
        perf_level: PerfLevel::Uninitialized,
        enable_region_bucket: true,
        region_bucket_size: ReadableSize::mb(1),
        region_size_threshold_for_approximate: ReadableSize::mb(3),
        region_bucket_merge_size_ratio: 0.4,
    };
    let mut cert_allowed_cn = HashSet::default();
    cert_allowed_cn.insert("example.tikv.com".to_owned());
    value.security = SecurityConfig {
        ca_path: "invalid path".to_owned(),
        cert_path: "invalid path".to_owned(),
        key_path: "invalid path".to_owned(),
        override_ssl_target: "".to_owned(),
        cert_allowed_cn,
        redact_info_log: Some(true),
        encryption: EncryptionConfig {
            data_encryption_method: EncryptionMethod::Aes128Ctr,
            data_key_rotation_period: ReadableDuration::days(14),
            enable_file_dictionary_log: false,
            file_dictionary_rewrite_threshold: 123456,
            master_key: MasterKeyConfig::File {
                config: FileConfig {
                    path: "/master/key/path".to_owned(),
                },
            },
            previous_master_key: MasterKeyConfig::Plaintext,
        },
    };
    value.backup = BackupConfig {
        num_threads: 456,
        batch_size: 7,
        sst_max_size: ReadableSize::mb(789),
        s3_multi_part_size: ReadableSize::mb(15),
        hadoop: HadoopConfig {
            home: "/root/hadoop".to_string(),
            linux_user: "hadoop".to_string(),
        },
        ..Default::default()
    };
    value.backup_stream = BackupStreamConfig {
        num_threads: 8,
        ..Default::default()
    };
    value.import = ImportConfig {
        num_threads: 123,
        stream_channel_window: 123,
        import_mode_timeout: ReadableDuration::secs(1453),
    };
    value.panic_when_unexpected_key_or_data = true;
    value.gc = GcConfig {
        ratio_threshold: 1.2,
        batch_keys: 256,
        max_write_bytes_per_sec: ReadableSize::mb(10),
        enable_compaction_filter: false,
        compaction_filter_skip_version_check: true,
    };
    value.pessimistic_txn = PessimisticTxnConfig {
        wait_for_lock_timeout: ReadableDuration::millis(10),
        wake_up_delay_duration: ReadableDuration::millis(100),
        pipelined: false,
        in_memory: false,
    };
    value.cdc = CdcConfig {
        min_ts_interval: ReadableDuration::secs(4),
        old_value_cache_size: 0,
        hibernate_regions_compatible: false,
        incremental_scan_threads: 3,
        incremental_scan_concurrency: 4,
        incremental_scan_speed_limit: ReadableSize(7),
        incremental_scan_ts_filter_ratio: 0.7,
        old_value_cache_memory_quota: ReadableSize::mb(14),
        sink_memory_quota: ReadableSize::mb(7),
    };
    value.resolved_ts = ResolvedTsConfig {
        enable: true,
        advance_ts_interval: ReadableDuration::secs(5),
        scan_lock_pool_size: 1,
    };
    value.causal_ts = CausalTsConfig {
        renew_interval: ReadableDuration::millis(100),
        renew_batch_min_size: 100,
    };

    let custom = read_file_in_project_dir("integrations/config/test-custom.toml");
    let load = toml::from_str(&custom).unwrap();
    if value != load {
        diff_config(&value, &load);
    }
    let dump = toml::to_string_pretty(&load).unwrap();
    let load_from_dump = toml::from_str(&dump).unwrap();
    if load != load_from_dump {
        diff_config(&load, &load_from_dump);
    }
}

fn diff_config(lhs: &TiKvConfig, rhs: &TiKvConfig) {
    let lhs_str = format!("{:?}", lhs);
    let rhs_str = format!("{:?}", rhs);

    fn find_index(l: impl Iterator<Item = (u8, u8)>) -> usize {
        let it = l
            .enumerate()
            .take_while(|(_, (l, r))| l == r)
            .filter(|(_, (l, _))| *l == b' ');
        let mut last = None;
        let mut second = None;
        for a in it {
            second = last;
            last = Some(a);
        }
        second.map_or(0, |(i, _)| i)
    }
    let cpl = find_index(lhs_str.bytes().zip(rhs_str.bytes()));
    let csl = find_index(lhs_str.bytes().rev().zip(rhs_str.bytes().rev()));
    if cpl + csl > lhs_str.len() || cpl + csl > rhs_str.len() {
        assert_eq!(lhs, rhs);
    }
    let lhs_diff = String::from_utf8_lossy(&lhs_str.as_bytes()[cpl..lhs_str.len() - csl]);
    let rhs_diff = String::from_utf8_lossy(&rhs_str.as_bytes()[cpl..rhs_str.len() - csl]);
    panic!(
        "config not matched:\nlhs: ...{}...,\nrhs: ...{}...",
        lhs_diff, rhs_diff
    );
}

#[test]
fn test_serde_default_config() {
    let cfg: TiKvConfig = toml::from_str("").unwrap();
    assert_eq!(cfg, TiKvConfig::default());

    let content = read_file_in_project_dir("integrations/config/test-default.toml");
    let cfg: TiKvConfig = toml::from_str(&content).unwrap();
    assert_eq!(cfg, TiKvConfig::default());
}

#[test]
fn test_readpool_default_config() {
    let content = r#"
        [readpool.unified]
        max-thread-count = 1
    "#;
    let cfg: TiKvConfig = toml::from_str(content).unwrap();
    let mut expected = TiKvConfig::default();
    expected.readpool.unified.max_thread_count = 1;
    assert_eq!(cfg, expected);
}

#[test]
fn test_do_not_use_unified_readpool_with_legacy_config() {
    let content = r#"
        [readpool.storage]
        normal-concurrency = 1

        [readpool.coprocessor]
        normal-concurrency = 1
    "#;
    let cfg: TiKvConfig = toml::from_str(content).unwrap();
    assert!(!cfg.readpool.is_unified_pool_enabled());
}

#[test]
fn test_block_cache_backward_compatible() {
    let content = read_file_in_project_dir("integrations/config/test-cache-compatible.toml");
    let mut cfg: TiKvConfig = toml::from_str(&content).unwrap();
    assert!(cfg.storage.block_cache.shared);
    assert!(cfg.storage.block_cache.capacity.is_none());
    cfg.compatible_adjust();
    assert!(cfg.storage.block_cache.capacity.is_some());
    assert_eq!(
        cfg.storage.block_cache.capacity.unwrap().0,
        cfg.rocksdb.defaultcf.block_cache_size.0
            + cfg.rocksdb.writecf.block_cache_size.0
            + cfg.rocksdb.lockcf.block_cache_size.0
            + cfg.raftdb.defaultcf.block_cache_size.0
    );
}

#[test]
fn test_log_backward_compatible() {
    let content = read_file_in_project_dir("integrations/config/test-log-compatible.toml");
    let mut cfg: TiKvConfig = toml::from_str(&content).unwrap();
    assert_eq!(cfg.log.level, slog::Level::Info);
    assert_eq!(cfg.log.file.filename, "");
    assert_eq!(cfg.log.format, LogFormat::Text);
    assert_eq!(cfg.log.file.max_size, 300);
    cfg.logger_compatible_adjust();
    assert_eq!(cfg.log.level, slog::Level::Critical);
    assert_eq!(cfg.log.file.filename, "foo");
    assert_eq!(cfg.log.format, LogFormat::Json);
    assert_eq!(cfg.log.file.max_size, 1024);
}
