// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::min, collections::HashMap, sync::Arc, time::Duration, u64};

use batch_system::Config as BatchSystemConfig;
use engine_traits::{perf_level_serde, PerfLevel};
use lazy_static::lazy_static;
use online_config::{ConfigChange, ConfigManager, ConfigValue, OnlineConfig};
use prometheus::register_gauge_vec;
use serde::{Deserialize, Serialize};
use serde_with::with_prefix;
use tikv_util::{
    box_err,
    config::{ReadableDuration, ReadableSchedule, ReadableSize, VersionTrack},
    error, info,
    sys::SysQuota,
    warn,
    worker::Scheduler,
};
use time::Duration as TimeDuration;

use super::worker::{RaftStoreBatchComponent, RefreshConfigTask};
use crate::{coprocessor::config::RAFTSTORE_V2_SPLIT_SIZE, Result};

lazy_static! {
    pub static ref CONFIG_RAFTSTORE_GAUGE: prometheus::GaugeVec = register_gauge_vec!(
        "tikv_config_raftstore",
        "Config information of raftstore",
        &["name"]
    )
    .unwrap();
}

#[doc(hidden)]
pub const DEFAULT_SNAP_MAX_BYTES_PER_SEC: u64 = 100 * 1024 * 1024;

// The default duration of waiting split. If a split does not finish in
// one-third of receiving snapshot time, split is likely very slow, so it is
// better to prioritize accepting a snapshot
const DEFAULT_SNAP_WAIT_SPLIT_DURATION: ReadableDuration =
    ReadableDuration::secs(RAFTSTORE_V2_SPLIT_SIZE.0 / DEFAULT_SNAP_MAX_BYTES_PER_SEC / 3);

with_prefix!(prefix_apply "apply-");
with_prefix!(prefix_store "store-");
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    // minimizes disruption when a partitioned node rejoins the cluster by using a two phase
    // election.
    #[online_config(skip)]
    pub prevote: bool,
    #[online_config(skip)]
    pub raftdb_path: String,

    // store capacity. 0 means no limit.
    #[online_config(skip)]
    pub capacity: ReadableSize,

    // raft_base_tick_interval is a base tick interval (ms).
    #[online_config(hidden)]
    pub raft_base_tick_interval: ReadableDuration,
    #[online_config(hidden)]
    pub raft_heartbeat_ticks: usize,
    #[online_config(hidden)]
    pub raft_election_timeout_ticks: usize,
    #[online_config(hidden)]
    pub raft_min_election_timeout_ticks: usize,
    #[online_config(hidden)]
    pub raft_max_election_timeout_ticks: usize,
    pub raft_max_size_per_msg: ReadableSize,
    pub raft_max_inflight_msgs: usize,
    // When the entry exceed the max size, reject to propose it.
    pub raft_entry_max_size: ReadableSize,

    // Interval to compact unnecessary raft log.
    pub raft_log_compact_sync_interval: ReadableDuration,
    // Interval to gc unnecessary raft log.
    pub raft_log_gc_tick_interval: ReadableDuration,
    // Interval to request voter_replicated_index for gc unnecessary raft log,
    // if the leader has not initiated gc for a long time.
    pub request_voter_replicated_index_interval: ReadableDuration,
    // A threshold to gc stale raft log, must >= 1.
    pub raft_log_gc_threshold: u64,
    // When entry count exceed this value, gc will be forced trigger.
    pub raft_log_gc_count_limit: Option<u64>,
    // When the approximate size of raft log entries exceed this value,
    // gc will be forced trigger.
    pub raft_log_gc_size_limit: Option<ReadableSize>,
    /// The maximum raft log numbers that applied_index can be ahead of
    /// persisted_index.
    pub max_apply_unpersisted_log_limit: u64,
    // follower will reject this follower request to avoid falling behind leader too far,
    // when the read index is ahead of the sum between the applied index and
    // follower_read_max_log_gap,
    #[doc(hidden)]
    pub follower_read_max_log_gap: u64,
    // Old Raft logs could be reserved if `raft_log_gc_threshold` is not reached.
    // GC them after ticks `raft_log_reserve_max_ticks` times.
    #[doc(hidden)]
    #[online_config(hidden)]
    pub raft_log_reserve_max_ticks: usize,
    // Old logs in Raft engine needs to be purged peridically.
    pub raft_engine_purge_interval: ReadableDuration,
    #[doc(hidden)]
    #[online_config(hidden)]
    pub max_manual_flush_rate: f64,
    // When a peer is not responding for this time, leader will not keep entry cache for it.
    pub raft_entry_cache_life_time: ReadableDuration,
    // When a peer is newly added, reject transferring leader to the peer for a while.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    #[deprecated = "The configuration has been removed. It has no effect"]
    pub raft_reject_transfer_leader_duration: ReadableDuration,

    /// Whether to disable checking quorum for the raft group. This will make
    /// leader lease unavailable.
    /// It cannot be changed in the config file, the only way to change it is
    /// programmatically change the config structure during bootstrapping
    /// the cluster.
    #[doc(hidden)]
    #[serde(skip)]
    #[online_config(skip)]
    pub unsafe_disable_check_quorum: bool,

    // Interval (ms) to check region whether need to be split or not.
    pub split_region_check_tick_interval: ReadableDuration,
    /// When size change of region exceed the diff since last check, it
    /// will be checked again whether it should be split.
    pub region_split_check_diff: Option<ReadableSize>,
    /// Interval (ms) to check whether start compaction for a region.
    pub region_compact_check_interval: ReadableDuration,
    /// Number of regions for each time checking.
    pub region_compact_check_step: Option<u64>,
    /// Minimum number of tombstones to trigger manual compaction.
    pub region_compact_min_tombstones: u64,
    /// Minimum percentage of tombstones to trigger manual compaction.
    /// Should between 1 and 100.
    pub region_compact_tombstones_percent: u64,
    /// Minimum number of redundant rows to trigger manual compaction.
    pub region_compact_min_redundant_rows: u64,
    /// Minimum percentage of redundant rows to trigger manual compaction.
    /// Should between 1 and 100.
    pub region_compact_redundant_rows_percent: Option<u64>,
    pub pd_heartbeat_tick_interval: ReadableDuration,
    pub pd_store_heartbeat_tick_interval: ReadableDuration,
    pub pd_report_min_resolved_ts_interval: ReadableDuration,
    pub snap_mgr_gc_tick_interval: ReadableDuration,
    pub snap_gc_timeout: ReadableDuration,
    /// The duration of snapshot waits for region split. It prevents leader from
    /// sending unnecessary snapshots when split is slow.
    /// It is only effective in raftstore v2.
    pub snap_wait_split_duration: ReadableDuration,
    pub lock_cf_compact_interval: ReadableDuration,
    pub lock_cf_compact_bytes_threshold: ReadableSize,

    /// Hours of the day during which we may execute a periodic full compaction.
    /// If not set or empty, periodic full compaction will not run. In toml this
    /// should be a list of timesin "HH:MM" format with an optional timezone
    /// offset. If no timezone is specified, local timezone is used. E.g.,
    /// `["23:00 +0000", "03:00 +0700"]` or `["23:00", "03:00"]`.
    pub periodic_full_compact_start_times: ReadableSchedule,
    /// Do not start a full compaction if cpu utilization exceeds this number.
    pub periodic_full_compact_start_max_cpu: f64,

    #[online_config(skip)]
    pub notify_capacity: usize,
    pub messages_per_tick: usize,

    /// When a peer is not active for max_peer_down_duration,
    /// the peer is considered to be down and is reported to PD.
    pub max_peer_down_duration: ReadableDuration,

    /// If the leader of a peer is missing for longer than
    /// max_leader_missing_duration, the peer would ask pd to confirm
    /// whether it is valid in any region. If the peer is stale and is not
    /// valid in any region, it will destroy itself.
    pub max_leader_missing_duration: ReadableDuration,
    /// Similar to the max_leader_missing_duration, instead it will log warnings
    /// and try to alert monitoring systems, if there is any.
    pub abnormal_leader_missing_duration: ReadableDuration,
    pub peer_stale_state_check_interval: ReadableDuration,
    /// Interval to check GC peers.
    #[doc(hidden)]
    pub gc_peer_check_interval: ReadableDuration,

    #[online_config(hidden)]
    pub leader_transfer_max_log_lag: u64,

    #[online_config(skip)]
    pub snap_apply_batch_size: ReadableSize,

    /// When applying a Region snapshot, its SST files can be modified by TiKV
    /// itself. However those files could be read-only, for example, a TiKV
    /// [agent](cmd/tikv-agent) is started based on a read-only remains. So
    /// we can set `snap_apply_copy_symlink` to `true` to make a copy on
    /// those SST files.
    #[online_config(skip)]
    pub snap_apply_copy_symlink: bool,

    // used to periodically check whether schedule pending applies in region runner
    #[doc(hidden)]
    #[online_config(skip)]
    pub region_worker_tick_interval: ReadableDuration,

    // used to periodically check whether we should delete a stale peer's range in
    // region runner
    #[doc(hidden)]
    #[online_config(skip)]
    pub clean_stale_ranges_tick: usize,

    // Interval (ms) to check region whether the data is consistent.
    pub consistency_check_interval: ReadableDuration,

    #[online_config(hidden)]
    pub report_region_flow_interval: ReadableDuration,

    // The lease provided by a successfully proposed and applied entry.
    pub raft_store_max_leader_lease: ReadableDuration,

    // Interval of scheduling a tick to check the leader lease.
    // It will be set to raft_store_max_leader_lease/4 by default.
    pub check_leader_lease_interval: ReadableDuration,

    // Check if leader lease will expire at `current_time + renew_leader_lease_advance_duration`.
    // It will be set to raft_store_max_leader_lease/4 by default.
    pub renew_leader_lease_advance_duration: ReadableDuration,

    // Set true to allow handling request vote messages within one election time
    // after TiKV start.
    //
    // Note: set to true may break leader lease. It should only be true in tests.
    #[doc(hidden)]
    #[serde(skip)]
    #[online_config(skip)]
    pub allow_unsafe_vote_after_start: bool,

    // Right region derive origin region id when split.
    #[online_config(hidden)]
    pub right_derive_when_split: bool,

    /// This setting can only ensure conf remove will not be proposed by the
    /// peer being removed. But it can't guarantee the remove is applied
    /// when the target is not leader. That means we always need to check if
    /// it's working as expected when a leader applies a self-remove conf
    /// change. Keep the configuration only for convenient test.
    #[cfg(any(test, feature = "testexport"))]
    pub allow_remove_leader: bool,

    /// Max log gap allowed to propose merge.
    #[online_config(hidden)]
    pub merge_max_log_gap: u64,
    /// Interval to re-propose merge.
    pub merge_check_tick_interval: ReadableDuration,

    #[online_config(hidden)]
    pub use_delete_range: bool,

    pub snap_generator_pool_size: usize,

    pub cleanup_import_sst_interval: ReadableDuration,

    /// Maximum size of every local read task batch.
    pub local_read_batch_size: u64,

    #[online_config(submodule)]
    #[serde(flatten, with = "prefix_apply")]
    pub apply_batch_system: BatchSystemConfig,

    #[online_config(submodule)]
    #[serde(flatten, with = "prefix_store")]
    pub store_batch_system: BatchSystemConfig,

    /// If it is 0, it means io tasks are handled in store threads.
    pub store_io_pool_size: usize,

    #[online_config(skip)]
    pub store_io_notify_capacity: usize,

    #[online_config(skip)]
    pub future_poll_size: usize,
    #[online_config(skip)]
    pub hibernate_regions: bool,
    #[doc(hidden)]
    #[online_config(hidden)]
    pub dev_assert: bool,
    #[online_config(hidden)]
    pub apply_yield_duration: ReadableDuration,
    /// yield the fsm when apply flushed data size exceeds this threshold.
    /// the yield is check after commit, so the actual handled messages can be
    /// bigger than the configed value.
    // NOTE: the default value is much smaller than the default max raft batch msg size(0.2
    // * raft_entry_max_size), this is intentional because in the common case, a raft entry
    // is unlikely to exceed this threshold, but in case when raftstore is the bottleneck,
    // we still allow big raft batch for better throughput.
    pub apply_yield_write_size: ReadableSize,

    #[serde(with = "perf_level_serde")]
    #[online_config(skip)]
    pub perf_level: PerfLevel,

    #[doc(hidden)]
    #[online_config(skip)]
    /// Disable this feature by set to 0, logic will be removed in other pr.
    /// When TiKV memory usage reaches `memory_usage_high_water` it will try to
    /// limit memory increasing. For raftstore layer entries will be evicted
    /// from entry cache, if they utilize memory more than
    /// `evict_cache_on_memory_ratio` * total.
    ///
    /// Set it to 0 can disable cache evict.
    // By default it's 0.1. So for different system memory capacity, cache evict happens:
    // * system=8G,  memory_usage_limit=6G,  evict=0.6G
    // * system=16G, memory_usage_limit=12G, evict=1.2G
    // * system=32G, memory_usage_limit=24G, evict=2.4G
    pub evict_cache_on_memory_ratio: f64,

    pub cmd_batch: bool,

    /// When the count of concurrent ready exceeds this value, command will not
    /// be proposed until the previous ready has been persisted.
    /// If `cmd_batch` is 0, this config will have no effect.
    /// If it is 0, it means no limit.
    pub cmd_batch_concurrent_ready_max_count: usize,

    /// When the size of raft db writebatch exceeds this value, write will be
    /// triggered.
    pub raft_write_size_limit: ReadableSize,

    /// When the size of raft db writebatch is smaller than this value, write
    /// will wait for a while to make the writebatch larger, which will reduce
    /// the write amplification.
    #[doc(hidden)]
    pub raft_write_batch_size_hint: ReadableSize,

    /// When the size of raft db writebatch is smaller than this value, write
    /// will wait for a while. This is used to reduce the write amplification.
    /// It should be smaller than 1ms. Invalid to use too long duration because
    /// it will make the write request wait too long.
    #[doc(hidden)]
    pub raft_write_wait_duration: ReadableDuration,

    pub waterfall_metrics: bool,

    pub io_reschedule_concurrent_max_count: usize,
    pub io_reschedule_hotpot_duration: ReadableDuration,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    #[deprecated = "The configuration has been removed. Batch is done in raft client."]
    pub raft_msg_flush_interval: ReadableDuration,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    #[deprecated = "The configuration has been moved to coprocessor.region_max_size."]
    pub region_max_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    #[deprecated = "The configuration has been moved to coprocessor.region_split_size."]
    pub region_split_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    #[deprecated = "The configuration has been removed. The time to clean stale peer safely can be decided based on RocksDB snapshot sequence number."]
    pub clean_stale_peer_delay: ReadableDuration,

    // Interval to inspect the latency of raftstore for slow store detection.
    pub inspect_interval: ReadableDuration,
    /// Threshold of CPU utilization to inspect for slow store detection.
    #[doc(hidden)]
    pub inspect_cpu_util_thd: f64,

    // The unsensitive(increase it to reduce sensitiveness) of the cause-trend detection
    pub slow_trend_unsensitive_cause: f64,
    // The unsensitive(increase it to reduce sensitiveness) of the result-trend detection
    pub slow_trend_unsensitive_result: f64,
    // The sensitiveness of slowness on network-io.
    pub slow_trend_network_io_factor: f64,

    /// Interval to check whether to reactivate in-memory pessimistic lock after
    /// being disabled before transferring leader.
    pub reactive_memory_lock_tick_interval: ReadableDuration,
    /// Max tick count before reactivating in-memory pessimistic lock.
    pub reactive_memory_lock_timeout_tick: usize,
    // Interval of scheduling a tick to report region buckets.
    pub report_region_buckets_tick_interval: ReadableDuration,

    /// Interval to check long uncommitted proposals.
    #[doc(hidden)]
    pub check_long_uncommitted_interval: ReadableDuration,
    /// Base threshold of long uncommitted proposal.
    #[doc(hidden)]
    pub long_uncommitted_base_threshold: ReadableDuration,

    /// Max duration for the entry cache to be warmed up.
    /// Set it to 0 to disable warmup.
    pub max_entry_cache_warmup_duration: ReadableDuration,

    #[doc(hidden)]
    pub max_snapshot_file_raw_size: ReadableSize,

    pub unreachable_backoff: ReadableDuration,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    // Interval to check peers availability info.
    pub check_peers_availability_interval: ReadableDuration,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    // Interval to check if need to request snapshot.
    pub check_request_snapshot_interval: ReadableDuration,

    /// Make raftstore v1 learners compatible with raftstore v2 by:
    /// * Recving tablet snapshot from v2.
    /// * Responsing GcPeerRequest from v2.
    #[doc(hidden)]
    #[online_config(hidden)]
    #[serde(alias = "enable-partitioned-raft-kv-compatible-learner")]
    pub enable_v2_compatible_learner: bool,

    /// The minimal count of region pending on applying raft logs.
    /// Only when the count of regions which not pending on applying logs is
    /// less than the threshold, can the raftstore supply service.
    #[doc(hidden)]
    #[online_config(hidden)]
    pub min_pending_apply_region_count: u64,

    /// Whether to skip manual compaction in the clean up worker for `write` and
    /// `default` column family
    #[doc(hidden)]
    pub skip_manual_compaction_in_clean_up_worker: bool,

    /// Minimal size of snapshot for generatng and applying with ingestion.
    /// If the size of snapshot is smaller than this value, it will be generated
    /// and applied without ingestion, just using plain write.
    pub snap_min_approximate_size: ReadableSize,
}

impl Default for Config {
    #[allow(deprecated)]
    fn default() -> Config {
        Config {
            prevote: true,
            raftdb_path: String::new(),
            capacity: ReadableSize(0),
            raft_base_tick_interval: ReadableDuration::secs(1),
            raft_heartbeat_ticks: 2,
            raft_election_timeout_ticks: 10,
            raft_min_election_timeout_ticks: 0,
            raft_max_election_timeout_ticks: 0,
            raft_max_size_per_msg: ReadableSize::mb(1),
            raft_max_inflight_msgs: 256,
            raft_entry_max_size: ReadableSize::mb(8),
            raft_log_compact_sync_interval: ReadableDuration::secs(2),
            raft_log_gc_tick_interval: ReadableDuration::secs(3),
            request_voter_replicated_index_interval: ReadableDuration::minutes(5),
            raft_log_gc_threshold: 50,
            raft_log_gc_count_limit: None,
            raft_log_gc_size_limit: None,
            max_apply_unpersisted_log_limit: 1024,
            follower_read_max_log_gap: 100,
            raft_log_reserve_max_ticks: 6,
            raft_engine_purge_interval: ReadableDuration::secs(10),
            max_manual_flush_rate: 3.0,
            raft_entry_cache_life_time: ReadableDuration::secs(30),
            raft_reject_transfer_leader_duration: ReadableDuration::secs(3),
            split_region_check_tick_interval: ReadableDuration::secs(10),
            region_split_check_diff: None,
            region_compact_check_interval: ReadableDuration::minutes(5),
            region_compact_check_step: None,
            region_compact_min_tombstones: 10000,
            region_compact_tombstones_percent: 30,
            region_compact_min_redundant_rows: 50000,
            region_compact_redundant_rows_percent: Some(20),
            pd_heartbeat_tick_interval: ReadableDuration::minutes(1),
            pd_store_heartbeat_tick_interval: ReadableDuration::secs(10),
            pd_report_min_resolved_ts_interval: ReadableDuration::secs(1),
            // Disable periodic full compaction by default.
            periodic_full_compact_start_times: ReadableSchedule::default(),
            // If periodic full compaction is enabled, do not start a full compaction
            // if the CPU utilization is over 10%.
            periodic_full_compact_start_max_cpu: 0.1,
            notify_capacity: 40960,
            snap_mgr_gc_tick_interval: ReadableDuration::minutes(1),
            snap_gc_timeout: ReadableDuration::hours(4),
            snap_wait_split_duration: DEFAULT_SNAP_WAIT_SPLIT_DURATION,
            messages_per_tick: 4096,
            max_peer_down_duration: ReadableDuration::minutes(10),
            max_leader_missing_duration: ReadableDuration::hours(2),
            abnormal_leader_missing_duration: ReadableDuration::minutes(10),
            peer_stale_state_check_interval: ReadableDuration::minutes(5),
            leader_transfer_max_log_lag: 128,
            snap_apply_batch_size: ReadableSize::mb(10),
            snap_apply_copy_symlink: false,
            region_worker_tick_interval: if cfg!(feature = "test") {
                ReadableDuration::millis(200)
            } else {
                ReadableDuration::millis(1000)
            },
            clean_stale_ranges_tick: if cfg!(feature = "test") { 1 } else { 10 },
            lock_cf_compact_interval: ReadableDuration::minutes(10),
            lock_cf_compact_bytes_threshold: ReadableSize::mb(256),
            // Disable consistency check by default as it will hurt performance.
            // We should turn on this only in our tests.
            consistency_check_interval: ReadableDuration::secs(0),
            report_region_flow_interval: ReadableDuration::minutes(1),
            raft_store_max_leader_lease: ReadableDuration::secs(9),
            right_derive_when_split: true,
            #[cfg(any(test, feature = "testexport"))]
            allow_remove_leader: false,
            merge_max_log_gap: 10,
            merge_check_tick_interval: ReadableDuration::secs(2),
            use_delete_range: false,
            snap_generator_pool_size: 2,
            cleanup_import_sst_interval: ReadableDuration::minutes(10),
            local_read_batch_size: 1024,
            apply_batch_system: BatchSystemConfig::default(),
            store_batch_system: BatchSystemConfig::default(),
            store_io_pool_size: 1,
            store_io_notify_capacity: 40960,
            future_poll_size: 1,
            hibernate_regions: true,
            dev_assert: false,
            apply_yield_duration: ReadableDuration::millis(500),
            apply_yield_write_size: ReadableSize::kb(32),
            perf_level: PerfLevel::Uninitialized,
            evict_cache_on_memory_ratio: 0.1,
            cmd_batch: true,
            cmd_batch_concurrent_ready_max_count: 1,
            raft_write_size_limit: ReadableSize::mb(1),
            raft_write_batch_size_hint: ReadableSize::kb(8),
            raft_write_wait_duration: ReadableDuration::micros(20),
            waterfall_metrics: true,
            io_reschedule_concurrent_max_count: 4,
            io_reschedule_hotpot_duration: ReadableDuration::secs(5),
            raft_msg_flush_interval: ReadableDuration::micros(250),
            reactive_memory_lock_tick_interval: ReadableDuration::secs(2),
            reactive_memory_lock_timeout_tick: 5,
            check_long_uncommitted_interval: ReadableDuration::secs(10),
            // In some cases, such as rolling upgrade, some regions' commit log
            // duration can be 12 seconds. Before #13078 is merged,
            // the commit log duration can be 2.8 minutes. So maybe
            // 20s is a relatively reasonable base threshold. Generally,
            // the log commit duration is less than 1s. Feel free to adjust
            // this config :)
            long_uncommitted_base_threshold: ReadableDuration::secs(20),
            max_entry_cache_warmup_duration: ReadableDuration::secs(1),

            // They are preserved for compatibility check.
            region_max_size: ReadableSize(0),
            region_split_size: ReadableSize(0),
            clean_stale_peer_delay: ReadableDuration::minutes(0),
            inspect_interval: ReadableDuration::millis(100),
            // The default value of `inspect_cpu_util_thd` is 0.4, which means
            // when the cpu utilization is greater than 40%, the store might be
            // regarded as a slow node if there exists delayed inspected messages.
            // It's good enough for most cases to reduce the false positive rate.
            inspect_cpu_util_thd: 0.4,
            // The param `slow_trend_unsensitive_cause == 2.0` can yield good results,
            // make it `10.0` to reduce a bit sensitiveness because SpikeFilter is disabled
            slow_trend_unsensitive_cause: 10.0,
            slow_trend_unsensitive_result: 0.5,
            slow_trend_network_io_factor: 0.0,
            check_leader_lease_interval: ReadableDuration::secs(0),
            renew_leader_lease_advance_duration: ReadableDuration::secs(0),
            allow_unsafe_vote_after_start: false,
            report_region_buckets_tick_interval: ReadableDuration::secs(10),
            gc_peer_check_interval: ReadableDuration::secs(60),
            max_snapshot_file_raw_size: ReadableSize::mb(100),
            unreachable_backoff: ReadableDuration::secs(10),
            // TODO: make its value reasonable
            check_peers_availability_interval: ReadableDuration::secs(30),
            // TODO: make its value reasonable
            check_request_snapshot_interval: ReadableDuration::minutes(1),
            enable_v2_compatible_learner: false,
            unsafe_disable_check_quorum: false,
            min_pending_apply_region_count: 10,
            skip_manual_compaction_in_clean_up_worker: false,
            snap_min_approximate_size: ReadableSize::mb(2),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn new_raft_config(&self, peer_id: u64, applied_index: u64) -> raft::Config {
        raft::Config {
            id: peer_id,
            election_tick: self.raft_election_timeout_ticks,
            heartbeat_tick: self.raft_heartbeat_ticks,
            min_election_tick: self.raft_min_election_timeout_ticks,
            max_election_tick: self.raft_max_election_timeout_ticks,
            max_size_per_msg: self.raft_max_size_per_msg.0,
            max_inflight_msgs: self.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            skip_bcast_commit: true,
            pre_vote: self.prevote,
            max_committed_size_per_ready: ReadableSize::mb(16).0,
            ..Default::default()
        }
    }

    pub fn raft_store_max_leader_lease(&self) -> TimeDuration {
        TimeDuration::from_std(self.raft_store_max_leader_lease.0).unwrap()
    }

    pub fn raft_base_tick_interval(&self) -> TimeDuration {
        TimeDuration::from_std(self.raft_base_tick_interval.0).unwrap()
    }

    pub fn raft_heartbeat_interval(&self) -> Duration {
        self.raft_base_tick_interval.0 * self.raft_heartbeat_ticks as u32
    }

    pub fn check_leader_lease_interval(&self) -> TimeDuration {
        TimeDuration::from_std(self.check_leader_lease_interval.0).unwrap()
    }

    pub fn renew_leader_lease_advance_duration(&self) -> TimeDuration {
        TimeDuration::from_std(self.renew_leader_lease_advance_duration.0).unwrap()
    }

    pub fn raft_log_gc_count_limit(&self) -> u64 {
        self.raft_log_gc_count_limit.unwrap()
    }

    pub fn raft_log_gc_size_limit(&self) -> ReadableSize {
        self.raft_log_gc_size_limit.unwrap()
    }

    pub fn follower_read_max_log_gap(&self) -> u64 {
        self.follower_read_max_log_gap
    }

    pub fn region_compact_check_step(&self) -> u64 {
        self.region_compact_check_step.unwrap()
    }

    pub fn region_compact_redundant_rows_percent(&self) -> u64 {
        self.region_compact_redundant_rows_percent.unwrap()
    }

    #[inline]
    pub fn warmup_entry_cache_enabled(&self) -> bool {
        self.max_entry_cache_warmup_duration.0 != Duration::from_secs(0)
    }

    pub fn region_split_check_diff(&self) -> ReadableSize {
        self.region_split_check_diff.unwrap()
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn allow_remove_leader(&self) -> bool {
        self.allow_remove_leader
    }

    #[cfg(not(any(test, feature = "testexport")))]
    pub fn allow_remove_leader(&self) -> bool {
        false
    }

    pub fn optimize_for(&mut self, raft_kv_v2: bool) {
        if self.region_compact_check_step.is_none() {
            if raft_kv_v2 {
                self.region_compact_check_step = Some(5);
            } else {
                self.region_compact_check_step = Some(100);
            }
        }

        // When use raft kv v2, we can set raft log gc size limit to a smaller value to
        // avoid too many entry logs in cache.
        // The snapshot support to increment snapshot sst, so the old snapshot files
        // still be useful even if needs to sent snapshot again.
        if self.raft_log_gc_size_limit.is_none() && raft_kv_v2 {
            self.raft_log_gc_size_limit = Some(ReadableSize::mb(200));
        }

        if self.raft_log_gc_count_limit.is_none() && raft_kv_v2 {
            self.raft_log_gc_count_limit = Some(10000);
        }
    }

    pub fn validate(
        &mut self,
        region_split_size: ReadableSize,
        enable_region_bucket: bool,
        region_bucket_size: ReadableSize,
        raft_kv_v2: bool,
    ) -> Result<()> {
        if self.raft_heartbeat_ticks == 0 {
            return Err(box_err!("heartbeat tick must greater than 0"));
        }

        if self.raft_election_timeout_ticks != 10 {
            warn!(
                "Election timeout ticks needs to be same across all the cluster, \
                 otherwise it may lead to inconsistency."
            );
        }
        if self.allow_unsafe_vote_after_start {
            warn!(
                "allow_unsafe_vote_after_start need to be false, otherwise \
                it may lead to inconsistency"
            );
        }

        if self.raft_election_timeout_ticks <= self.raft_heartbeat_ticks {
            return Err(box_err!(
                "election tick must be greater than heartbeat tick"
            ));
        }

        if self.raft_min_election_timeout_ticks == 0 {
            self.raft_min_election_timeout_ticks = self.raft_election_timeout_ticks;
        }

        if self.raft_max_election_timeout_ticks == 0 {
            self.raft_max_election_timeout_ticks = self.raft_election_timeout_ticks * 2;
        }

        if self.raft_min_election_timeout_ticks < self.raft_election_timeout_ticks
            || self.raft_min_election_timeout_ticks >= self.raft_max_election_timeout_ticks
        {
            return Err(box_err!(
                "invalid timeout range [{}, {}) for timeout {}",
                self.raft_min_election_timeout_ticks,
                self.raft_max_election_timeout_ticks,
                self.raft_election_timeout_ticks
            ));
        }

        // The adjustment of this value is related to the number of regions, usually
        // 16384 is already a large enough value
        if self.raft_max_inflight_msgs == 0 || self.raft_max_inflight_msgs > 16384 {
            return Err(box_err!(
                "raft max inflight msgs should be greater than 0 and less than or equal to 16384"
            ));
        }

        if self.raft_max_size_per_msg.0 == 0 || self.raft_max_size_per_msg.0 > ReadableSize::gb(3).0
        {
            return Err(box_err!(
                "raft max size per message should be greater than 0 and less than or equal to 3GiB"
            ));
        }

        if self.raft_entry_max_size.0 == 0 || self.raft_entry_max_size.0 > ReadableSize::gb(3).0 {
            return Err(box_err!(
                "raft entry max size should be greater than 0 and less than or equal to 3GiB"
            ));
        }

        if self.raft_log_gc_threshold < 1 {
            return Err(box_err!(
                "raft log gc threshold must >= 1, not {}",
                self.raft_log_gc_threshold
            ));
        }

        let election_timeout =
            self.raft_base_tick_interval.as_millis() * self.raft_election_timeout_ticks as u64;
        let lease = self.raft_store_max_leader_lease.as_millis();
        if election_timeout < lease {
            return Err(box_err!(
                "election timeout {} ms is less than lease {} ms",
                election_timeout,
                lease
            ));
        }

        let tick = self.raft_base_tick_interval.as_millis();
        if lease > election_timeout - tick {
            return Err(box_err!(
                "lease {} ms should not be greater than election timeout {} ms - 1 tick({} ms)",
                lease,
                election_timeout,
                tick
            ));
        }

        if self.merge_check_tick_interval.as_millis() == 0 {
            return Err(box_err!("raftstore.merge-check-tick-interval can't be 0."));
        }

        let stale_state_check = self.peer_stale_state_check_interval.as_millis();
        if stale_state_check < election_timeout * 2 {
            return Err(box_err!(
                "peer stale state check interval {} ms is less than election timeout x 2 {} ms",
                stale_state_check,
                election_timeout * 2
            ));
        }

        if self.leader_transfer_max_log_lag < 10 {
            return Err(box_err!(
                "raftstore.leader-transfer-max-log-lag should be >= 10."
            ));
        }

        let abnormal_leader_missing = self.abnormal_leader_missing_duration.as_millis();
        if abnormal_leader_missing < stale_state_check {
            return Err(box_err!(
                "abnormal leader missing {} ms is less than peer stale state check interval {} ms",
                abnormal_leader_missing,
                stale_state_check
            ));
        }

        let max_leader_missing = self.max_leader_missing_duration.as_millis();
        if max_leader_missing < abnormal_leader_missing {
            return Err(box_err!(
                "max leader missing {} ms is less than abnormal leader missing {} ms",
                max_leader_missing,
                abnormal_leader_missing
            ));
        }

        if self.region_compact_tombstones_percent < 1
            || self.region_compact_tombstones_percent > 100
        {
            return Err(box_err!(
                "region-compact-tombstones-percent must between 1 and 100, current value is {}",
                self.region_compact_tombstones_percent
            ));
        }

        let region_compact_redundant_rows_percent =
            self.region_compact_redundant_rows_percent.unwrap();
        if !(1..=100).contains(&region_compact_redundant_rows_percent) {
            return Err(box_err!(
                "region-compact-redundant-rows-percent must between 1 and 100, current value is {}",
                region_compact_redundant_rows_percent
            ));
        }

        if self.local_read_batch_size == 0 {
            return Err(box_err!("local-read-batch-size must be greater than 0"));
        }

        if self.raft_write_wait_duration.as_micros() > 1000 {
            return Err(box_err!(
                "raft-write-wait-duration should be less than 1ms, current value is {}ms",
                self.raft_write_wait_duration.as_millis()
            ));
        }

        // Since the following configuration supports online update, in order to
        // prevent mistakenly inputting too large values, the max limit is made
        // according to the cpu quota * 10. Notice 10 is only an estimate, not an
        // empirical value.
        let limit = (SysQuota::cpu_cores_quota() * 10.0) as usize;
        if self.apply_batch_system.pool_size == 0 || self.apply_batch_system.pool_size > limit {
            return Err(box_err!(
                "apply-pool-size should be greater than 0 and less than or equal to: {}",
                limit
            ));
        }
        if let Some(size) = self.apply_batch_system.max_batch_size {
            if size == 0 || size > 10240 {
                return Err(box_err!(
                    "apply-max-batch-size should be greater than 0 and less than or equal to 10240"
                ));
            }
        } else {
            self.apply_batch_system.max_batch_size = Some(256);
        }
        if self.store_batch_system.pool_size == 0 || self.store_batch_system.pool_size > limit {
            return Err(box_err!(
                "store-pool-size should be greater than 0 and less than or equal to: {}",
                limit
            ));
        }
        if self.store_batch_system.low_priority_pool_size > 0 {
            // The store thread pool doesn't need a low-priority thread currently.
            self.store_batch_system.low_priority_pool_size = 0;
        }
        if let Some(size) = self.store_batch_system.max_batch_size {
            if size == 0 || size > 10240 {
                return Err(box_err!(
                    "store-max-batch-size should be greater than 0 and less than or equal to 10240"
                ));
            }
        } else if self.hibernate_regions {
            self.store_batch_system.max_batch_size = Some(256);
        } else {
            self.store_batch_system.max_batch_size = Some(1024);
        }
        if self.store_io_notify_capacity == 0 {
            return Err(box_err!(
                "store-io-notify-capacity should be greater than 0"
            ));
        }
        if self.future_poll_size == 0 {
            return Err(box_err!("future-poll-size should be greater than 0"));
        }

        // Avoid hibernated peer being reported as down peer.
        if self.hibernate_regions {
            self.max_peer_down_duration = std::cmp::max(
                self.max_peer_down_duration,
                self.peer_stale_state_check_interval * 2,
            );
        }

        if self.evict_cache_on_memory_ratio < 0.0 {
            return Err(box_err!(
                "evict_cache_on_memory_ratio must be greater than 0"
            ));
        }

        if self.snap_generator_pool_size == 0 {
            return Err(box_err!(
                "snap-generator-pool-size should be greater than 0."
            ));
        }

        if self.check_leader_lease_interval.as_millis() == 0 {
            self.check_leader_lease_interval = self.raft_store_max_leader_lease / 4;
        }

        if self.renew_leader_lease_advance_duration.as_millis() == 0 && self.hibernate_regions {
            self.renew_leader_lease_advance_duration = self.raft_store_max_leader_lease / 4;
        }

        #[cfg(not(any(test, feature = "testexport")))]
        if self.max_snapshot_file_raw_size.0 != 0 && self.max_snapshot_file_raw_size.as_mb() < 100 {
            return Err(box_err!(
                "max_snapshot_file_raw_size should be no less than 100MB."
            ));
        }

        match self.raft_log_gc_size_limit {
            Some(size_limit) => {
                if size_limit.0 == 0 {
                    return Err(box_err!("raft log gc size limit should large than 0."));
                }
            }
            None => self.raft_log_gc_size_limit = Some(region_split_size * 3 / 4),
        }
        match self.raft_log_gc_count_limit {
            Some(count_limit) => {
                if self.merge_max_log_gap >= count_limit {
                    return Err(box_err!(
                        "merge log gap {} should be less than log gc limit {}.",
                        self.merge_max_log_gap,
                        count_limit
                    ));
                }
            }
            None => {
                // Assume the average size of entries is 1k.
                self.raft_log_gc_count_limit =
                    Some(region_split_size * 3 / 4 / ReadableSize::kb(1));
            }
        }
        match self.region_split_check_diff {
            Some(split_check_diff) => {
                if split_check_diff.0 == 0 {
                    return Err(box_err!("region split check diff should large than 0."));
                }
            }
            None => {
                self.region_split_check_diff = if !enable_region_bucket {
                    Some(region_split_size / 16)
                } else {
                    Some(ReadableSize(min(
                        region_split_size.0 / 16,
                        region_bucket_size.0,
                    )))
                }
            }
        }
        assert!(self.region_compact_check_step.is_some());
        if raft_kv_v2 && self.use_delete_range {
            return Err(box_err!(
                "partitioned-raft-kv doesn't support RocksDB delete range."
            ));
        }

        if self.slow_trend_network_io_factor < 0.0 {
            return Err(box_err!(
                "slow_trend_network_io_factor must be greater than 0"
            ));
        }

        if self.min_pending_apply_region_count == 0 {
            return Err(box_err!(
                "min_pending_apply_region_count must be greater than 0"
            ));
        }

        if self.snap_min_approximate_size > region_split_size {
            self.snap_min_approximate_size = region_split_size;
            warn!("snap_min_approximate_size is too large, set to region_split_size");
        }

        Ok(())
    }

    pub fn write_into_metrics(&self) {
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["prevote"])
            .set((self.prevote as i32).into());

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["capacity"])
            .set(self.capacity.0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_base_tick_interval"])
            .set(self.raft_base_tick_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_heartbeat_ticks"])
            .set(self.raft_heartbeat_ticks as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_election_timeout_ticks"])
            .set(self.raft_election_timeout_ticks as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_min_election_timeout_ticks"])
            .set(self.raft_min_election_timeout_ticks as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_max_election_timeout_ticks"])
            .set(self.raft_max_election_timeout_ticks as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_max_size_per_msg"])
            .set(self.raft_max_size_per_msg.0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_max_inflight_msgs"])
            .set(self.raft_max_inflight_msgs as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_entry_max_size"])
            .set(self.raft_entry_max_size.0 as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_compact_sync_interval"])
            .set(self.raft_log_compact_sync_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_gc_tick_interval"])
            .set(self.raft_log_gc_tick_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["request_voter_replicated_index_interval"])
            .set(self.request_voter_replicated_index_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_gc_threshold"])
            .set(self.raft_log_gc_threshold as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_gc_count_limit"])
            .set(self.raft_log_gc_count_limit.unwrap_or_default() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_gc_size_limit"])
            .set(self.raft_log_gc_size_limit.unwrap_or_default().0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_reserve_max_ticks"])
            .set(self.raft_log_reserve_max_ticks as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_engine_purge_interval"])
            .set(self.raft_engine_purge_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["max_manual_flush_rate"])
            .set(self.max_manual_flush_rate);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_entry_cache_life_time"])
            .set(self.raft_entry_cache_life_time.as_secs_f64());

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["split_region_check_tick_interval"])
            .set(self.split_region_check_tick_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_split_check_diff"])
            .set(self.region_split_check_diff.unwrap_or_default().0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_check_interval"])
            .set(self.region_compact_check_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_check_step"])
            .set(self.region_compact_check_step.unwrap_or_default() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_min_tombstones"])
            .set(self.region_compact_min_tombstones as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_tombstones_percent"])
            .set(self.region_compact_tombstones_percent as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_min_redundant_rows"])
            .set(self.region_compact_min_redundant_rows as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_redundant_rows_percent"])
            .set(
                self.region_compact_redundant_rows_percent
                    .unwrap_or_default() as f64,
            );
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["pd_heartbeat_tick_interval"])
            .set(self.pd_heartbeat_tick_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["pd_store_heartbeat_tick_interval"])
            .set(self.pd_store_heartbeat_tick_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["pd_report_min_resolved_ts_interval"])
            .set(self.pd_report_min_resolved_ts_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["snap_mgr_gc_tick_interval"])
            .set(self.snap_mgr_gc_tick_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["snap_gc_timeout"])
            .set(self.snap_gc_timeout.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["lock_cf_compact_interval"])
            .set(self.lock_cf_compact_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["lock_cf_compact_bytes_threshold"])
            .set(self.lock_cf_compact_bytes_threshold.0 as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["notify_capacity"])
            .set(self.notify_capacity as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["messages_per_tick"])
            .set(self.messages_per_tick as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["max_peer_down_duration"])
            .set(self.max_peer_down_duration.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["max_leader_missing_duration"])
            .set(self.max_leader_missing_duration.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["abnormal_leader_missing_duration"])
            .set(self.abnormal_leader_missing_duration.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["peer_stale_state_check_interval"])
            .set(self.peer_stale_state_check_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["leader_transfer_max_log_lag"])
            .set(self.leader_transfer_max_log_lag as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["gc_peer_check_interval"])
            .set(self.gc_peer_check_interval.as_secs_f64());

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["snap_apply_batch_size"])
            .set(self.snap_apply_batch_size.0 as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["consistency_check_interval_seconds"])
            .set(self.consistency_check_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["report_region_flow_interval"])
            .set(self.report_region_flow_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_store_max_leader_lease"])
            .set(self.raft_store_max_leader_lease.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["right_derive_when_split"])
            .set((self.right_derive_when_split as i32).into());

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["merge_max_log_gap"])
            .set(self.merge_max_log_gap as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["merge_check_tick_interval"])
            .set(self.merge_check_tick_interval.as_secs_f64());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["use_delete_range"])
            .set((self.use_delete_range as i32).into());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["cleanup_import_sst_interval"])
            .set(self.cleanup_import_sst_interval.as_secs_f64());

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["local_read_batch_size"])
            .set(self.local_read_batch_size as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["apply_yield_write_size"])
            .set(self.apply_yield_write_size.0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["apply_max_batch_size"])
            .set(self.apply_batch_system.max_batch_size() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["apply_pool_size"])
            .set(self.apply_batch_system.pool_size as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["store_max_batch_size"])
            .set(self.store_batch_system.max_batch_size() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["store_pool_size"])
            .set(self.store_batch_system.pool_size as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["store_io_pool_size"])
            .set(self.store_io_pool_size as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["store_io_notify_capacity"])
            .set(self.store_io_notify_capacity as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["future_poll_size"])
            .set(self.future_poll_size as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["snap_generator_pool_size"])
            .set(self.snap_generator_pool_size as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["hibernate_regions"])
            .set((self.hibernate_regions as i32).into());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["cmd_batch"])
            .set((self.cmd_batch as i32).into());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["cmd_batch_concurrent_ready_max_count"])
            .set(self.cmd_batch_concurrent_ready_max_count as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_write_size_limit"])
            .set(self.raft_write_size_limit.0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_write_batch_size_hint"])
            .set(self.raft_write_batch_size_hint.0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_write_wait_duration"])
            .set(self.raft_write_wait_duration.as_micros() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["waterfall_metrics"])
            .set((self.waterfall_metrics as i32).into());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["io_reschedule_concurrent_max_count"])
            .set(self.io_reschedule_concurrent_max_count as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["io_reschedule_hotpot_duration"])
            .set(self.io_reschedule_hotpot_duration.as_secs_f64());
    }

    fn write_change_into_metrics(change: ConfigChange) {
        for (name, value) in change {
            if let Ok(v) = match value {
                ConfigValue::F64(v) => Ok(v),
                ConfigValue::U64(v) => Ok(v as f64),
                ConfigValue::Size(v) => Ok(v as f64),
                ConfigValue::Usize(v) => Ok(v as f64),
                ConfigValue::Bool(v) => Ok((v as i32).into()),
                ConfigValue::Duration(v) => Ok((v / 1000) as f64), // millis -> secs
                _ => Err(()),
            } {
                CONFIG_RAFTSTORE_GAUGE
                    .with_label_values(&[name.as_str()])
                    .set(v);
            }
        }
    }
}

pub struct RaftstoreConfigManager {
    scheduler: Scheduler<RefreshConfigTask>,
    config: Arc<VersionTrack<Config>>,
}

impl RaftstoreConfigManager {
    pub fn new(
        scheduler: Scheduler<RefreshConfigTask>,
        config: Arc<VersionTrack<Config>>,
    ) -> RaftstoreConfigManager {
        RaftstoreConfigManager { scheduler, config }
    }

    fn schedule_config_change(
        &self,
        pool: RaftStoreBatchComponent,
        cfg_change: &HashMap<String, ConfigValue>,
    ) {
        if let Some(pool_size) = cfg_change.get("pool_size") {
            let scale_pool = RefreshConfigTask::ScalePool(pool, pool_size.into());
            if let Err(e) = self.scheduler.schedule(scale_pool) {
                error!("raftstore configuration manager schedule scale {} pool_size work task failed", pool; "err"=> ?e);
            }
        }
        if let Some(size) = cfg_change.get("max_batch_size") {
            let scale_batch = RefreshConfigTask::ScaleBatchSize(pool, size.into());
            if let Err(e) = self.scheduler.schedule(scale_batch) {
                error!("raftstore configuration manager schedule scale {} max_batch_size work task failed", pool; "err"=> ?e);
            }
        }
    }
}

impl ConfigManager for RaftstoreConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        {
            let change = change.clone();
            self.config.update(move |cfg: &mut Config| {
                // Currently, it's forbidden to modify the write mode either from `async` to
                // `sync` or from `sync` to `async`.
                if let Some(ConfigValue::Usize(resized_io_size)) = change.get("store_io_pool_size")
                {
                    if cfg.store_io_pool_size == 0 && *resized_io_size > 0 {
                        return Err(
                            "SYNC mode, not allowed to resize the size of store-io-pool-size"
                                .into(),
                        );
                    } else if cfg.store_io_pool_size > 0 && *resized_io_size == 0 {
                        return Err(
                            "ASYNC mode, not allowed to be set to SYNC mode by resizing store-io-pool-size to 0"
                                .into(),
                        );
                    }
                }
                cfg.update(change)
            })?;
        }
        if let Some(ConfigValue::Module(raft_batch_system_change)) =
            change.get("store_batch_system")
        {
            self.schedule_config_change(RaftStoreBatchComponent::Store, raft_batch_system_change);
        }
        if let Some(ConfigValue::Module(apply_batch_system_change)) =
            change.get("apply_batch_system")
        {
            self.schedule_config_change(RaftStoreBatchComponent::Apply, apply_batch_system_change);
        }
        if let Some(ConfigValue::Usize(resized_io_size)) = change.get("store_io_pool_size") {
            let resize_io_task = RefreshConfigTask::ScaleWriters(*resized_io_size);
            if let Err(e) = self.scheduler.schedule(resize_io_task) {
                error!("raftstore configuration manager schedule to resize store-io-pool-size work task failed"; "err"=> ?e);
            }
        }
        if let Some(ConfigValue::Usize(resize_reader_size)) = change.get("snap_generator_pool_size")
        {
            let resize_reader_task = RefreshConfigTask::ScaleAsyncReader(*resize_reader_size);
            if let Err(e) = self.scheduler.schedule(resize_reader_task) {
                error!("raftstore configuration manager schedule to resize snap-generator-pool-size work task failed"; "err"=> ?e);
            }
        }
        info!(
            "raftstore config changed";
            "change" => ?change,
        );
        Config::write_change_into_metrics(change);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor;

    #[test]
    fn test_config_validate() {
        let split_size = coprocessor::config::SPLIT_SIZE;
        let mut cfg = Config::new();
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(
            cfg.raft_min_election_timeout_ticks,
            cfg.raft_election_timeout_ticks
        );
        assert_eq!(
            cfg.raft_max_election_timeout_ticks,
            cfg.raft_election_timeout_ticks * 2
        );

        cfg.raft_heartbeat_ticks = 0;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_heartbeat_ticks = 10;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_min_election_timeout_ticks = 5;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();
        cfg.raft_min_election_timeout_ticks = 25;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();
        cfg.raft_min_election_timeout_ticks = 10;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();

        cfg.raft_heartbeat_ticks = 11;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_log_gc_threshold = 0;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_log_gc_size_limit = Some(ReadableSize(0));
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_log_gc_size_limit = None;
        cfg.optimize_for(false);
        cfg.validate(ReadableSize(20), false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(cfg.raft_log_gc_size_limit, Some(ReadableSize(15)));

        cfg = Config::new();
        cfg.raft_base_tick_interval = ReadableDuration::secs(1);
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_store_max_leader_lease = ReadableDuration::secs(20);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_log_gc_count_limit = Some(100);
        cfg.merge_max_log_gap = 110;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_log_gc_count_limit = None;
        cfg.optimize_for(false);
        cfg.validate(ReadableSize::mb(1), false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(cfg.raft_log_gc_count_limit, Some(768));

        cfg = Config::new();
        cfg.merge_check_tick_interval = ReadableDuration::secs(0);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_base_tick_interval = ReadableDuration::secs(1);
        cfg.raft_election_timeout_ticks = 10;
        cfg.peer_stale_state_check_interval = ReadableDuration::secs(5);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.peer_stale_state_check_interval = ReadableDuration::minutes(2);
        cfg.abnormal_leader_missing_duration = ReadableDuration::minutes(1);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.abnormal_leader_missing_duration = ReadableDuration::minutes(2);
        cfg.max_leader_missing_duration = ReadableDuration::minutes(1);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.local_read_batch_size = 0;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.apply_batch_system.max_batch_size = Some(0);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.apply_batch_system.pool_size = 0;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.store_batch_system.max_batch_size = Some(0);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.store_batch_system.pool_size = 0;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.apply_batch_system.max_batch_size = Some(10241);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.store_batch_system.max_batch_size = Some(10241);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.hibernate_regions = true;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(cfg.store_batch_system.max_batch_size, Some(256));
        assert_eq!(cfg.apply_batch_system.max_batch_size, Some(256));

        cfg = Config::new();
        cfg.hibernate_regions = false;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(cfg.store_batch_system.max_batch_size, Some(1024));
        assert_eq!(cfg.apply_batch_system.max_batch_size, Some(256));

        cfg = Config::new();
        cfg.hibernate_regions = true;
        cfg.store_batch_system.max_batch_size = Some(123);
        cfg.apply_batch_system.max_batch_size = Some(234);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(cfg.store_batch_system.max_batch_size, Some(123));
        assert_eq!(cfg.apply_batch_system.max_batch_size, Some(234));

        cfg = Config::new();
        cfg.future_poll_size = 0;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.snap_generator_pool_size = 0;
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.raft_base_tick_interval = ReadableDuration::secs(1);
        cfg.raft_election_timeout_ticks = 11;
        cfg.raft_store_max_leader_lease = ReadableDuration::secs(11);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();

        cfg = Config::new();
        cfg.hibernate_regions = true;
        cfg.max_peer_down_duration = ReadableDuration::minutes(5);
        cfg.peer_stale_state_check_interval = ReadableDuration::minutes(5);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(cfg.max_peer_down_duration, ReadableDuration::minutes(10));

        cfg = Config::new();
        cfg.raft_max_size_per_msg = ReadableSize(0);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();
        cfg.raft_max_size_per_msg = ReadableSize::gb(64);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();
        cfg.raft_max_size_per_msg = ReadableSize::gb(3);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();

        cfg = Config::new();
        cfg.raft_entry_max_size = ReadableSize(0);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();
        cfg.raft_entry_max_size = ReadableSize::mb(3073);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap_err();
        cfg.raft_entry_max_size = ReadableSize::gb(3);
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();

        cfg = Config::new();
        cfg.optimize_for(false);
        cfg.validate(split_size, false, ReadableSize(0), false)
            .unwrap();
        assert_eq!(cfg.region_split_check_diff(), split_size / 16);

        cfg = Config::new();
        cfg.optimize_for(false);
        cfg.validate(split_size, true, split_size / 8, false)
            .unwrap();
        assert_eq!(cfg.region_split_check_diff(), split_size / 16);

        cfg = Config::new();
        cfg.optimize_for(false);
        cfg.validate(split_size, true, split_size / 20, false)
            .unwrap();
        assert_eq!(cfg.region_split_check_diff(), split_size / 20);

        cfg = Config::new();
        cfg.region_split_check_diff = Some(ReadableSize(1));
        cfg.optimize_for(false);
        cfg.validate(split_size, true, split_size / 20, false)
            .unwrap();
        assert_eq!(cfg.region_split_check_diff(), ReadableSize(1));

        cfg = Config::new();
        cfg.optimize_for(true);
        cfg.validate(split_size, true, split_size / 20, false)
            .unwrap();
        assert_eq!(cfg.raft_log_gc_size_limit(), ReadableSize::mb(200));
        assert_eq!(cfg.raft_log_gc_count_limit(), 10000);

        cfg = Config::new();
        cfg.optimize_for(false);
        cfg.validate(split_size, true, split_size / 20, false)
            .unwrap();
        assert_eq!(cfg.raft_log_gc_size_limit(), split_size * 3 / 4);
        assert_eq!(
            cfg.raft_log_gc_count_limit(),
            split_size * 3 / 4 / ReadableSize::kb(1)
        );

        cfg = Config::new();
        cfg.optimize_for(false);
        cfg.raft_write_wait_duration = ReadableDuration::micros(1001);
        cfg.validate(split_size, true, split_size / 20, false)
            .unwrap_err();
    }
}
