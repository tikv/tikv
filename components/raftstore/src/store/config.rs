// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::u64;
use time::Duration as TimeDuration;

use crate::{coprocessor, Result};
use batch_system::{
    BatchRouter, Config as BatchSystemConfig, Fsm, FsmTypes, HandlerBuilder, Poller, PoolState,
    Priority, SCALE_FINISHED_CHANNEL,
};
use engine_traits::config as engine_config;
use engine_traits::PerfLevel;
use file_system::{set_io_type, IOType};
use lazy_static::lazy_static;
use online_config::{ConfigChange, ConfigManager, ConfigValue, OnlineConfig};
use prometheus::register_gauge_vec;
use serde::{Deserialize, Serialize};
use serde_with::with_prefix;
use tikv_util::config::{ReadableDuration, ReadableSize, VersionTrack};
use tikv_util::worker::Scheduler;
use tikv_util::{box_err, debug, error, info, safe_panic, thd_name, warn};

lazy_static! {
    pub static ref CONFIG_RAFTSTORE_GAUGE: prometheus::GaugeVec = register_gauge_vec!(
        "tikv_config_raftstore",
        "Config information of raftstore",
        &["name"]
    )
    .unwrap();
}

with_prefix!(prefix_apply "apply-");
with_prefix!(prefix_store "store-");
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    // minimizes disruption when a partitioned node rejoins the cluster by using a two phase election.
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
    #[online_config(hidden)]
    pub raft_max_size_per_msg: ReadableSize,
    #[online_config(hidden)]
    pub raft_max_inflight_msgs: usize,
    // When the entry exceed the max size, reject to propose it.
    pub raft_entry_max_size: ReadableSize,

    // Interval to gc unnecessary raft log (ms).
    pub raft_log_gc_tick_interval: ReadableDuration,
    // A threshold to gc stale raft log, must >= 1.
    pub raft_log_gc_threshold: u64,
    // When entry count exceed this value, gc will be forced trigger.
    pub raft_log_gc_count_limit: u64,
    // When the approximate size of raft log entries exceed this value,
    // gc will be forced trigger.
    pub raft_log_gc_size_limit: ReadableSize,
    // Old Raft logs could be reserved if `raft_log_gc_threshold` is not reached.
    // GC them after ticks `raft_log_reserve_max_ticks` times.
    #[doc(hidden)]
    #[online_config(hidden)]
    pub raft_log_reserve_max_ticks: usize,
    // Old logs in Raft engine needs to be purged peridically.
    pub raft_engine_purge_interval: ReadableDuration,
    // When a peer is not responding for this time, leader will not keep entry cache for it.
    pub raft_entry_cache_life_time: ReadableDuration,
    // When a peer is newly added, reject transferring leader to the peer for a while.
    pub raft_reject_transfer_leader_duration: ReadableDuration,

    // Interval (ms) to check region whether need to be split or not.
    pub split_region_check_tick_interval: ReadableDuration,
    /// When size change of region exceed the diff since last check, it
    /// will be checked again whether it should be split.
    pub region_split_check_diff: ReadableSize,
    /// Interval (ms) to check whether start compaction for a region.
    pub region_compact_check_interval: ReadableDuration,
    /// Number of regions for each time checking.
    pub region_compact_check_step: u64,
    /// Minimum number of tombstones to trigger manual compaction.
    pub region_compact_min_tombstones: u64,
    /// Minimum percentage of tombstones to trigger manual compaction.
    /// Should between 1 and 100.
    pub region_compact_tombstones_percent: u64,
    pub pd_heartbeat_tick_interval: ReadableDuration,
    pub pd_store_heartbeat_tick_interval: ReadableDuration,
    pub snap_mgr_gc_tick_interval: ReadableDuration,
    pub snap_gc_timeout: ReadableDuration,
    pub lock_cf_compact_interval: ReadableDuration,
    pub lock_cf_compact_bytes_threshold: ReadableSize,

    #[online_config(skip)]
    pub notify_capacity: usize,
    pub messages_per_tick: usize,

    /// When a peer is not active for max_peer_down_duration,
    /// the peer is considered to be down and is reported to PD.
    pub max_peer_down_duration: ReadableDuration,

    /// If the leader of a peer is missing for longer than max_leader_missing_duration,
    /// the peer would ask pd to confirm whether it is valid in any region.
    /// If the peer is stale and is not valid in any region, it will destroy itself.
    pub max_leader_missing_duration: ReadableDuration,
    /// Similar to the max_leader_missing_duration, instead it will log warnings and
    /// try to alert monitoring systems, if there is any.
    pub abnormal_leader_missing_duration: ReadableDuration,
    pub peer_stale_state_check_interval: ReadableDuration,

    #[online_config(hidden)]
    pub leader_transfer_max_log_lag: u64,

    #[online_config(skip)]
    pub snap_apply_batch_size: ReadableSize,

    // Interval (ms) to check region whether the data is consistent.
    pub consistency_check_interval: ReadableDuration,

    #[online_config(hidden)]
    pub report_region_flow_interval: ReadableDuration,

    // The lease provided by a successfully proposed and applied entry.
    pub raft_store_max_leader_lease: ReadableDuration,

    // Right region derive origin region id when split.
    #[online_config(hidden)]
    pub right_derive_when_split: bool,

    pub allow_remove_leader: bool,

    /// Max log gap allowed to propose merge.
    #[online_config(hidden)]
    pub merge_max_log_gap: u64,
    /// Interval to re-propose merge.
    pub merge_check_tick_interval: ReadableDuration,

    #[online_config(hidden)]
    pub use_delete_range: bool,

    pub cleanup_import_sst_interval: ReadableDuration,

    /// Maximum size of every local read task batch.
    pub local_read_batch_size: u64,

    #[online_config(submodule)]
    #[serde(flatten, with = "prefix_apply")]
    pub apply_batch_system: BatchSystemConfig,

    #[online_config(submodule)]
    #[serde(flatten, with = "prefix_store")]
    pub store_batch_system: BatchSystemConfig,

    #[online_config(skip)]
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

    #[serde(with = "engine_config::perf_level_serde")]
    #[online_config(skip)]
    pub perf_level: PerfLevel,

    #[doc(hidden)]
    #[online_config(skip)]
    /// When TiKV memory usage reaches `memory_usage_high_water` it will try to limit memory
    /// increasing. For raftstore layer entries will be evicted from entry cache, if they
    /// utilize memory more than `evict_cache_on_memory_ratio` * total.
    ///
    /// Set it to 0 can disable cache evict.
    // By default it's 0.2. So for different system memory capacity, cache evict happens:
    // * system=8G,  memory_usage_limit=6G,  evict=1.2G
    // * system=16G, memory_usage_limit=12G, evict=2.4G
    // * system=32G, memory_usage_limit=24G, evict=4.8G
    pub evict_cache_on_memory_ratio: f64,

    pub cmd_batch: bool,

    /// When the count of concurrent ready exceeds this value, command will not be proposed
    /// until the previous ready has been persisted.
    /// If `cmd_batch` is 0, this config will have no effect.
    /// If it is 0, it means no limit.
    pub cmd_batch_concurrent_ready_max_count: usize,

    /// When the size of raft db writebatch exceeds this value, write will be triggered.
    pub raft_write_size_limit: ReadableSize,

    pub waterfall_metrics: bool,

    pub io_reschedule_concurrent_max_count: usize,
    pub io_reschedule_hotpot_duration: ReadableDuration,

    // Deprecated! These configuration has been moved to Coprocessor.
    // They are preserved for compatibility check.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub region_max_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub region_split_size: ReadableSize,
    // Deprecated! The time to clean stale peer safely can be decided based on RocksDB snapshot sequence number.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub clean_stale_peer_delay: ReadableDuration,

    // Interval to inspect the latency of raftstore for slow store detection.
    pub inspect_interval: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(coprocessor::config::SPLIT_SIZE_MB);
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
            raft_log_gc_tick_interval: ReadableDuration::secs(10),
            raft_log_gc_threshold: 50,
            // Assume the average size of entries is 1k.
            raft_log_gc_count_limit: split_size * 3 / 4 / ReadableSize::kb(1),
            raft_log_gc_size_limit: split_size * 3 / 4,
            raft_log_reserve_max_ticks: 6,
            raft_engine_purge_interval: ReadableDuration::secs(10),
            raft_entry_cache_life_time: ReadableDuration::secs(30),
            raft_reject_transfer_leader_duration: ReadableDuration::secs(3),
            split_region_check_tick_interval: ReadableDuration::secs(10),
            region_split_check_diff: split_size / 16,
            region_compact_check_interval: ReadableDuration::minutes(5),
            region_compact_check_step: 100,
            region_compact_min_tombstones: 10000,
            region_compact_tombstones_percent: 30,
            pd_heartbeat_tick_interval: ReadableDuration::minutes(1),
            pd_store_heartbeat_tick_interval: ReadableDuration::secs(10),
            notify_capacity: 40960,
            snap_mgr_gc_tick_interval: ReadableDuration::minutes(1),
            snap_gc_timeout: ReadableDuration::hours(4),
            messages_per_tick: 4096,
            max_peer_down_duration: ReadableDuration::minutes(10),
            max_leader_missing_duration: ReadableDuration::hours(2),
            abnormal_leader_missing_duration: ReadableDuration::minutes(10),
            peer_stale_state_check_interval: ReadableDuration::minutes(5),
            leader_transfer_max_log_lag: 128,
            snap_apply_batch_size: ReadableSize::mb(10),
            lock_cf_compact_interval: ReadableDuration::minutes(10),
            lock_cf_compact_bytes_threshold: ReadableSize::mb(256),
            // Disable consistency check by default as it will hurt performance.
            // We should turn on this only in our tests.
            consistency_check_interval: ReadableDuration::secs(0),
            report_region_flow_interval: ReadableDuration::minutes(1),
            raft_store_max_leader_lease: ReadableDuration::secs(9),
            right_derive_when_split: true,
            allow_remove_leader: false,
            merge_max_log_gap: 10,
            merge_check_tick_interval: ReadableDuration::secs(2),
            use_delete_range: false,
            cleanup_import_sst_interval: ReadableDuration::minutes(10),
            local_read_batch_size: 1024,
            apply_batch_system: BatchSystemConfig::default(),
            store_batch_system: BatchSystemConfig::default(),
            store_io_pool_size: 2,
            store_io_notify_capacity: 40960,
            future_poll_size: 1,
            hibernate_regions: true,
            dev_assert: false,
            apply_yield_duration: ReadableDuration::millis(500),
            perf_level: PerfLevel::EnableTime,
            evict_cache_on_memory_ratio: 0.2,
            cmd_batch: true,
            cmd_batch_concurrent_ready_max_count: 1,
            raft_write_size_limit: ReadableSize::mb(1),
            waterfall_metrics: false,
            io_reschedule_concurrent_max_count: 4,
            io_reschedule_hotpot_duration: ReadableDuration::secs(5),

            // They are preserved for compatibility check.
            region_max_size: ReadableSize(0),
            region_split_size: ReadableSize(0),
            clean_stale_peer_delay: ReadableDuration::minutes(0),
            inspect_interval: ReadableDuration::millis(500),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn raft_store_max_leader_lease(&self) -> TimeDuration {
        TimeDuration::from_std(self.raft_store_max_leader_lease.0).unwrap()
    }

    pub fn raft_heartbeat_interval(&self) -> Duration {
        self.raft_base_tick_interval.0 * self.raft_heartbeat_ticks as u32
    }

    pub fn validate(&mut self) -> Result<()> {
        if self.raft_heartbeat_ticks == 0 {
            return Err(box_err!("heartbeat tick must greater than 0"));
        }

        if self.raft_election_timeout_ticks != 10 {
            warn!(
                "Election timeout ticks needs to be same across all the cluster, \
                 otherwise it may lead to inconsistency."
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

        if self.raft_log_gc_threshold < 1 {
            return Err(box_err!(
                "raft log gc threshold must >= 1, not {}",
                self.raft_log_gc_threshold
            ));
        }

        if self.raft_log_gc_size_limit.0 == 0 {
            return Err(box_err!("raft log gc size limit should large than 0."));
        }

        let election_timeout =
            self.raft_base_tick_interval.as_millis() * self.raft_election_timeout_ticks as u64;
        let lease = self.raft_store_max_leader_lease.as_millis() as u64;
        if election_timeout < lease {
            return Err(box_err!(
                "election timeout {} ms is less than lease {} ms",
                election_timeout,
                lease
            ));
        }

        let tick = self.raft_base_tick_interval.as_millis() as u64;
        if lease > election_timeout - tick {
            return Err(box_err!(
                "lease {} ms should not be greater than election timeout {} ms - 1 tick({} ms)",
                lease,
                election_timeout,
                tick
            ));
        }

        if self.merge_max_log_gap >= self.raft_log_gc_count_limit {
            return Err(box_err!(
                "merge log gap {} should be less than log gc limit {}.",
                self.merge_max_log_gap,
                self.raft_log_gc_count_limit
            ));
        }

        if self.merge_check_tick_interval.as_millis() == 0 {
            return Err(box_err!("raftstore.merge-check-tick-interval can't be 0."));
        }

        let stale_state_check = self.peer_stale_state_check_interval.as_millis() as u64;
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

        let abnormal_leader_missing = self.abnormal_leader_missing_duration.as_millis() as u64;
        if abnormal_leader_missing < stale_state_check {
            return Err(box_err!(
                "abnormal leader missing {} ms is less than peer stale state check interval {} ms",
                abnormal_leader_missing,
                stale_state_check
            ));
        }

        let max_leader_missing = self.max_leader_missing_duration.as_millis() as u64;
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

        if self.local_read_batch_size == 0 {
            return Err(box_err!("local-read-batch-size must be greater than 0"));
        }

        if self.apply_batch_system.pool_size == 0 {
            return Err(box_err!("apply-pool-size should be greater than 0"));
        }
        if let Some(size) = self.apply_batch_system.max_batch_size {
            if size == 0 {
                return Err(box_err!("apply-max-batch-size should be greater than 0"));
            }
        } else {
            self.apply_batch_system.max_batch_size = Some(256);
        }
        if self.store_batch_system.pool_size == 0 {
            return Err(box_err!("store-pool-size should be greater than 0"));
        }
        if self.store_batch_system.low_priority_pool_size > 0 {
            // The store thread pool doesn't need a low-priority thread currently.
            self.store_batch_system.low_priority_pool_size = 0;
        }
        if let Some(size) = self.store_batch_system.max_batch_size {
            if size == 0 {
                return Err(box_err!("store-max-batch-size should be greater than 0"));
            }
        } else if self.hibernate_regions {
            self.store_batch_system.max_batch_size = Some(256);
        } else {
            self.store_batch_system.max_batch_size = Some(1024);
        }
        if self.store_io_pool_size == 0 {
            return Err(box_err!("store-io-pool-size should be greater than 0"));
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
            .set(self.raft_base_tick_interval.as_secs() as f64);
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
            .with_label_values(&["raft_log_gc_tick_interval"])
            .set(self.raft_log_gc_tick_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_gc_threshold"])
            .set(self.raft_log_gc_threshold as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_gc_count_limit"])
            .set(self.raft_log_gc_count_limit as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_gc_size_limit"])
            .set(self.raft_log_gc_size_limit.0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_log_reserve_max_ticks"])
            .set(self.raft_log_reserve_max_ticks as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_engine_purge_interval"])
            .set(self.raft_engine_purge_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_entry_cache_life_time"])
            .set(self.raft_entry_cache_life_time.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_reject_transfer_leader_duration"])
            .set(self.raft_reject_transfer_leader_duration.as_secs() as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["split_region_check_tick_interval"])
            .set(self.split_region_check_tick_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_split_check_diff"])
            .set(self.region_split_check_diff.0 as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_check_interval"])
            .set(self.region_compact_check_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_check_step"])
            .set(self.region_compact_check_step as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_min_tombstones"])
            .set(self.region_compact_min_tombstones as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["region_compact_tombstones_percent"])
            .set(self.region_compact_tombstones_percent as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["pd_heartbeat_tick_interval"])
            .set(self.pd_heartbeat_tick_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["pd_store_heartbeat_tick_interval"])
            .set(self.pd_store_heartbeat_tick_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["snap_mgr_gc_tick_interval"])
            .set(self.snap_mgr_gc_tick_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["snap_gc_timeout"])
            .set(self.snap_gc_timeout.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["lock_cf_compact_interval"])
            .set(self.lock_cf_compact_interval.as_secs() as f64);
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
            .set(self.max_peer_down_duration.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["max_leader_missing_duration"])
            .set(self.max_leader_missing_duration.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["abnormal_leader_missing_duration"])
            .set(self.abnormal_leader_missing_duration.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["peer_stale_state_check_interval"])
            .set(self.peer_stale_state_check_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["leader_transfer_max_log_lag"])
            .set(self.leader_transfer_max_log_lag as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["snap_apply_batch_size"])
            .set(self.snap_apply_batch_size.0 as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["consistency_check_interval_seconds"])
            .set(self.consistency_check_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["report_region_flow_interval"])
            .set(self.report_region_flow_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["raft_store_max_leader_lease"])
            .set(self.raft_store_max_leader_lease.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["right_derive_when_split"])
            .set((self.right_derive_when_split as i32).into());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["allow_remove_leader"])
            .set((self.allow_remove_leader as i32).into());

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["merge_max_log_gap"])
            .set(self.merge_max_log_gap as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["merge_check_tick_interval"])
            .set(self.merge_check_tick_interval.as_secs() as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["use_delete_range"])
            .set((self.use_delete_range as i32).into());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["cleanup_import_sst_interval"])
            .set(self.cleanup_import_sst_interval.as_secs() as f64);

        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["local_read_batch_size"])
            .set(self.local_read_batch_size as f64);
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
            .with_label_values(&["waterfall_metrics"])
            .set((self.waterfall_metrics as i32).into());
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["io_reschedule_concurrent_max_count"])
            .set(self.io_reschedule_concurrent_max_count as f64);
        CONFIG_RAFTSTORE_GAUGE
            .with_label_values(&["io_reschedule_hotpot_duration"])
            .set(self.io_reschedule_hotpot_duration.as_secs() as f64);
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

pub struct PoolController<N: Fsm, C: Fsm, H: HandlerBuilder<N, C>> {
    pub router: BatchRouter<N, C>,
    pub state: PoolState<N, C, H>,
}

impl<N, C, H> PoolController<N, C, H>
where
    N: Fsm,
    C: Fsm,
    H: HandlerBuilder<N, C>,
{
    pub fn new(router: BatchRouter<N, C>, state: PoolState<N, C, H>) -> Self {
        PoolController { router, state }
    }
}

impl<N, C, H> PoolController<N, C, H>
where
    N: Fsm + std::marker::Send + 'static,
    C: Fsm + std::marker::Send + 'static,
    H: HandlerBuilder<N, C>,
{
    pub fn resize_to(&mut self, size: usize) -> (bool, usize) {
        let current_pool_size = self.state.pool_size.load(Ordering::Relaxed);
        if current_pool_size > size {
            return (true, current_pool_size - size);
        }
        (false, size - current_pool_size)
    }

    pub fn decrease_by(&mut self, size: usize) {
        let orignal_pool_size = self.state.pool_size.load(Ordering::Relaxed);
        let s = self
            .state
            .expected_pool_size
            .fetch_sub(size, Ordering::Relaxed);
        for _ in 0..size {
            if let Err(e) = self.state.fsm_sender.send(FsmTypes::Empty) {
                error!(
                    "failed to decrese thread pool";
                    "decrease to" => size,
                    "err" => %e,
                );
                return;
            }
        }
        info!(
            "decrease thread pool";
            "from" => orignal_pool_size,
            "decrease to" => s - 1,
            "pool" => ?self.state.name_prefix
        );
    }

    pub fn increase_by(&mut self, size: usize) {
        let name_prefix = self.state.name_prefix.clone();
        let mut workers = self.state.workers.lock().unwrap();
        self.state.id_base += size;
        for i in 0..size {
            let handler = self.state.handler_builder.build(Priority::Normal);
            let pool_size = Arc::clone(&self.state.pool_size);
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.state.fsm_receiver.clone(),
                handler,
                max_batch_size: self.state.max_batch_size,
                reschedule_duration: self.state.reschedule_duration,
                pool_size: Some(pool_size),
                expected_pool_size: Some(Arc::clone(&self.state.expected_pool_size)),
                pool_scale_finished: Some(SCALE_FINISHED_CHANNEL.0.clone()),
                joinable_workers: Some(Arc::clone(&self.state.joinable_workers)),
            };
            let props = tikv_util::thread_group::current_properties();
            let t = thread::Builder::new()
                .name(thd_name!(format!(
                    "{}-{}",
                    name_prefix,
                    i + self.state.id_base,
                )))
                .spawn(move || {
                    tikv_util::thread_group::set_properties(props);
                    set_io_type(IOType::ForegroundWrite);
                    poller.poll();
                })
                .unwrap();
            workers.push(t);
        }
        let _ = self
            .state
            .expected_pool_size
            .fetch_add(size, Ordering::Relaxed);
        let s = self.state.pool_size.fetch_add(size, Ordering::Relaxed);
        info!("increase thread pool"; "from" => s, "increase to" => s + size, "pool" => ?self.state.name_prefix);
        SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
    }

    pub fn cleanup_poller_threads(&mut self) {
        let mut joinable_workers = self.state.joinable_workers.lock().unwrap();
        let mut workers = self.state.workers.lock().unwrap();
        let mut last_error = None;
        for tid in joinable_workers.drain(..) {
            if let Some(i) = workers.iter().position(|v| v.thread().id() == tid) {
                let h = workers.swap_remove(i);
                debug!("cleanup poller waiting for {}", h.thread().name().unwrap());
                if let Err(e) = h.join() {
                    error!("failed to join worker thread: {:?}", e);
                    last_error = Some(e);
                }
            }
        }
        if let Some(e) = last_error {
            safe_panic!("failed to join worker thread: {:?}", e);
        }
    }
}

#[derive(Debug)]
pub enum RaftstoreConfigTask {
    ScalePool(String, usize),
    CleanupPollerThreads(String),
}

impl Display for RaftstoreConfigTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self {
            RaftstoreConfigTask::ScalePool(type_, size) => {
                write!(f, "Scale pool ")?;
                match type_.as_str() {
                    "apply" => write!(f, "ajusts apply: {} ", size),
                    "raft" => write!(f, "ajusts raft: {} ", size),
                    _ => unreachable!(),
                }
            }
            RaftstoreConfigTask::CleanupPollerThreads(type_) => {
                write!(f, "Cleanup {}'s poller threads", type_)
            }
        }
    }
}

pub struct RaftstoreConfigManager {
    scheduler: Scheduler<RaftstoreConfigTask>,
    config: Arc<VersionTrack<Config>>,
}

impl RaftstoreConfigManager {
    pub fn new(
        scheduler: Scheduler<RaftstoreConfigTask>,
        config: Arc<VersionTrack<Config>>,
    ) -> RaftstoreConfigManager {
        RaftstoreConfigManager { scheduler, config }
    }
}

impl ConfigManager for RaftstoreConfigManager {
    fn dispatch(
        &mut self,
        mut change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let change_str = format!("{:?}", change);
        {
            let change = change.clone();
            self.config
                .update(move |cfg: &mut Config| cfg.update(change));
        }
        if let Some(ConfigValue::Module(mut apply_batch_system_change)) =
            change.remove("apply_batch_system").map(Into::into)
        {
            if let Some(pool_size) = apply_batch_system_change.remove("pool_size") {
                let pool_size: usize = pool_size.into();
                CONFIG_RAFTSTORE_GAUGE
                    .with_label_values(&["apply_pool_size"])
                    .set(pool_size as f64);
                let scale_pool = RaftstoreConfigTask::ScalePool("apply".to_string(), pool_size);
                if let Err(e) = self.scheduler.schedule(scale_pool) {
                    error!("raftstore configuration manager schedule scale apply pool work task failed"; "err"=> ?e);
                }
                SCALE_FINISHED_CHANNEL.1.recv().unwrap();
                info!("recv RaftstoreConfigTask::ScalePool finished");
                if let Err(e) = self
                    .scheduler
                    .schedule(RaftstoreConfigTask::CleanupPollerThreads(
                        "apply".to_string(),
                    ))
                {
                    error!("raftstore configuration manager schedule cleanup apply poller threads work task failed"; "err"=> ?e);
                }
                SCALE_FINISHED_CHANNEL.1.recv().unwrap();
                info!("recv RaftstoreConfigTask::CleanupPollerThreads finished");
            }
        }
        if let Some(ConfigValue::Module(mut raft_batch_system_change)) =
            change.remove("store_batch_system").map(Into::into)
        {
            if let Some(pool_size) = raft_batch_system_change.remove("pool_size") {
                let pool_size: usize = pool_size.into();
                CONFIG_RAFTSTORE_GAUGE
                    .with_label_values(&["store_pool_size"])
                    .set(pool_size as f64);
                let scale_pool = RaftstoreConfigTask::ScalePool("raft".to_string(), pool_size);
                if let Err(e) = self.scheduler.schedule(scale_pool) {
                    error!("raftstore configuration manager schedule scale raft pool work task failed"; "err"=> ?e);
                }
                SCALE_FINISHED_CHANNEL.1.recv().unwrap();
                info!("recv RaftstoreConfigTask::ScalePool finished");
                if let Err(e) = self
                    .scheduler
                    .schedule(RaftstoreConfigTask::CleanupPollerThreads(
                        "raft".to_string(),
                    ))
                {
                    error!("raftstore configuration manager schedule cleanup raft poller threads work task failed"; "err"=> ?e);
                }
                SCALE_FINISHED_CHANNEL.1.recv().unwrap();
                info!("recv RaftstoreConfigTask::CleanupPollerThreads finished");
            }
        }
        Config::write_change_into_metrics(change);
        info!(
            "raftstore config changed";
            "change" => change_str,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validate() {
        let mut cfg = Config::new();
        cfg.validate().unwrap();
        assert_eq!(
            cfg.raft_min_election_timeout_ticks,
            cfg.raft_election_timeout_ticks
        );
        assert_eq!(
            cfg.raft_max_election_timeout_ticks,
            cfg.raft_election_timeout_ticks * 2
        );

        cfg.raft_heartbeat_ticks = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_heartbeat_ticks = 10;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_min_election_timeout_ticks = 5;
        cfg.validate().unwrap_err();
        cfg.raft_min_election_timeout_ticks = 25;
        cfg.validate().unwrap_err();
        cfg.raft_min_election_timeout_ticks = 10;
        cfg.validate().unwrap();

        cfg.raft_heartbeat_ticks = 11;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_log_gc_threshold = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_log_gc_size_limit = ReadableSize(0);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_base_tick_interval = ReadableDuration::secs(1);
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_store_max_leader_lease = ReadableDuration::secs(20);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_log_gc_count_limit = 100;
        cfg.merge_max_log_gap = 110;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.merge_check_tick_interval = ReadableDuration::secs(0);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_base_tick_interval = ReadableDuration::secs(1);
        cfg.raft_election_timeout_ticks = 10;
        cfg.peer_stale_state_check_interval = ReadableDuration::secs(5);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.peer_stale_state_check_interval = ReadableDuration::minutes(2);
        cfg.abnormal_leader_missing_duration = ReadableDuration::minutes(1);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.abnormal_leader_missing_duration = ReadableDuration::minutes(2);
        cfg.max_leader_missing_duration = ReadableDuration::minutes(1);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.local_read_batch_size = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.apply_batch_system.max_batch_size = Some(0);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.apply_batch_system.pool_size = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.store_batch_system.max_batch_size = Some(0);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.store_batch_system.pool_size = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.hibernate_regions = true;
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.store_batch_system.max_batch_size, Some(256));
        assert_eq!(cfg.apply_batch_system.max_batch_size, Some(256));

        cfg = Config::new();
        cfg.hibernate_regions = false;
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.store_batch_system.max_batch_size, Some(1024));
        assert_eq!(cfg.apply_batch_system.max_batch_size, Some(256));

        cfg = Config::new();
        cfg.hibernate_regions = true;
        cfg.store_batch_system.max_batch_size = Some(123);
        cfg.apply_batch_system.max_batch_size = Some(234);
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.store_batch_system.max_batch_size, Some(123));
        assert_eq!(cfg.apply_batch_system.max_batch_size, Some(234));

        cfg = Config::new();
        cfg.future_poll_size = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_base_tick_interval = ReadableDuration::secs(1);
        cfg.raft_election_timeout_ticks = 11;
        cfg.raft_store_max_leader_lease = ReadableDuration::secs(11);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.hibernate_regions = true;
        cfg.max_peer_down_duration = ReadableDuration::minutes(5);
        cfg.peer_stale_state_check_interval = ReadableDuration::minutes(5);
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.max_peer_down_duration, ReadableDuration::minutes(10));
    }
}
