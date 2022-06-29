// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use online_config::OnlineConfig;
use raftstore::coprocessor;
use serde::{Deserialize, Serialize};
use tikv_util::config::{ReadableDuration, ReadableSize};
use time::Duration as TimeDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
pub struct Config {
    // minimizes disruption when a partitioned node rejoins the cluster by using a two phase election.
    #[online_config(skip)]
    pub prevote: bool,

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

    pub leader_transfer_max_log_lag: u64,

    // The lease provided by a successfully proposed and applied entry.
    pub raft_store_max_leader_lease: ReadableDuration,

    pub renew_leader_lease_advance_duration: ReadableDuration,

    pub allow_remove_leader: bool,

    // Interval to gc unnecessary raft log (ms).
    pub raft_log_gc_tick_interval: ReadableDuration,

    // Interval (ms) to check region whether need to be split or not.
    pub split_region_check_tick_interval: ReadableDuration,

    // Interval (ms) to check region whether need to switch mem-table or not.
    pub switch_mem_table_check_tick_interval: ReadableDuration,

    pub pd_heartbeat_tick_interval: ReadableDuration,

    pub pd_store_heartbeat_tick_interval: ReadableDuration,

    pub local_file_gc_timeout: ReadableDuration,

    pub local_file_gc_tick_interval: ReadableDuration,

    pub update_safe_ts_interval: ReadableDuration,

    pub consistency_check_interval: ReadableDuration,

    pub channel_capacity: usize,

    pub region_split_size: ReadableSize,

    pub apply_pool_size: usize,

    pub async_io: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            prevote: true,
            capacity: ReadableSize(0),
            raft_base_tick_interval: ReadableDuration::secs(1),
            raft_heartbeat_ticks: 2,
            raft_election_timeout_ticks: 10,
            raft_min_election_timeout_ticks: 0,
            raft_max_election_timeout_ticks: 0,
            raft_max_size_per_msg: ReadableSize::mb(1),
            raft_max_inflight_msgs: 256,
            raft_entry_max_size: ReadableSize::mb(8),
            max_peer_down_duration: ReadableDuration::minutes(10),
            max_leader_missing_duration: ReadableDuration::hours(2),
            abnormal_leader_missing_duration: ReadableDuration::minutes(10),
            peer_stale_state_check_interval: ReadableDuration::minutes(5),
            leader_transfer_max_log_lag: 128,
            raft_store_max_leader_lease: ReadableDuration::secs(9),
            renew_leader_lease_advance_duration: ReadableDuration::secs(0),
            allow_remove_leader: false,
            raft_log_gc_tick_interval: ReadableDuration::secs(10),
            split_region_check_tick_interval: ReadableDuration::secs(3),
            switch_mem_table_check_tick_interval: ReadableDuration::minutes(1),
            region_split_size: ReadableSize::mb(256),
            pd_heartbeat_tick_interval: ReadableDuration::minutes(1),
            pd_store_heartbeat_tick_interval: ReadableDuration::secs(10),
            local_file_gc_timeout: ReadableDuration::minutes(30),
            local_file_gc_tick_interval: ReadableDuration::minutes(10),
            update_safe_ts_interval: ReadableDuration::secs(60),
            // Disable consistency check by default as it will hurt performance.
            // We should turn on this only in our tests.
            consistency_check_interval: ReadableDuration::secs(0),
            channel_capacity: 40960,
            apply_pool_size: 3,
            async_io: false,
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

    pub fn renew_leader_lease_advance_duration(&self) -> TimeDuration {
        TimeDuration::from_std(self.renew_leader_lease_advance_duration.0).unwrap()
    }

    pub fn raft_heartbeat_interval(&self) -> Duration {
        self.raft_base_tick_interval.0 * self.raft_heartbeat_ticks as u32
    }

    pub fn from_old(old: &raftstore::store::Config, old_cop: &coprocessor::Config) -> Self {
        let mut cfg = Config::default();
        cfg.raft_base_tick_interval = old.raft_base_tick_interval;
        cfg.raft_heartbeat_ticks = old.raft_heartbeat_ticks;
        if cfg.split_region_check_tick_interval > old.split_region_check_tick_interval {
            // The default old interval is too large, we only set if it's smaller for test.
            cfg.split_region_check_tick_interval = old.split_region_check_tick_interval;
        }
        cfg.raft_election_timeout_ticks = old.raft_election_timeout_ticks;
        cfg.raft_min_election_timeout_ticks = old.raft_min_election_timeout_ticks;
        cfg.raft_max_election_timeout_ticks = old.raft_max_election_timeout_ticks;
        cfg.raft_max_size_per_msg = old.raft_max_size_per_msg;
        cfg.raft_max_inflight_msgs = old.raft_max_inflight_msgs;
        cfg.raft_entry_max_size = old.raft_entry_max_size;
        cfg.raft_store_max_leader_lease = old.raft_store_max_leader_lease;
        cfg.renew_leader_lease_advance_duration = old.renew_leader_lease_advance_duration;
        cfg.allow_remove_leader = old.allow_remove_leader();
        cfg.pd_heartbeat_tick_interval = old.pd_heartbeat_tick_interval;
        cfg.pd_store_heartbeat_tick_interval = old.pd_store_heartbeat_tick_interval;
        cfg.max_peer_down_duration = old.max_peer_down_duration;
        cfg.max_leader_missing_duration = old.max_leader_missing_duration;
        cfg.abnormal_leader_missing_duration = old.abnormal_leader_missing_duration;
        cfg.peer_stale_state_check_interval = old.peer_stale_state_check_interval;
        cfg.leader_transfer_max_log_lag = old.leader_transfer_max_log_lag;

        cfg.region_split_size = old_cop.region_split_size;
        cfg.apply_pool_size = old.apply_batch_system.pool_size;
        cfg.async_io = old.store_io_pool_size > 0;
        cfg.local_file_gc_tick_interval = old.local_file_gc_tick_interval;
        cfg.local_file_gc_timeout = old.local_file_gc_timeout;
        cfg
    }
}
