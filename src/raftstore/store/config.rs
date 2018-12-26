// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;
use std::u64;

use time::Duration as TimeDuration;

use raftstore::{coprocessor, Result};
use util::config::{ReadableDuration, ReadableSize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    // true for high reliability, prevent data loss when power failure.
    pub sync_log: bool,
    // minimizes disruption when a partitioned node rejoins the cluster by using a two phase election.
    pub prevote: bool,
    pub raftdb_path: String,

    // store capacity. 0 means no limit.
    pub capacity: ReadableSize,

    // raft_base_tick_interval is a base tick interval (ms).
    pub raft_base_tick_interval: ReadableDuration,
    pub raft_heartbeat_ticks: usize,
    pub raft_election_timeout_ticks: usize,
    pub raft_min_election_timeout_ticks: usize,
    pub raft_max_election_timeout_ticks: usize,
    pub raft_max_size_per_msg: ReadableSize,
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
    // delay time before deleting a stale peer
    pub clean_stale_peer_delay: ReadableDuration,
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

    pub leader_transfer_max_log_lag: u64,

    pub snap_apply_batch_size: ReadableSize,

    // Interval (ms) to check region whether the data is consistent.
    pub consistency_check_interval: ReadableDuration,

    pub report_region_flow_interval: ReadableDuration,

    // The lease provided by a successfully proposed and applied entry.
    pub raft_store_max_leader_lease: ReadableDuration,

    // Right region derive origin region id when split.
    pub right_derive_when_split: bool,

    pub allow_remove_leader: bool,

    /// Max log gap allowed to propose merge.
    pub merge_max_log_gap: u64,
    /// Interval to repropose merge.
    pub merge_check_tick_interval: ReadableDuration,

    pub use_delete_range: bool,

    pub cleanup_import_sst_interval: ReadableDuration,

    /// Maximum size of every local read task batch.
    pub local_read_batch_size: u64,

    // Deprecated! These two configuration has been moved to Coprocessor.
    // They are preserved for compatibility check.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub region_max_size: ReadableSize,
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub region_split_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(coprocessor::config::SPLIT_SIZE_MB);
        Config {
            sync_log: true,
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
            raft_entry_cache_life_time: ReadableDuration::secs(30),
            raft_reject_transfer_leader_duration: ReadableDuration::secs(3),
            split_region_check_tick_interval: ReadableDuration::secs(10),
            region_split_check_diff: split_size / 16,
            clean_stale_peer_delay: ReadableDuration::minutes(10),
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
            max_peer_down_duration: ReadableDuration::minutes(5),
            max_leader_missing_duration: ReadableDuration::hours(2),
            abnormal_leader_missing_duration: ReadableDuration::minutes(10),
            peer_stale_state_check_interval: ReadableDuration::minutes(5),
            leader_transfer_max_log_lag: 10,
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
            merge_check_tick_interval: ReadableDuration::secs(10),
            use_delete_range: false,
            cleanup_import_sst_interval: ReadableDuration::minutes(10),
            local_read_batch_size: 1024,

            // They are preserved for compatibility check.
            region_max_size: ReadableSize(0),
            region_split_size: ReadableSize(0),
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use util::config::*;

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
    }
}
