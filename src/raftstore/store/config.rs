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

use std::u64;

use time::Duration as TimeDuration;

use raftstore::Result;
use util::config::{ReadableDuration, ReadableSize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    // true for high reliability, prevent data loss when power failure.
    pub sync_log: bool,

    // store capacity. 0 means no limit.
    pub capacity: ReadableSize,

    // raft_base_tick_interval is a base tick interval (ms).
    pub raft_base_tick_interval: ReadableDuration,
    pub raft_heartbeat_ticks: usize,
    pub raft_election_timeout_ticks: usize,
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

    // Interval (ms) to check region whether need to be split or not.
    pub split_region_check_tick_interval: ReadableDuration,
    /// When region [a, b) size meets region_max_size, it will be split
    /// into two region into [a, c), [c, b). And the size of [a, c) will
    /// be region_split_size (or a little bit smaller).
    pub region_max_size: ReadableSize,
    pub region_split_size: ReadableSize,
    /// When size change of region exceed the diff since last check, it
    /// will be checked again whether it should be split.
    pub region_split_check_diff: ReadableSize,
    /// Interval (ms) to check whether start compaction for a region.
    pub region_compact_check_interval: ReadableDuration,
    /// When delete keys of a region exceeds the size, a compaction will
    /// be started.
    pub region_compact_delete_keys_count: u64,
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

    pub snap_apply_batch_size: ReadableSize,

    // Interval (ms) to check region whether the data is consistent.
    pub consistency_check_interval: ReadableDuration,

    pub report_region_flow_interval: ReadableDuration,

    // The lease provided by a successfully proposed and applied entry.
    pub raft_store_max_leader_lease: ReadableDuration,

    pub use_sst_file_snapshot: bool,

    // Right region derive origin region id when split.
    pub right_derive_when_split: bool,

    pub allow_remove_leader: bool,
}

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(256);
        Config {
            sync_log: true,
            capacity: ReadableSize(0),
            raft_base_tick_interval: ReadableDuration::secs(1),
            raft_heartbeat_ticks: 2,
            raft_election_timeout_ticks: 10,
            raft_max_size_per_msg: ReadableSize::mb(1),
            raft_max_inflight_msgs: 256,
            raft_entry_max_size: ReadableSize::mb(8),
            raft_log_gc_tick_interval: ReadableDuration::secs(10),
            raft_log_gc_threshold: 50,
            // Assume the average size of entries is 1k.
            raft_log_gc_count_limit: split_size * 3 / 4 / ReadableSize::kb(1),
            raft_log_gc_size_limit: split_size * 3 / 4,
            split_region_check_tick_interval: ReadableDuration::secs(10),
            region_max_size: split_size / 2 * 3,
            region_split_size: split_size,
            region_split_check_diff: split_size / 8,
            // Disable manual compaction by default.
            region_compact_check_interval: ReadableDuration::secs(0),
            region_compact_delete_keys_count: 1_000_000,
            pd_heartbeat_tick_interval: ReadableDuration::minutes(1),
            pd_store_heartbeat_tick_interval: ReadableDuration::secs(10),
            notify_capacity: 40960,
            snap_mgr_gc_tick_interval: ReadableDuration::minutes(1),
            snap_gc_timeout: ReadableDuration::hours(4),
            messages_per_tick: 4096,
            max_peer_down_duration: ReadableDuration::minutes(5),
            max_leader_missing_duration: ReadableDuration::hours(2),
            snap_apply_batch_size: ReadableSize::mb(10),
            lock_cf_compact_interval: ReadableDuration::minutes(10),
            lock_cf_compact_bytes_threshold: ReadableSize::mb(256),
            // Disable consistency check by default as it will hurt performance.
            // We should turn on this only in our tests.
            consistency_check_interval: ReadableDuration::secs(0),
            report_region_flow_interval: ReadableDuration::minutes(1),
            raft_store_max_leader_lease: ReadableDuration::secs(9),
            use_sst_file_snapshot: true,
            right_derive_when_split: true,
            allow_remove_leader: false,
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

    pub fn validate(&self) -> Result<()> {
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

        if self.raft_log_gc_threshold < 1 {
            return Err(box_err!(
                "raft log gc threshold must >= 1, not {}",
                self.raft_log_gc_threshold
            ));
        }

        if self.raft_log_gc_size_limit.0 == 0 {
            return Err(box_err!("raft log gc size limit should large than 0."));
        }

        if self.region_max_size.0 < self.region_split_size.0 {
            return Err(box_err!(
                "region max size {} must >= split size {}",
                self.region_max_size.0,
                self.region_split_size.0
            ));
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
        assert!(cfg.validate().is_ok());

        cfg.raft_heartbeat_ticks = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_heartbeat_ticks = 10;
        assert!(cfg.validate().is_err());

        cfg.raft_heartbeat_ticks = 11;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_log_gc_threshold = 0;
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_log_gc_size_limit = ReadableSize(0);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.region_max_size = ReadableSize(10);
        cfg.region_split_size = ReadableSize(20);
        assert!(cfg.validate().is_err());

        cfg = Config::new();
        cfg.raft_base_tick_interval = ReadableDuration::secs(1);
        cfg.raft_election_timeout_ticks = 10;
        cfg.raft_store_max_leader_lease = ReadableDuration::secs(20);
        assert!(cfg.validate().is_err());
    }
}
