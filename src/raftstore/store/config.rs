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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    // true for high reliability, prevent data loss when power failure.
    pub sync_log: bool,
    pub raftdb_path: String,

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

    // Right region derive origin region id when split.
    pub right_derive_when_split: bool,

    pub allow_remove_leader: bool,
}

impl Default for Config {
    fn default() -> Config {
        let split_size = ReadableSize::mb(256);
        Config {
            sync_log: true,
            raftdb_path: String::new(),
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
pub mod tests {
    use super::*;

    use util::config::*;

    extern crate serde_test;
    use self::serde_test::{assert_de_tokens, Token};

    pub fn default_de_tokens() -> Vec<Token> {
        let config = Config::default();
        vec![
            Token::Struct {
                name: "Config",
                len: 35,
            },
            Token::Str("sync-log"),
            Token::Bool(config.sync_log),

            Token::Str("raftdb-path"),
            Token::Str(""),

            Token::Str("capacity"),
            Token::U64(config.capacity.0),

            Token::Str("raft-base-tick-interval"),
            Token::Str("1s"),

            Token::Str("raft-heartbeat-ticks"),
            Token::U64(config.raft_heartbeat_ticks as u64),

            Token::Str("raft-election-timeout-ticks"),
            Token::U64(config.raft_election_timeout_ticks as u64),

            Token::Str("raft-max-size-per-msg"),
            Token::U64(config.raft_max_size_per_msg.0),

            Token::Str("raft-max-inflight-msgs"),
            Token::U64(config.raft_max_inflight_msgs as u64),

            Token::Str("raft-entry-max-size"),
            Token::U64(config.raft_entry_max_size.0),

            Token::Str("raft-log-gc-tick-interval"),
            Token::Str("10s"),

            Token::Str("raft-log-gc-threshold"),
            Token::U64(config.raft_log_gc_threshold),

            Token::Str("raft-log-gc-count-limit"),
            Token::U64(config.raft_log_gc_count_limit),

            Token::Str("raft-log-gc-size-limit"),
            Token::U64(config.raft_log_gc_size_limit.0),

            Token::Str("split-region-check-tick-interval"),
            Token::Str("10s"),

            Token::Str("region-max-size"),
            Token::U64(config.region_max_size.0),

            Token::Str("region-split-size"),
            Token::U64(config.region_split_size.0),

            Token::Str("region-split-check-diff"),
            Token::U64(config.region_split_check_diff.0),

            Token::Str("region-compact-check-interval"),
            Token::Str("0s"),

            Token::Str("region-compact-delete-keys-count"),
            Token::U64(config.region_compact_delete_keys_count),

            Token::Str("pd-heartbeat-tick-interval"),
            Token::Str("1m"),

            Token::Str("pd-store-heartbeat-tick-interval"),
            Token::Str("10s"),

            Token::Str("notify-capacity"),
            Token::U64(config.notify_capacity as u64),

            Token::Str("snap-mgr-gc-tick-interval"),
            Token::Str("1m"),

            Token::Str("snap-gc-timeout"),
            Token::Str("4h"),

            Token::Str("messages-per-tick"),
            Token::U64(config.messages_per_tick as u64),

            Token::Str("max-peer-down-duration"),
            Token::Str("5m"),

            Token::Str("max-leader-missing-duration"),
            Token::Str("2h"),

            Token::Str("snap-apply-batch-size"),
            Token::U64(config.snap_apply_batch_size.0),

            Token::Str("lock-cf-compact-interval"),
            Token::Str("10m"),

            Token::Str("lock-cf-compact-bytes-threshold"),
            Token::U64(config.lock_cf_compact_bytes_threshold.0),

            Token::Str("consistency-check-interval"),
            Token::Str("0s"),

            Token::Str("report-region-flow-interval"),
            Token::Str("1m"),

            Token::Str("raft-store-max-leader-lease"),
            Token::Str("9s"),

            Token::Str("right-derive-when-split"),
            Token::Bool(config.right_derive_when_split),

            Token::Str("allow-remove-leader"),
            Token::Bool(config.allow_remove_leader),

            Token::StructEnd,
        ]
    }

    #[test]
    fn test_de_config() {
        let config = Config::default();
        assert_de_tokens(&config, &default_de_tokens());
    }

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
