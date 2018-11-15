// Copyright 2018 PingCAP, Inc.
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

use std::cmp::max;

use storage::{Command, Engine, DATA_CFS};
use util::rocksdb::CompactionStats;
use util::rocksdb::{get_cf_handle, get_cf_num_files_at_level, get_cf_num_immutable_mem_table};
use util::time::{duration_to_sec, Instant};
use std::convert::From;

use super::super::metrics::*;

const LIMIT_INTERVAL: usize = 60;
const MICROS_PER_SEC: u64 = 1000 * 1000;
const THROTTLE_FACTOR: u64 = 17;
const REFILL_INTERVAL: u64 = 1024;

pub struct WriteLimiter<E: Engine> {
    engine: E,

    compaction_stats: [CompactionStats; LIMIT_INTERVAL + 1],
    flush_stats: CompactionStats,

    replace_index: usize,

    // TODO: Dynamically calculate this value according to processing
    // speed of recent write requests.
    sched_pending_write_threshold: usize,

    // used to control write flow
    running_write_bytes: usize,

    // limit rate
    throttle: u64,

    // indicate whether have ever calculate throttle once
    is_first: bool,

    last_refill_time: Option<Instant>,

    bytes_left: u64,

    has_backlog: bool,
}

impl<E: Engine> WriteLimiter<E> {
    pub fn new(engine: E, sched_pending_write_threshold: usize) -> Self {
        WriteLimiter {
            engine,
            compaction_stats: Default::default(),
            flush_stats: Default::default(),
            replace_index: 1,
            sched_pending_write_threshold,
            is_first: true,
            bytes_left: 0,
            last_refill_time: None,
            has_backlog: false,
        }
    }

    pub fn should_limit(&self, cmd: &Command) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        if cmd.need_flow_control() {
            return false;
        }

        if self.running_write_bytes >= self.sched_pending_write_threshold {
            return true;
        }

        if !self.has_backlog {
            return false;
        }

        let write_bytes = u64::from(cmd.write_bytes());
        // rate_limiter token and refill
        if write_bytes < self.bytes_left {
            self.bytes_left -= write_bytes;
            return false;
        }

        // avoid to call it frequently
        let now = Instant::now();

        if let Some(last_refill_time) = self.last_refill_time {
            if last_refill_time > now {
                return true;
            } else {
                let elapsed = duration_to_sec(now - last_refill_time) * MICROS_PER_SEC;
                self.bytes_left += elapsed / MICROS_PER_SEC * self.throttle;
                if elapsed >= REFILL_INTERVAL && self.bytes_left > write_bytes {
                    last_refill_time = Instant::now();
                    self.bytes_left -= write_bytes;
                    return false;
                }
            }
        }

        let single_refill_amount = self.throttle * REFILL_INTERVAL / MICROS_PER_SEC;
        if self.bytes_left + single_refill_amount >= write_bytes {
            self.bytes_left += single_refill_amount - write_bytes;
            self.last_refill_time = Some(now + REFILL_INTERVAL);
            return false;
        }

        let cost = write_bytes / self.throttle * MICROS_PER_SEC;
        self.last_refill_time = now + cost;
        false
    }

    pub fn update_stats(&mut self, stats: CompactionStats) {
        if stats.is_level0 {
            self.flush_stats += stats
        } else {
            self.compaction_stats[0] += stats
        }
    }

    pub fn recaculate_limit(&mut self) {
        // replace the oldest stats with the newest stats,
        // and reset
        self.compaction_stats[self.replace_index] = self.compaction_stats[0];
        self.compaction_stats[0] = Default::default();
        self.replace_index += 1;
        if self.replace_index == LIMIT_INTERVAL {
            self.replace_index = 1;
        }

        let (mut total_bytes, mut total_keys, mut total_micros, mut total_compacts) = (0, 0, 0, 0);
        for i in 1..LIMIT_INTERVAL {
            total_micros += self.compaction_stats[i].elapsed;
            total_bytes += self.compaction_stats[i].bytes;
            total_keys += self.compaction_stats[i].keys;
            total_compacts += 1;
        }

        let backlog = self.compute_backlog();
        self.has_backlog = backlog != 0;
        if !self.has_backlog {
            return
        }

        // reduce bytes by 10% for each excess level_0 files or excess write buffers.
        let adjustment_bytes = (total_bytes * backlog) / 10;
        total_bytes = max(total_bytes - adjustment_bytes, 1);

        // calculate new throttle on compaction stats.
        let mut new_throttle = if total_bytes != 0 && total_micros != 0 {
            total_bytes * MICROS_PER_SEC / total_micros
        } else if self.flush_stats.bytes != 0 && self.flush_stats.elapsed != 0 {
            self.flush_stats.bytes * MICROS_PER_SEC / self.flush_stats.elapsed
        } else {
            1
        };

        if !self.is_first {
            // using EMA to smooth
            let old_throttle = self.throttle;
            self.throttle = max(
                if new_throttle < old_throttle {
                    new_throttle + (new_throttle - old_throttle) / THROTTLE_FACTOR + 1
                } else {
                    new_throttle - (old_throttle - new_throttle) / THROTTLE_FACTOR + 2
                },
                1,
            );
        } else if new_throttle > 1 {
            self.throttle = new_throttle;
            self.is_first = false;
        }

        // reset flush stats
        self.flush_stats = Default::default();
    }

    fn compute_backlog(&mut self) -> u64 {
        let mut backlog = 0;
        for cf in DATA_CFS {
            let handle = get_cf_handle(&self.engine, cf).unwrap();
            let options = self.engine.get_options_cf(handle);

            let level0_trigger = options.get_level_zero_slowdown_writes_trigger() - 2;
            let memtable_trigger = options.get_max_write_buffer_number() - 1;

            let level0 = get_cf_num_files_at_level(&self.engine, handle, 0).unwrap();
            let memtable = get_cf_num_immutable_mem_table(&self.engine, handle).unwrap();

            backlog += max(level0 - level0_trigger + 1, 0);
            backlog += max(memtable - memtable_trigger + 1, 0);
        }
        backlog
    }

    pub fn on_task_begin(&mut self, bytes: usize) {
        self.running_write_bytes += bytes;
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);
    }

    pub fn on_task_finish(&mut self, bytes: usize) {
        self.running_write_bytes -= bytes;
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);
    }
}
