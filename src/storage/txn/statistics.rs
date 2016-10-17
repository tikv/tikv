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

use std::time::{Instant, Duration};
use std::collections::HashMap;
use rand::{self, Rng};
use std::u8;

pub const ROLL_INTERVAL_SECS: u64 = 60 * 10;
pub const CHECK_EXPIRED_INTERVAL_SECS: u64 = 60 * 60;
pub const U8_MAX: u64 = u8::max_value() as u64;

struct WriteStats {
    pub history: u64,
    current: u64,
    pub last_active: Instant,
}

impl WriteStats {
    pub fn new() -> WriteStats {
        WriteStats {
            history: 0,
            current: 0,
            last_active: Instant::now(),
        }
    }

    pub fn roll(&mut self) {
        self.history += self.current;
        self.current = 0;
    }

    pub fn incr(&mut self, n: u64) {
        self.current += n;
    }

    pub fn clear_history(&mut self) {
        self.history = 0;
    }
}

pub struct RegionsWriteStats {
    // region_id -> write stats
    stats: HashMap<u64, WriteStats>,
}

impl RegionsWriteStats {
    pub fn new() -> RegionsWriteStats {
        RegionsWriteStats { stats: HashMap::new() }
    }

    pub fn incr(&mut self, region_id: u64, n: u64) {
        let stat = self.stats.entry(region_id).or_insert_with(WriteStats::new);
        stat.incr(n);
    }

    pub fn roll_and_up(&mut self, base: u64) {
        let mut rng = rand::thread_rng();
        for (_, stat) in &mut self.stats {
            stat.roll();

            // We initialize the stat with random number between [0 - base]. If we just use base
            // initialize the stat, A large number of regions that haven't been written yet might
            // execute GC command at about the same time.
            let score = base * rng.gen::<u8>() as u64 / U8_MAX;
            stat.incr(score);
        }
    }

    pub fn clear_history(&mut self, region_id: u64) {
        let stat = self.stats.entry(region_id).or_insert_with(WriteStats::new);
        stat.clear_history();
    }

    pub fn get_history(&self, region_id: u64) -> u64 {
        self.stats.get(&region_id).map_or(0, |s| s.history)
    }

    fn collect_expired(&self, timeout_secs: u64) -> Vec<u64> {
        let timeout = Duration::from_secs(timeout_secs);
        let mut expired = Vec::with_capacity(self.stats.len());
        let now = Instant::now();
        for (region_id, stat) in &self.stats {
            let duration = now.duration_since(stat.last_active.clone());
            if duration > timeout {
                expired.push(*region_id);
            }
        }
        expired
    }

    // If a region don't receive GC command for a long time, it probably that
    // 1) the region has moved to another store, or
    // 2) the region's leader has transferred to another peer
    // has happened, we don't need to hold WriteStats for this region anymore.
    pub fn check_expired(&mut self, timeout_secs: u64) {
        let expired = self.collect_expired(timeout_secs);

        for region_id in expired {
            info!("Remove expired statistics for region {}", region_id);
            self.stats.remove(&region_id);
        }
    }

    // When receive GC command for a region, we update the last_active of this region.
    pub fn update_active(&mut self, region_id: u64) {
        let stat = self.stats.entry(region_id).or_insert_with(WriteStats::new);
        stat.last_active = Instant::now();
    }

    // used for test
    #[allow(dead_code)]
    pub fn exist(&self, region_id: u64) -> bool {
        self.stats.contains_key(&region_id)
    }
}

#[cfg(test)]
mod tests {
    use super::RegionsWriteStats;
    use std::{thread, time};

    #[test]
    fn test_regions_write_stats() {
        let mut stats = RegionsWriteStats::new();

        stats.incr(1, 100);
        assert_eq!(stats.get_history(1), 0);
        stats.roll_and_up(100);
        assert_eq!(stats.get_history(1), 100);
        stats.clear_history(1);
        assert_eq!(stats.get_history(1), 0);

        stats.update_active(1);
        stats.check_expired(10);
        assert_eq!(stats.exist(1), true);

        let two_seconds = time::Duration::from_secs(2);
        thread::sleep(two_seconds);
        stats.check_expired(1);
        assert_eq!(stats.exist(1), false);
    }
}
