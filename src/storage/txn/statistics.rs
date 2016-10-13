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

use time::{get_time, Timespec};
use std::collections::HashMap;

pub const ROLL_INTERVAL_SECS: u64 = 60 * 10;
pub const CHECK_EXPIRED_INTERVAL_SECS: u64 = 60 * 60;

struct WriteStats {
    pub history: u64,
    pub current: u64,
    pub last_active: Timespec,
}

impl WriteStats {
    pub fn new() -> WriteStats {
        WriteStats {
            history: 0,
            current: 0,
            last_active: get_time(),
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
        for (_, stat) in &mut self.stats {
            stat.roll();
            stat.incr(base);
        }
    }

    pub fn clear_history(&mut self, region_id: u64) {
        let stat = self.stats.entry(region_id).or_insert_with(WriteStats::new);
        stat.clear_history();
    }

    pub fn get_history(&self, region_id: u64) -> u64 {
        self.stats.get(&region_id).map_or(0, |s| s.history)
    }

    // used for test
    #[allow(dead_code)]
    pub fn get_current(&self, region_id: u64) -> u64 {
        self.stats.get(&region_id).map_or(0, |s| s.current)
    }

    fn collect_expired(&self, timeout_secs: i64) -> Vec<u64> {
        let now = get_time();
        let mut expired = Vec::with_capacity(self.stats.len());
        for (region_id, stat) in &self.stats {
            if stat.last_active.sec + timeout_secs < now.sec {
                expired.push(*region_id);
            }
        }
        expired
    }

    // If a region don't receive GC command for a long time, it probably that
    // 1) the region has moved to another store, or
    // 2) the region's leader has transferred to another peer
    // has happened, we don't need to hold WriteStats for this region anymore.
    pub fn check_expired(&mut self, timeout_secs: i64) {
        let expired = self.collect_expired(timeout_secs);

        for region_id in expired {
            info!("Remove expired statistics for region {}", region_id);
            self.stats.remove(&region_id);
        }
    }

    // When receive GC command for a region, we update the last_active of this region.
    pub fn update_active(&mut self, region_id: u64) {
        let now = get_time();
        let stat = self.stats.entry(region_id).or_insert_with(WriteStats::new);
        stat.last_active = now;
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
        assert_eq!(stats.get_current(1), 100);
        stats.roll_and_up(100);
        assert_eq!(stats.get_history(1), 100);
        assert_eq!(stats.get_current(1), 100);
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
