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

use std::collections::HashMap;

pub const ROLL_INTERVAL_SECS: u64 = 60 * 10;

struct WriteStats {
    pub history: u64,
    pub current: u64,
}

impl WriteStats {
    pub fn new() -> WriteStats {
        WriteStats {
            history: 0,
            current: 0,
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

    pub fn incr_foreach(&mut self, n: u64) {
        for (_, stat) in &mut self.stats {
            stat.incr(n);
        }
    }

    pub fn roll(&mut self) {
        for (_, stat) in &mut self.stats {
            stat.roll();
        }
    }

    pub fn clear_history(&mut self, region_id: u64) {
        let stat = self.stats.entry(region_id).or_insert_with(WriteStats::new);
        stat.clear_history();
    }

    pub fn get_history(&self, region_id: u64) -> u64 {
        self.stats.get(&region_id).map_or(0, |s| s.history)
    }
}

#[cfg(test)]
mod tests {
    use super::RegionsWriteStats;

    #[test]
    fn test_regions_write_stats() {
        let mut stats = RegionsWriteStats::new();

        stats.incr(1, 100);
        assert_eq!(stats.get_history(1), 0);
        stats.roll();
        assert_eq!(stats.get_history(1), 100);
        stats.clear_history(1);
        assert_eq!(stats.get_history(1), 0);

        stats.incr_foreach(100);
        assert_eq!(stats.get_history(1), 0);
        stats.roll();
        assert_eq!(stats.get_history(1), 100);
    }
}
