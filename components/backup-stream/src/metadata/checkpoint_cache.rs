// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use tikv_util::time::Instant;
use txn_types::TimeStamp;

/// The lease time of a checkpoint.
/// 12s is the default interval of the coornaditor tick.
const CACHE_LEASE_TIME: Duration = Duration::from_secs(12);

pub struct CheckpointCache {
    last_access: Instant,
    checkpoint: TimeStamp,

    cache_lease_time: Duration,
}

impl Default for CheckpointCache {
    fn default() -> Self {
        Self {
            last_access: Instant::now_coarse(),
            checkpoint: TimeStamp::zero(),

            cache_lease_time: CACHE_LEASE_TIME,
        }
    }
}

impl CheckpointCache {
    #[cfg(test)]
    pub fn with_cache_lease(lease: Duration) -> Self {
        Self {
            cache_lease_time: lease,
            ..Self::default()
        }
    }

    pub fn update(&mut self, checkpoint: impl Into<TimeStamp>) {
        self.last_access = Instant::now_coarse();
        self.checkpoint = self.checkpoint.max(checkpoint.into())
    }

    pub fn get(&self) -> Option<TimeStamp> {
        if self.checkpoint.is_zero()
            || self.last_access.saturating_elapsed() > self.cache_lease_time
        {
            return None;
        }
        Some(self.checkpoint)
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::CheckpointCache;

    #[test]
    fn test_basic() {
        let mut c = CheckpointCache::with_cache_lease(Duration::from_millis(100));
        assert_eq!(c.get(), None);
        c.update(42);
        assert_eq!(c.get(), Some(42.into()));
        c.update(41);
        assert_eq!(c.get(), Some(42.into()));
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(c.get(), None);
    }
}
