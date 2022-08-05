// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

#[derive(Debug)]
struct LatestTablet<EK> {
    data: Mutex<Option<EK>>,
    version: AtomicU64,
}

/// Tablet may change during split, merge and applying snapshot. So we need a
/// shared value to reflect the latest tablet. `CachedTablet` provide cache that
/// can speed up common access.
#[derive(Clone, Debug)]
pub struct CachedTablet<EK> {
    latest: Arc<LatestTablet<EK>>,
    cache: Option<EK>,
    version: u64,
}

impl<EK: Clone> CachedTablet<EK> {
    #[inline]
    pub fn new(data: Option<EK>) -> Self {
        CachedTablet {
            latest: Arc::new(LatestTablet {
                data: Mutex::new(data.clone()),
                version: AtomicU64::new(0),
            }),
            cache: data,
            version: 0,
        }
    }

    pub fn set(&mut self, data: EK) {
        self.version = {
            let mut latest_data = self.latest.data.lock().unwrap();
            *latest_data = Some(data.clone());
            self.latest.version.fetch_add(1, Ordering::Relaxed) + 1
        };
        self.cache = Some(data);
    }

    /// Get the tablet from cache without checking if it's up to date.
    #[inline]
    pub fn cache(&self) -> Option<&EK> {
        self.cache.as_ref()
    }

    /// Get the latest tablet.
    #[inline]
    pub fn latest(&mut self) -> Option<&EK> {
        if self.latest.version.load(Ordering::Relaxed) > self.version {
            let latest_data = self.latest.data.lock().unwrap();
            self.version = self.latest.version.load(Ordering::Relaxed);
            self.cache = latest_data.clone();
        }
        self.cache()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_tablet() {
        let mut cached_tablet = CachedTablet::new(None);
        assert_eq!(cached_tablet.cache(), None);
        assert_eq!(cached_tablet.latest(), None);

        cached_tablet = CachedTablet::new(Some(1));
        assert_eq!(cached_tablet.cache().cloned(), Some(1));
        assert_eq!(cached_tablet.latest().cloned(), Some(1));

        // Setting tablet will refresh cache immediately.
        cached_tablet.set(2);
        assert_eq!(cached_tablet.cache().cloned(), Some(2));

        // Test `latest()` will use cache.
        // Unsafe modify the data.
        let old_data = *cached_tablet.latest.data.lock().unwrap();
        *cached_tablet.latest.data.lock().unwrap() = Some(0);
        assert_eq!(cached_tablet.latest().cloned(), old_data);
        // Restore the data.
        *cached_tablet.latest.data.lock().unwrap() = old_data;

        let mut cloned = cached_tablet.clone();
        // Clone should reuse cache.
        assert_eq!(cloned.cache().cloned(), Some(2));
        cloned.set(1);
        assert_eq!(cloned.cache().cloned(), Some(1));
        assert_eq!(cloned.latest().cloned(), Some(1));

        // Local cache won't be refreshed until querying latest.
        assert_eq!(cached_tablet.cache().cloned(), Some(2));
        assert_eq!(cached_tablet.latest().cloned(), Some(1));
        assert_eq!(cached_tablet.cache().cloned(), Some(1));
    }
}
