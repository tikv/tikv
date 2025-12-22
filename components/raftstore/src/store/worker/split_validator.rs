// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crossbeam_skiplist::{SkipMap, map::Entry};

// CAPACITY is the cache capacity. When it is exceeded, GC will be triggered.
// If each Region is counted as 128 MiB, the total size needed to fully exhaust
// CAPACITY would be 4 TiB, which is practically impossible to reach in real
// workloads.
const CAPACITY: usize = 0x8000;
// GC_ENTRY_LIMIT is the number of iterations during a single GC operation.
const GC_ENTRY_LIMIT: usize = 0x80;
// GC_LIFETIME is the effective time of the cache, in seconds.
const GC_LIFETIME: Duration = Duration::from_secs(600);

// SplitValidator can disable split for a specific Region within GC_LIFETIME via
// disable(regionID). It uses a SkipMap internally for concurrent access. When
// the number of entries exceeds CAPACITY, a full GC round is triggered, and
// each GC in `disable` scans at most GC_ENTRY_LIMIT entries to avoid
// excessively long execution time.
struct SplitValidatorCore<P> {
    disabled_region_ids: SkipMap<u64, Instant>,
    capacity: usize,
    gc_offset: Mutex<u64>,
    gc_filter: P,
}

impl<P> SplitValidatorCore<P>
where
    P: Fn(&Entry<'_, u64, Instant>) -> bool,
{
    #[inline]
    pub fn new(gc_filter: P, capacity: usize) -> Self {
        Self {
            disabled_region_ids: SkipMap::new(),
            capacity,
            gc_offset: Mutex::new(0),
            gc_filter,
        }
    }

    #[inline]
    pub fn is_disabled(&self, region_id: u64) -> bool {
        let Some(entry) = self.disabled_region_ids.get(&region_id) else {
            return false;
        };

        if (self.gc_filter)(&entry) {
            entry.remove();
            return false;
        }

        true
    }

    #[inline]
    pub fn disable(&self, region_id: u64) {
        self.disabled_region_ids.insert(region_id, Instant::now());
        self.gc()
    }

    #[inline]
    pub fn enable(&self, region_id: u64) {
        self.disabled_region_ids.remove(&region_id);
    }

    fn gc(&self) {
        let _ = self.gc_offset.try_lock().map(|mut gc_offset| {
            if *gc_offset == 0 && self.disabled_region_ids.len() < self.capacity {
                // If capacity is not reached, GC will not be performed.
                // When gc_offset is not 0, the current GC round will be completed.
                return;
            }
            let mut count = 0;
            self.disabled_region_ids
                .range(*gc_offset..)
                .take(GC_ENTRY_LIMIT)
                .filter(|e| {
                    count += 1;
                    *gc_offset = e.key() + 1;
                    (self.gc_filter)(e)
                })
                .for_each(|e| {
                    e.remove();
                });
            if count < GC_ENTRY_LIMIT {
                *gc_offset = 0;
            }
        });
    }
}

#[derive(Clone)]
pub struct SplitValidator {
    core: Arc<SplitValidatorCore<fn(&Entry<'_, u64, Instant>) -> bool>>,
}

impl SplitValidator {
    #[inline]
    pub fn new() -> Self {
        Self {
            core: Arc::new(SplitValidatorCore::new(
                |e| e.value().elapsed() > GC_LIFETIME,
                CAPACITY,
            )),
        }
    }

    #[inline]
    pub fn is_disabled(&self, region_id: u64) -> bool {
        self.core.is_disabled(region_id)
    }

    #[inline]
    pub fn disable(&self, region_id: u64) {
        self.core.disable(region_id);
    }

    #[inline]
    pub fn enable(&self, region_id: u64) {
        self.core.enable(region_id);
    }
}

impl Default for SplitValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use test::{Bencher, black_box};

    use super::*;
    #[test]
    fn test_multiple_regions_management() {
        let validator = SplitValidator::new();
        let region1 = 1;
        let region2 = 2;

        assert!(!validator.is_disabled(region1));
        assert!(!validator.is_disabled(region2));
        assert!(validator.core.disabled_region_ids.is_empty());

        validator.disable(region1);
        validator.disable(region2);
        assert!(validator.is_disabled(region1));
        assert!(validator.is_disabled(region2));
        assert_eq!(validator.core.disabled_region_ids.len(), 2);

        validator.enable(region1);
        assert!(!validator.is_disabled(region1));
        assert!(validator.is_disabled(region2));
        assert_eq!(validator.core.disabled_region_ids.len(), 1);
    }

    #[test]
    fn test_enable_non_disabled_region() {
        let validator = SplitValidator::new();
        let region_id = 1;

        validator.enable(region_id);
        assert!(!validator.is_disabled(region_id));
        assert!(validator.core.disabled_region_ids.is_empty());
    }

    #[test]
    fn test_expired() {
        let validator = SplitValidatorCore::new(|e| e.key() & 1 == 0, 300);
        validator.disable(1);
        assert!(validator.is_disabled(1));
        validator.disable(2);
        assert!(!validator.is_disabled(2));
    }

    #[test]
    fn test_gc_removes_expired_entries() {
        let validator = SplitValidatorCore::new(|e| e.key() & 1 == 0, 300);
        for i in 0..200 {
            validator.disabled_region_ids.insert(i, Instant::now());
        }
        validator.gc();
        assert_eq!(*validator.gc_offset.lock().unwrap(), 0);
        assert_eq!(validator.disabled_region_ids.len(), 200);
        for i in 200..300 {
            validator.disabled_region_ids.insert(i, Instant::now());
        }
        validator.gc();
        assert_eq!(*validator.gc_offset.lock().unwrap(), 128);
        validator.gc();
        assert_eq!(*validator.gc_offset.lock().unwrap(), 256);
        validator.gc();
        assert_eq!(*validator.gc_offset.lock().unwrap(), 0);
        assert_eq!(validator.disabled_region_ids.len(), 150);
        validator.gc();
        assert_eq!(*validator.gc_offset.lock().unwrap(), 0);
        assert_eq!(validator.disabled_region_ids.len(), 150);
    }

    #[test]
    fn test_disable_trigger_gc() {
        let validator = SplitValidatorCore::new(|e| e.key() & 1 == 0, 300);
        for i in 0..200 {
            validator.disable(i);
        }
        assert_eq!(validator.disabled_region_ids.len(), 200);
        for i in 200..300 {
            validator.disable(i);
        }
        assert_eq!(validator.disabled_region_ids.len(), 236);
        validator.disable(300);
        assert_eq!(validator.disabled_region_ids.len(), 173);
        validator.disable(301);
        assert_eq!(validator.disabled_region_ids.len(), 151);
        validator.disable(302);
        assert_eq!(validator.disabled_region_ids.len(), 152);
    }

    #[bench]
    fn bench_1m_regions_mixed_ops(b: &mut Bencher) {
        const TOTAL_REGIONS: u64 = 1_000_000;

        let validator = SplitValidator::new();
        let mut rng = rand::thread_rng();

        for i in 0..=TOTAL_REGIONS {
            validator.disable(i);
        }
        b.iter(|| {
            let region_id = rng.gen_range(0..TOTAL_REGIONS);
            if rng.gen() {
                validator.disable(black_box(region_id));
            } else {
                validator.enable(black_box(region_id));
            }
        });
    }
}
