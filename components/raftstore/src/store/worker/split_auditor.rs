// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use crossbeam_skiplist::{SkipMap, map::Entry};

const CAPACITY: usize = 0x2000;
const GC_ENTRY_LIMIT: usize = 0x80;
const GC_LIFETIME: u64 = 600;
struct SplitAuditorCore<P> {
    disabled_region_ids: SkipMap<u64, Instant>,
    capacity: usize,
    gc_offset: Mutex<u64>,
    gc_filter: P,
}

impl<P> SplitAuditorCore<P>
where
    P: Fn(&Entry<u64, Instant>) -> bool,
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
        self.disabled_region_ids.contains_key(&region_id)
    }

    #[inline]
    pub fn disable(&self, region_id: u64) {
        self.disabled_region_ids.insert(region_id, Instant::now());
    }

    #[inline]
    pub fn enable(&self, region_id: u64) {
        self.disabled_region_ids.remove(&region_id);
        self.gc();
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
pub struct SplitAuditor {
    core: Arc<SplitAuditorCore<fn(&Entry<u64, Instant>) -> bool>>,
}

impl SplitAuditor {
    #[inline]
    pub fn new() -> Self {
        Self {
            core: Arc::new(SplitAuditorCore::new(
                |e| e.value().elapsed().as_secs() > GC_LIFETIME,
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

#[cfg(test)]
mod tests {
    use rand::Rng;
    use test::{Bencher, black_box};

    use super::*;
    #[test]
    fn test_multiple_regions_management() {
        let auditor = SplitAuditor::new();
        let region1 = 1;
        let region2 = 2;

        assert!(!auditor.is_disabled(region1));
        assert!(!auditor.is_disabled(region2));
        assert!(auditor.core.disabled_region_ids.is_empty());

        auditor.disable(region1);
        auditor.disable(region2);
        assert!(auditor.is_disabled(region1));
        assert!(auditor.is_disabled(region2));
        assert_eq!(auditor.core.disabled_region_ids.len(), 2);

        auditor.enable(region1);
        assert!(!auditor.is_disabled(region1));
        assert!(auditor.is_disabled(region2));
        assert_eq!(auditor.core.disabled_region_ids.len(), 1);
    }

    #[test]
    fn test_enable_non_disabled_region() {
        let auditor = SplitAuditor::new();
        let region_id = 1;

        auditor.enable(region_id);
        assert!(!auditor.is_disabled(region_id));
        assert!(auditor.core.disabled_region_ids.is_empty());
    }

    #[test]
    fn test_gc_removes_expired_entries() {
        let auditor = SplitAuditorCore::new(|e| e.key() & 1 == 0, 300);
        for i in 0..200 {
            auditor.disabled_region_ids.insert(i, Instant::now());
        }
        auditor.gc();
        assert_eq!(*auditor.gc_offset.lock().unwrap(), 0);
        assert_eq!(auditor.disabled_region_ids.len(), 200);
        for i in 0..100 {
            auditor.disabled_region_ids.insert(i, Instant::now());
        }
        auditor.gc();
        assert_eq!(*auditor.gc_offset.lock().unwrap(), 128);
        auditor.gc();
        assert_eq!(*auditor.gc_offset.lock().unwrap(), 256);
        auditor.gc();
        assert_eq!(*auditor.gc_offset.lock().unwrap(), 0);
        assert_eq!(auditor.disabled_region_ids.len(), 150);
        auditor.gc();
        assert_eq!(*auditor.gc_offset.lock().unwrap(), 0);
        assert_eq!(auditor.disabled_region_ids.len(), 150);
    }

    #[bench]
    fn bench_1m_regions_mixed_ops(b: &mut Bencher) {
        const TOTAL_REGIONS: u64 = 1_000_000;

        let auditor = SplitAuditor::new();
        let mut rng = rand::thread_rng();

        for i in 0..=TOTAL_REGIONS {
            auditor.disable(i);
        }
        b.iter(|| {
            let region_id = rng.gen_range(0..TOTAL_REGIONS);
            if rng.gen() {
                auditor.disable(black_box(region_id));
            } else {
                auditor.enable(black_box(region_id));
            }
        });
    }
}
