// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use collections::HashMap;
use futures_util::compat::Future01CompatExt;
use kvproto::{kvrpcpb::KeyRange, metapb::Region};
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tokio::runtime::Handle;

use super::Config;

#[derive(PartialEq, Eq, Hash, Clone)]
struct Range {
    pub start_key: std::vec::Vec<u8>,
    pub end_key: std::vec::Vec<u8>,
}

impl From<KeyRange> for Range {
    fn from(key_range: KeyRange) -> Self {
        Self {
            start_key: key_range.start_key,
            end_key: key_range.end_key,
        }
    }
}

struct ImportModeSwitcherInnerV2 {
    is_regional_import: Arc<AtomicBool>,
    timeout: Duration,
    next_check: HashMap<Range, Instant>,

    import_mode_regions: HashMap<Range, HashSet<u64>>,
}

#[derive(Clone)]
pub struct ImportModeSwitcherV2 {
    inner: Arc<Mutex<ImportModeSwitcherInnerV2>>,
    is_regional_import: Arc<AtomicBool>,
}

impl ImportModeSwitcherV2 {
    pub fn new(cfg: &Config) -> ImportModeSwitcherV2 {
        let timeout = cfg.import_mode_timeout.0;
        let is_regional_import = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(Mutex::new(ImportModeSwitcherInnerV2 {
            timeout,
            next_check: HashMap::default(),
            import_mode_regions: HashMap::default(),
            is_regional_import: is_regional_import.clone(),
        }));
        ImportModeSwitcherV2 {
            inner,
            is_regional_import,
        }
    }

    // Periodically perform timeout check to change import mode of some regions back
    // to normal mode.
    pub fn start(&self, executor: &Handle) {
        // spawn a background future to put TiKV back into normal mode after timeout
        let inner = self.inner.clone();
        let switcher = Arc::downgrade(&inner);
        let timer_loop = async move {
            let mut prev_range = None;
            // loop until the switcher has been dropped
            while let Some(switcher) = switcher.upgrade() {
                let next_check = {
                    let now = Instant::now();
                    let mut switcher = switcher.lock().unwrap();
                    if let Some(ref range) = prev_range.take() {
                        if let Some(next_check) = switcher.next_check.get(range) {
                            if now >= *next_check {
                                switcher.next_check.remove(range);
                                switcher.import_mode_regions.remove(range);
                            }
                        }
                    }

                    let mut min_next_check = now + switcher.timeout;
                    for (range, next_check) in &switcher.next_check {
                        if *next_check < min_next_check {
                            min_next_check = *next_check;
                            prev_range = Some(range.clone());
                        }
                    }
                    min_next_check
                };

                let ok = GLOBAL_TIMER_HANDLE.delay(next_check).compat().await.is_ok();
                if !ok {
                    warn!("failed to delay with global timer");
                }
            }
        };
        executor.spawn(timer_loop);
    }

    pub fn enter_or_extend_import_mode_regions_in_range(
        &self,
        range: KeyRange,
        store_regions_info: &HashMap<u64, (Region, bool)>,
    ) {
        let range = Range::from(range);
        let mut inner = self.inner.lock().unwrap();
        inner.is_regional_import.store(true, Ordering::Release);
        let mut regions = HashSet::default();
        for (region_id, (region, _)) in store_regions_info {
            if region_overlap_with_range(&range, region) {
                regions.insert(*region_id);
            }
        }
        let next_check = Instant::now() + inner.timeout;
        // range may exist before, but store_regions_info may have been changed
        inner.import_mode_regions.insert(range.clone(), regions);
        inner.next_check.insert(range, next_check);
    }

    pub fn clear_import_mode_regions_in_range(&self, range: KeyRange) {
        let mut inner = self.inner.lock().unwrap();
        let range = Range::from(range);
        inner.import_mode_regions.remove(&range);
        inner.next_check.remove(&range);
    }

    pub fn regional_import_mode(&self) -> bool {
        self.is_regional_import.load(Ordering::Acquire)
    }

    pub fn region_in_import_mode(&self, region_id: u64) -> bool {
        for regions in self.inner.lock().unwrap().import_mode_regions.values() {
            if regions.contains(&region_id) {
                return true;
            }
        }
        false
    }
}

fn region_overlap_with_range(range: &Range, region: &Region) -> bool {
    (region.end_key.is_empty() || range.start_key < region.end_key)
        && (range.end_key.is_empty() || region.start_key < range.end_key)
}
