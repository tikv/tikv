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

impl ImportModeSwitcherInnerV2 {
    fn clear_import_mode_regions_in_range(&mut self, range: Range) {
        self.import_mode_regions.remove(&range);
        self.next_check.remove(&range);

        if self.import_mode_regions.is_empty() {
            // no region is in import mode
            self.is_regional_import.store(false, Ordering::Release);
        }
    }
}

#[derive(Clone)]
pub struct ImportModeSwitcherV2 {
    inner: Arc<Mutex<ImportModeSwitcherInnerV2>>,
    // true if any one region is in import mode
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
                    if let Some(range) = prev_range.take() {
                        if let Some(next_check) = switcher.next_check.get(&range) {
                            if now >= *next_check {
                                switcher.clear_import_mode_regions_in_range(range);
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
        let mut regions = HashSet::default();
        for (region_id, (region, _)) in store_regions_info {
            if region_overlap_with_range(&range, region) {
                regions.insert(*region_id);
            }
        }
        if regions.is_empty() {
            return;
        }
        inner.is_regional_import.store(true, Ordering::Release);
        let next_check = Instant::now() + inner.timeout;
        // range may exist before, but store_regions_info may have been changed
        inner.import_mode_regions.insert(range.clone(), regions);
        inner.next_check.insert(range, next_check);
    }

    pub fn clear_import_mode_regions_in_range(&self, range: KeyRange) {
        let mut inner = self.inner.lock().unwrap();
        let range = Range::from(range);
        inner.clear_import_mode_regions_in_range(range);
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

#[cfg(test)]
mod test {
    use std::thread;

    use tikv_util::config::ReadableDuration;

    use super::*;

    #[test]
    fn test_region_import_mode() {
        let cfg = Config::default();
        let switcher = ImportModeSwitcherV2::new(&cfg);
        let mut region_infos: HashMap<u64, _> = HashMap::default();
        for i in 1..=5 {
            let mut region = Region::default();
            region.set_id(i);
            region.set_start_key(format!("k{:02}", (i - 1) * 10).into());
            region.set_end_key(format!("k{:02}", i * 10).into());
            region_infos.insert(i, (region, true));
        }

        let mut key_range = KeyRange::default();
        key_range.set_end_key(b"j".to_vec());
        switcher.enter_or_extend_import_mode_regions_in_range(key_range.clone(), &region_infos);
        // no regions should be set in import mode
        for i in 1..=5 {
            assert!(!switcher.region_in_import_mode(i));
        }
        assert!(!switcher.is_regional_import.load(Ordering::Acquire));

        // region 1 2 3 should be included
        key_range.set_start_key(b"k09".to_vec());
        key_range.set_end_key(b"k21".to_vec());
        switcher.enter_or_extend_import_mode_regions_in_range(key_range.clone(), &region_infos);
        for i in 1..=3 {
            assert!(switcher.region_in_import_mode(i));
        }
        for i in 4..=5 {
            assert!(!switcher.region_in_import_mode(i));
        }
        assert!(switcher.is_regional_import.load(Ordering::Acquire));

        let mut key_range2 = KeyRange::default();
        // region 3 4 5 should be included
        key_range2.set_start_key(b"k29".to_vec());
        key_range2.set_end_key(b"".to_vec());
        switcher.enter_or_extend_import_mode_regions_in_range(key_range2.clone(), &region_infos);
        for i in 1..=5 {
            assert!(switcher.region_in_import_mode(i));
        }

        switcher.clear_import_mode_regions_in_range(key_range);
        for i in 1..=2 {
            assert!(!switcher.region_in_import_mode(i));
        }
        for i in 3..5 {
            assert!(switcher.region_in_import_mode(i));
        }
        assert!(switcher.is_regional_import.load(Ordering::Acquire));

        // Mock that region 3 is spliited to region 3 and region 6
        let mut region = Region::default();
        region.set_id(3);
        region.set_start_key(b"k20".to_vec());
        region.set_end_key(b"k25".to_vec());
        region_infos.insert(3, (region, true));
        let mut region = Region::default();
        region.set_id(6);
        region.set_start_key(b"k25".to_vec());
        region.set_end_key(b"k30".to_vec());
        region_infos.insert(6, (region, true));
        // use key_range2 again
        switcher.enter_or_extend_import_mode_regions_in_range(key_range2.clone(), &region_infos);
        for i in 4..=6 {
            assert!(switcher.region_in_import_mode(i));
        }

        switcher.clear_import_mode_regions_in_range(key_range2);
        for i in 4..=6 {
            assert!(!switcher.region_in_import_mode(i));
        }
        assert!(!switcher.is_regional_import.load(Ordering::Acquire));
    }

    #[test]
    fn test_import_mode_timeout() {
        let cfg = Config {
            import_mode_timeout: ReadableDuration::millis(300),
            ..Config::default()
        };

        let threads = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let switcher = ImportModeSwitcherV2::new(&cfg);
        let mut region_infos: HashMap<u64, _> = HashMap::default();
        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(b"k1".to_vec());
        region.set_end_key(b"k3".to_vec());
        region_infos.insert(1, (region, true));
        let mut region = Region::default();
        region.set_id(2);
        region.set_start_key(b"k3".to_vec());
        region.set_end_key(b"k5".to_vec());
        region_infos.insert(2, (region, true));

        let mut key_range = KeyRange::default();
        key_range.set_start_key(b"k2".to_vec());
        key_range.set_end_key(b"k4".to_vec());
        switcher.enter_or_extend_import_mode_regions_in_range(key_range, &region_infos);
        assert!(switcher.region_in_import_mode(1));
        assert!(switcher.region_in_import_mode(2));

        switcher.start(threads.handle());

        thread::sleep(Duration::from_secs(1));
        threads.block_on(tokio::task::yield_now());

        let mut key_range = KeyRange::default();
        key_range.set_start_key(b"k4".to_vec());
        key_range.set_end_key(b"k5".to_vec());
        switcher.enter_or_extend_import_mode_regions_in_range(key_range, &region_infos);

        assert!(!switcher.region_in_import_mode(1));
        assert!(switcher.region_in_import_mode(2));
        assert!(switcher.is_regional_import.load(Ordering::Acquire));

        thread::sleep(Duration::from_secs(1));
        threads.block_on(tokio::task::yield_now());
        assert!(!switcher.region_in_import_mode(2));

        assert!(!switcher.is_regional_import.load(Ordering::Acquire));
    }
}
