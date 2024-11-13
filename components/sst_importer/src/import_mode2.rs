// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use collections::{HashMap, HashSet};
use futures_util::compat::Future01CompatExt;
use kvproto::{import_sstpb::Range, metapb::Region};
use tikv_util::{resizable_threadpool::RuntimeHandle, timer::GLOBAL_TIMER_HANDLE};

use super::Config;

#[derive(PartialEq, Eq, Hash, Clone)]
// implement hash so that it can be a key in HashMap
pub struct HashRange {
    pub start_key: std::vec::Vec<u8>,
    pub end_key: std::vec::Vec<u8>,
}

impl From<Range> for HashRange {
    fn from(key_range: Range) -> Self {
        Self {
            start_key: key_range.start,
            end_key: key_range.end,
        }
    }
}

struct ImportModeSwitcherInnerV2 {
    timeout: Duration,
    // range in import mode -> timeout to restore to normal mode
    import_mode_ranges: HashMap<HashRange, Instant>,
}

impl ImportModeSwitcherInnerV2 {
    fn clear_import_mode_range(&mut self, range: HashRange) {
        self.import_mode_ranges.remove(&range);
    }
}

#[derive(Clone)]
pub struct ImportModeSwitcherV2 {
    inner: Arc<Mutex<ImportModeSwitcherInnerV2>>,
}

impl ImportModeSwitcherV2 {
    pub fn new(cfg: &Config) -> ImportModeSwitcherV2 {
        let timeout = cfg.import_mode_timeout.0;
        let inner = Arc::new(Mutex::new(ImportModeSwitcherInnerV2 {
            timeout,
            import_mode_ranges: HashMap::default(),
        }));
        ImportModeSwitcherV2 { inner }
    }

    pub fn start_resizable_threads(&self, executor: &RuntimeHandle) {
        // spawn a background future to put regions back into normal mode after timeout
        let inner = self.inner.clone();
        let switcher = Arc::downgrade(&inner);
        let timer_loop = async move {
            let mut prev_ranges = vec![];
            // loop until the switcher has been dropped
            while let Some(switcher) = switcher.upgrade() {
                let next_check = {
                    let now = Instant::now();
                    let mut switcher = switcher.lock().unwrap();
                    for range in prev_ranges.drain(..) {
                        if let Some(next_check) = switcher.import_mode_ranges.get(&range) {
                            if now >= *next_check {
                                switcher.clear_import_mode_range(range);
                            }
                        }
                    }

                    let mut min_next_check = now + switcher.timeout;
                    for (range, next_check) in &switcher.import_mode_ranges {
                        if *next_check <= min_next_check {
                            if *next_check < min_next_check {
                                min_next_check = *next_check;
                                prev_ranges.clear();
                            }
                            prev_ranges.push(range.clone());
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

    pub fn ranges_enter_import_mode(&self, ranges: Vec<Range>) {
        let mut inner = self.inner.lock().unwrap();
        let next_check = Instant::now() + inner.timeout;
        for range in ranges {
            let range = HashRange::from(range);
            // if the range exists before, the timeout is updated
            inner.import_mode_ranges.insert(range, next_check);
        }
    }

    pub fn clear_import_mode_range(&self, ranges: Vec<Range>) {
        let mut inner = self.inner.lock().unwrap();
        for range in ranges {
            let range = HashRange::from(range);
            inner.clear_import_mode_range(range);
        }
    }

    pub fn region_in_import_mode(&self, region: &Region) -> bool {
        let inner = self.inner.lock().unwrap();
        for r in inner.import_mode_ranges.keys() {
            if region_overlap_with_range(r, region) {
                return true;
            }
        }
        false
    }

    pub fn range_in_import_mode(&self, range: &Range) -> bool {
        let inner = self.inner.lock().unwrap();
        for r in inner.import_mode_ranges.keys() {
            if range_overlaps(r, range) {
                return true;
            }
        }
        false
    }

    pub fn ranges_in_import(&self) -> HashSet<HashRange> {
        let inner = self.inner.lock().unwrap();
        HashSet::from_iter(inner.import_mode_ranges.keys().cloned())
    }
}

fn region_overlap_with_range(range: &HashRange, region: &Region) -> bool {
    (region.end_key.is_empty() || range.start_key < region.end_key)
        && (range.end_key.is_empty() || region.start_key < range.end_key)
}

pub fn range_overlaps(range1: &HashRange, range2: &Range) -> bool {
    (range2.end.is_empty() || range1.start_key < range2.end)
        && (range1.end_key.is_empty() || range2.start < range1.end_key)
}

#[cfg(test)]
mod test {
    use std::thread;

    use tikv_util::{config::ReadableDuration, resizable_threadpool::ResizableRuntime};
    use tokio::runtime::Runtime;

    use super::*;

    type TokioResult<T> = std::io::Result<T>;

    fn create_tokio_runtime(_: usize, _: &str) -> TokioResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
    }

    #[test]
    fn test_region_range_overlaps() {
        let verify_overlap = |ranges1: &[(&str, &str)], ranges2: &[(&str, &str)], overlap: bool| {
            for r in ranges1 {
                let hash_range = HashRange {
                    start_key: r.0.as_bytes().to_vec(),
                    end_key: r.1.as_bytes().to_vec(),
                };

                for r2 in ranges2 {
                    let mut region = Region::default();
                    region.set_start_key(r2.0.as_bytes().to_vec());
                    region.set_end_key(r2.1.as_bytes().to_vec());

                    if overlap {
                        assert!(region_overlap_with_range(&hash_range, &region));
                    } else {
                        assert!(!region_overlap_with_range(&hash_range, &region));
                    }

                    let mut range = Range::default();
                    range.set_start(r2.0.as_bytes().to_vec());
                    range.set_end(r2.1.as_bytes().to_vec());
                    if overlap {
                        assert!(range_overlaps(&hash_range, &range));
                    } else {
                        assert!(!range_overlaps(&hash_range, &range));
                    }
                }
            }
        };

        let ranges1 = vec![("", ""), ("", "k10"), ("k01", ""), ("k01", "k08")];
        let ranges2 = vec![("", ""), ("k02", "k07"), ("k07", "k11"), ("k07", "")];
        verify_overlap(&ranges1, &ranges2, true);
        verify_overlap(&ranges2, &ranges1, true);

        let ranges1 = vec![("k10", "k20")];
        let ranges2 = vec![("", "k10"), ("k20", "k30"), ("k20", "")];
        verify_overlap(&ranges1, &ranges2, false);
        verify_overlap(&ranges2, &ranges1, false);
    }

    #[test]
    fn test_region_import_mode() {
        let cfg = Config::default();
        let switcher = ImportModeSwitcherV2::new(&cfg);
        let mut regions = vec![];
        for i in 1..=5 {
            let mut region = Region::default();
            region.set_id(i);
            region.set_start_key(format!("k{:02}", (i - 1) * 10).into());
            region.set_end_key(format!("k{:02}", i * 10).into());
            regions.push(region);
        }

        let mut key_range = Range::default();
        key_range.set_end(b"j".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range.clone()]);
        // no regions should be set in import mode
        for i in 1..=5 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }

        let mut r = Range::default();
        r.set_end(b"k".to_vec());
        assert!(switcher.range_in_import_mode(&r));

        // region 1 2 3 should be included
        key_range.set_start(b"k09".to_vec());
        key_range.set_end(b"k21".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range.clone()]);
        for i in 1..=3 {
            assert!(switcher.region_in_import_mode(&regions[i - 1]));
        }
        for i in 4..=5 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }

        let mut key_range2 = Range::default();
        // region 3 4 5 should be included
        key_range2.set_start(b"k29".to_vec());
        key_range2.set_end(b"".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range2.clone()]);
        for i in 1..=5 {
            assert!(switcher.region_in_import_mode(&regions[i - 1]));
        }

        switcher.clear_import_mode_range(vec![key_range]);
        for i in 1..=2 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }
        for i in 3..5 {
            assert!(switcher.region_in_import_mode(&regions[i - 1]));
        }

        switcher.clear_import_mode_range(vec![key_range2]);
        for i in 3..=5 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }
    }

    #[test]
    fn test_import_mode_timeout() {
        let cfg = Config {
            import_mode_timeout: ReadableDuration::millis(700),
            ..Config::default()
        };

        let threads =
            ResizableRuntime::new(4, "test", Box::new(create_tokio_runtime), Box::new(|_| {}));
        let handle = threads.handle();

        let switcher = ImportModeSwitcherV2::new(&cfg);
        let mut region = Region::default();
        region.set_id(1);
        region.set_start_key(b"k1".to_vec());
        region.set_end_key(b"k3".to_vec());
        let mut region2 = Region::default();
        region2.set_id(2);
        region2.set_start_key(b"k3".to_vec());
        region2.set_end_key(b"k5".to_vec());
        let mut region3 = Region::default();
        region3.set_id(3);
        region3.set_start_key(b"k5".to_vec());
        region3.set_end_key(b"k7".to_vec());

        let mut key_range = Range::default();
        key_range.set_start(b"k2".to_vec());
        key_range.set_end(b"k4".to_vec());
        let mut key_range2 = Range::default();
        key_range2.set_start(b"k5".to_vec());
        key_range2.set_end(b"k8".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range, key_range2.clone()]);
        assert!(switcher.region_in_import_mode(&region));
        assert!(switcher.region_in_import_mode(&region2));
        assert!(switcher.region_in_import_mode(&region3));

        switcher.start_resizable_threads(&handle);

        thread::sleep(Duration::from_millis(400));
        // renew the timeout of key_range2
        switcher.ranges_enter_import_mode(vec![key_range2]);
        thread::sleep(Duration::from_millis(400));

        handle.block_on(tokio::task::yield_now());

        // the range covering region and region2 should be cleared due to timeout.
        // assert!(!switcher.region_in_import_mode(&region));
        // assert!(!switcher.region_in_import_mode(&region2));
        assert!(switcher.region_in_import_mode(&region3));

        thread::sleep(Duration::from_millis(400));
        handle.block_on(tokio::task::yield_now());
        assert!(!switcher.region_in_import_mode(&region3));
    }
}
