// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};

use dashmap::DashMap;
use lazy_static::lazy_static;

/// Default time window for MVCC read tracking (in seconds)
const DEFAULT_MVCC_READ_WINDOW_SECS: u64 = 300; // same as compaction check interval

lazy_static! {
    pub static ref MVCC_READ_TRACKER: MvccReadTracker = MvccReadTracker::new();
}

/// Simple atomic counter for tracking MVCC versions scanned per region
#[derive(Debug)]
pub struct RegionMvccReadStats {
    /// Total MVCC versions scanned (atomic for lock-free updates)
    total_mvcc_versions_scanned: AtomicU64,
    /// Total number of read requests (atomic for lock-free updates)
    total_requests: AtomicU64,
}

impl RegionMvccReadStats {
    pub fn new() -> Self {
        Self {
            total_mvcc_versions_scanned: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
        }
    }

    /// Add MVCC versions scanned and increment request counter (lock-free
    /// atomic increment)
    fn add_mvcc_versions(&self, mvcc_versions: u64) {
        self.total_mvcc_versions_scanned
            .fetch_add(mvcc_versions, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total MVCC versions scanned in current window
    pub fn get_total_mvcc_versions(&self) -> u64 {
        self.total_mvcc_versions_scanned.load(Ordering::Relaxed)
    }

    /// Get total number of requests in current window
    pub fn get_total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }
}

impl Default for RegionMvccReadStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Global tracker for MVCC read activity across all regions (lock-free with
/// DashMap)
#[derive(Clone)]
pub struct MvccReadTracker {
    /// Lock-free concurrent map from region_id to read statistics
    stats: Arc<DashMap<u64, RegionMvccReadStats>>,
    /// Flag to stop background reset thread
    stop_flag: Arc<AtomicBool>,
    /// Time window in seconds
    window_secs: u64,
}

impl MvccReadTracker {
    pub fn new() -> Self {
        Self::with_window_secs(DEFAULT_MVCC_READ_WINDOW_SECS)
    }

    pub fn with_window_secs(window_secs: u64) -> Self {
        let tracker = Self {
            stats: Arc::new(DashMap::new()),
            stop_flag: Arc::new(AtomicBool::new(false)),
            window_secs,
        };

        // Start background thread to reset stats every window
        tracker.start_background_reset_thread();

        tracker
    }

    /// Start a background thread that clears all stats every window
    fn start_background_reset_thread(&self) {
        let stats = Arc::clone(&self.stats);
        let stop_flag = Arc::clone(&self.stop_flag);
        let window_secs = self.window_secs;

        thread::Builder::new()
            .name("mvcc-tracker-reset".to_string())
            .spawn(move || {
                while !stop_flag.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(window_secs));

                    // Clear all stats (reset the window)
                    stats.clear();
                }
            })
            .expect("Failed to spawn MVCC tracker reset thread");
    }

    /// Record a read operation for a region
    pub fn record_read(&self, region_id: u64, mvcc_versions_scanned: u64) {
        if mvcc_versions_scanned == 0 {
            return;
        }

        // Lock-free: DashMap handles concurrency internally
        self.stats
            .entry(region_id)
            .or_insert_with(RegionMvccReadStats::new)
            .add_mvcc_versions(mvcc_versions_scanned);
    }

    /// Get average MVCC versions scanned per request for a region (for
    /// compaction scoring)
    pub fn get_mvcc_versions_scanned(&self, region_id: u64) -> u64 {
        self.stats
            .get(&region_id)
            .map(|stats| {
                let total_versions = stats.get_total_mvcc_versions();
                let total_requests = stats.get_total_requests();

                if total_requests == 0 {
                    return 0;
                }

                // Calculate average versions per request
                total_versions / total_requests
            })
            .unwrap_or(0)
    }

    /// Get total number of tracked regions
    pub fn tracked_region_count(&self) -> usize {
        self.stats.len()
    }

    /// Clear all statistics (useful for testing)
    #[cfg(test)]
    pub fn clear(&self) {
        self.stats.clear();
    }
}

impl Drop for MvccReadTracker {
    fn drop(&mut self) {
        // Signal background thread to stop
        self.stop_flag.store(true, Ordering::Relaxed);
    }
}

impl Default for MvccReadTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_mvcc_read_stats() {
        let stats = RegionMvccReadStats::new();
        assert_eq!(stats.get_total_mvcc_versions(), 0);

        // Add different values (accumulates atomically)
        stats.add_mvcc_versions(5);
        assert_eq!(stats.get_total_mvcc_versions(), 5);

        stats.add_mvcc_versions(15);
        assert_eq!(stats.get_total_mvcc_versions(), 20); // 5 + 15

        stats.add_mvcc_versions(10);
        assert_eq!(stats.get_total_mvcc_versions(), 30); // 20 + 10
    }

    #[test]
    fn test_mvcc_read_tracker() {
        let tracker = MvccReadTracker::with_window_secs(600);
        tracker.clear();

        // Record reads for multiple regions
        let region1 = 1001;
        let region2 = 1002;

        tracker.record_read(region1, 2000); // 1 request with 2000 versions
        tracker.record_read(region2, 1000); // 1 request with 1000 versions

        assert_eq!(tracker.tracked_region_count(), 2);

        // Check per-request averages (1 request each, so avg = total)
        let avg1 = tracker.get_mvcc_versions_scanned(region1);
        let avg2 = tracker.get_mvcc_versions_scanned(region2);
        assert_eq!(avg1, 2000); // 2000 versions / 1 request
        assert_eq!(avg2, 1000); // 1000 versions / 1 request

        // Non-existent region
        assert_eq!(tracker.get_mvcc_versions_scanned(9999), 0);
    }

    #[test]
    fn test_accumulation() {
        let tracker = MvccReadTracker::with_window_secs(10);
        tracker.clear();

        let region = 2001;

        // Record multiple times - should accumulate both versions and requests
        tracker.record_read(region, 50); // Request 1: 50 versions
        tracker.record_read(region, 100); // Request 2: 100 versions
        tracker.record_read(region, 200); // Request 3: 200 versions
        tracker.record_read(region, 150); // Request 4: 150 versions

        // Verify accumulation (total should be 500 versions, 4 requests)
        let stats = tracker.stats.get(&region).unwrap();
        assert_eq!(stats.get_total_mvcc_versions(), 500);
        assert_eq!(stats.get_total_requests(), 4);

        // Average should be 500 / 4 = 125 versions per request
        let avg = tracker.get_mvcc_versions_scanned(region);
        assert_eq!(avg, 125); // 500 versions / 4 requests
    }

    #[test]
    fn test_clear_stats() {
        let tracker = MvccReadTracker::with_window_secs(600);
        tracker.clear();

        tracker.record_read(1, 100);
        tracker.record_read(2, 200);
        assert_eq!(tracker.tracked_region_count(), 2);

        // Clear all stats
        tracker.clear();
        assert_eq!(tracker.tracked_region_count(), 0);
    }
}
