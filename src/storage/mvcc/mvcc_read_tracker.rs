// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;

use crate::server::gc_worker::GcWorkerConfigManager;

/// Global MVCC read tracker instance
pub static MVCC_READ_TRACKER: OnceLock<MvccReadTracker> = OnceLock::new();

/// Initialize the global MVCC read tracker with config manager
pub fn init_mvcc_read_tracker(cfg_tracker: GcWorkerConfigManager) {
    let _ = MVCC_READ_TRACKER.set(MvccReadTracker::new(cfg_tracker));
}

/// Simple atomic counter for tracking MVCC versions scanned per region
#[derive(Debug)]
pub struct RegionMvccReadStats {
    /// Total MVCC versions scanned (atomic for lock-free updates)
    redundant_versions_scanned: AtomicU64,
    /// Total number of read requests (atomic for lock-free updates)
    total_requests: AtomicU64,
}

impl RegionMvccReadStats {
    pub fn new() -> Self {
        Self {
            redundant_versions_scanned: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
        }
    }

    /// Add MVCC versions scanned and increment request counter (lock-free
    /// atomic increment)
    fn add_mvcc_versions(&self, mvcc_versions: u64) {
        self.redundant_versions_scanned
            .fetch_add(mvcc_versions, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total MVCC versions scanned in current window
    pub fn get_total_mvcc_versions(&self) -> u64 {
        self.redundant_versions_scanned.load(Ordering::Relaxed)
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
    /// Timestamp (in seconds since epoch) when stats were last reset
    reset_time_secs: Arc<AtomicU64>,
    /// Config tracker for accessing mvcc_scan_threshold
    cfg_tracker: GcWorkerConfigManager,
}

impl MvccReadTracker {
    pub fn new(cfg_tracker: GcWorkerConfigManager) -> Self {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            stats: Arc::new(DashMap::new()),
            reset_time_secs: Arc::new(AtomicU64::new(now_secs)),
            cfg_tracker,
        }
    }

    /// Reset stats unconditionally
    /// Should be called periodically by the compaction runner
    pub fn reset_if_needed(&self) {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Clear all stats (reset the window)
        self.stats.clear();

        // Update reset time
        self.reset_time_secs.store(now_secs, Ordering::Relaxed);
    }

    /// Record a read operation for a region
    /// Only records stats when mvcc_versions_scanned exceeds
    /// mvcc_scan_threshold
    pub fn record_read(&self, region_id: u64, mvcc_versions_scanned: u64) {
        let threshold = self.cfg_tracker.value().auto_compaction.mvcc_scan_threshold;
        if mvcc_versions_scanned <= threshold {
            return;
        }

        // Lock-free: DashMap handles concurrency internally
        self.stats
            .entry(region_id)
            .or_default()
            .add_mvcc_versions(mvcc_versions_scanned);
    }

    /// Get MVCC versions scanned per second for a region (for compaction
    /// scoring).
    ///
    /// Returns total MVCC throughput: total_versions / elapsed_time.
    pub fn get_mvcc_versions_scanned(&self, region_id: u64) -> u64 {
        self.stats
            .get(&region_id)
            .map(|stats| {
                // Read counters without resetting
                let total_versions = stats.redundant_versions_scanned.load(Ordering::Relaxed);

                if total_versions == 0 {
                    return 0;
                }

                // Get current time
                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                // Calculate elapsed time since last reset
                let start_time_secs = self.reset_time_secs.load(Ordering::Relaxed);
                let elapsed_secs = now_secs.saturating_sub(start_time_secs);

                if elapsed_secs == 0 {
                    return 0;
                }

                // Calculate throughput: total_mvcc_versions / elapsed_time
                // This naturally combines:
                // - Read intensity: regions with high mvcc/req contribute more total_versions
                // - Read frequency: regions with high req/sec accumulate more total_versions
                total_versions / elapsed_secs
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use tikv_util::config::VersionTrack;

    use super::*;
    use crate::server::gc_worker::GcConfig;

    fn create_test_config_manager_with_threshold(threshold: u64) -> GcWorkerConfigManager {
        let mut config = GcConfig::default();
        config.auto_compaction.mvcc_scan_threshold = threshold;
        GcWorkerConfigManager(Arc::new(VersionTrack::new(config)), None)
    }

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
        // Use threshold of 0 so all reads are recorded
        let cfg_manager = create_test_config_manager_with_threshold(0);
        let tracker = MvccReadTracker::new(cfg_manager);
        tracker.clear();

        // Record reads for multiple regions
        let region1 = 1001;
        let region2 = 1002;

        tracker.record_read(region1, 2000); // 2000 total MVCC versions
        tracker.record_read(region2, 1000); // 1000 total MVCC versions

        assert_eq!(tracker.tracked_region_count(), 2);

        // Sleep to ensure elapsed time > 0
        thread::sleep(Duration::from_secs(2));

        // Check throughput (total mvcc versions per second)
        // region1: 2000 versions / 2 secs = 1000 versions/sec
        // region2: 1000 versions / 2 secs = 500 versions/sec
        let rate1 = tracker.get_mvcc_versions_scanned(region1);
        let rate2 = tracker.get_mvcc_versions_scanned(region2);

        // Allow some tolerance for timing
        assert!((900..=1100).contains(&rate1), "rate1 = {}", rate1);
        assert!((400..=600).contains(&rate2), "rate2 = {}", rate2);

        // Counters are not reset anymore, so same values should be returned
        let rate1_again = tracker.get_mvcc_versions_scanned(region1);
        assert!(
            (900..=1100).contains(&rate1_again),
            "rate1_again = {}",
            rate1_again
        );

        // Non-existent region
        assert_eq!(tracker.get_mvcc_versions_scanned(9999), 0);
    }

    #[test]
    fn test_accumulation() {
        // Use threshold of 0 so all reads are recorded
        let cfg_manager = create_test_config_manager_with_threshold(0);
        let tracker = MvccReadTracker::new(cfg_manager);
        tracker.clear();

        let region = 2001;

        // Record multiple times - should accumulate both versions and requests
        tracker.record_read(region, 50); // 50 versions
        tracker.record_read(region, 100); // 100 versions
        tracker.record_read(region, 200); // 200 versions
        tracker.record_read(region, 150); // 150 versions

        // Verify accumulation (total should be 500 versions, 4 requests)
        // Use a block to ensure the Ref is dropped before the sleep
        {
            let stats = tracker.stats.get(&region).unwrap();
            assert_eq!(stats.get_total_mvcc_versions(), 500);
            assert_eq!(stats.get_total_requests(), 4);
        }

        // Sleep to ensure elapsed time > 0
        thread::sleep(Duration::from_secs(2));

        // Throughput = 500 versions / 2 secs = 250 versions/sec
        let rate = tracker.get_mvcc_versions_scanned(region);
        assert!((225..=275).contains(&rate), "rate = {}", rate);

        // Counters are not reset anymore, so values should remain
        {
            let stats = tracker.stats.get(&region).unwrap();
            assert_eq!(stats.get_total_mvcc_versions(), 500);
            assert_eq!(stats.get_total_requests(), 4);
        }

        // Add more reads
        tracker.record_read(region, 100); // 100 total versions
        thread::sleep(Duration::from_secs(1));

        // Now we have 600 versions total over ~3 secs = ~200 versions/sec
        let rate2 = tracker.get_mvcc_versions_scanned(region);
        assert!((150..=250).contains(&rate2), "rate2 = {}", rate2);
    }

    #[test]
    fn test_clear_stats() {
        // Use threshold of 0 so all reads are recorded
        let cfg_manager = create_test_config_manager_with_threshold(0);
        let tracker = MvccReadTracker::new(cfg_manager);
        tracker.clear();

        tracker.record_read(1, 100);
        tracker.record_read(2, 200);
        assert_eq!(tracker.tracked_region_count(), 2);

        // Clear all stats
        tracker.clear();
        assert_eq!(tracker.tracked_region_count(), 0);
    }

    #[test]
    fn test_threshold_filtering() {
        // Use threshold of 500 - only reads above this should be recorded
        let cfg_manager = create_test_config_manager_with_threshold(500);
        let tracker = MvccReadTracker::new(cfg_manager);
        tracker.clear();

        let region = 3001;

        // These should NOT be recorded (below or equal to threshold)
        tracker.record_read(region, 100);
        tracker.record_read(region, 500);
        assert_eq!(tracker.tracked_region_count(), 0);

        // This should be recorded (above threshold)
        tracker.record_read(region, 501);
        assert_eq!(tracker.tracked_region_count(), 1);

        let stats = tracker.stats.get(&region).unwrap();
        assert_eq!(stats.get_total_mvcc_versions(), 501);
        assert_eq!(stats.get_total_requests(), 1);
    }
}
