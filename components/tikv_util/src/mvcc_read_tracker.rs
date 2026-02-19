// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

//! MVCC Read Tracker for load-based compaction prioritization.
//!
//! Tracks MVCC read activity per region to help prioritize compaction
//! for regions with high read overhead from scanning multiple versions.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;

/// Global MVCC read tracker instance
pub static MVCC_READ_TRACKER: OnceLock<MvccReadTracker> = OnceLock::new();

/// Initialize the global MVCC read tracker
pub fn init_mvcc_read_tracker() {
    let _ = MVCC_READ_TRACKER.set(MvccReadTracker::new());
}

/// Atomic counter for tracking MVCC versions scanned per region
#[derive(Debug)]
pub struct RegionMvccReadStats {
    redundant_versions_scanned: AtomicU64,
    total_requests: AtomicU64,
}

impl RegionMvccReadStats {
    pub fn new() -> Self {
        Self {
            redundant_versions_scanned: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
        }
    }

    fn add_mvcc_versions(&self, mvcc_versions: u64) {
        self.redundant_versions_scanned
            .fetch_add(mvcc_versions, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_total_mvcc_versions(&self) -> u64 {
        self.redundant_versions_scanned.load(Ordering::Relaxed)
    }

    pub fn get_total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }
}

impl Default for RegionMvccReadStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Global tracker for MVCC read activity across all regions.
/// Keyed by region_id for easy lookup during compaction checks.
#[derive(Clone)]
pub struct MvccReadTracker {
    stats: Arc<DashMap<u64, RegionMvccReadStats>>,
    reset_time_secs: Arc<AtomicU64>,
    // Config values stored as atomics for lock-free access
    enabled: Arc<AtomicBool>,
    mvcc_scan_threshold: Arc<AtomicU64>,
    mvcc_read_weight: Arc<AtomicU64>, // Stored as bits representation of f64
}

impl MvccReadTracker {
    pub fn new() -> Self {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            stats: Arc::new(DashMap::new()),
            reset_time_secs: Arc::new(AtomicU64::new(now_secs)),
            enabled: Arc::new(AtomicBool::new(false)),
            mvcc_scan_threshold: Arc::new(AtomicU64::new(1000)),
            mvcc_read_weight: Arc::new(AtomicU64::new(3.0f64.to_bits())),
        }
    }

    /// Update config values. Called by GcWorkerConfigManager when config
    /// changes.
    pub fn update_config(&self, enabled: bool, threshold: u64, weight: f64) {
        self.enabled.store(enabled, Ordering::Relaxed);
        self.mvcc_scan_threshold.store(threshold, Ordering::Relaxed);
        self.mvcc_read_weight
            .store(weight.to_bits(), Ordering::Relaxed);
    }

    /// Reset stats - call periodically after compaction checks
    pub fn reset_stats(&self) {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.stats.clear();
        self.reset_time_secs.store(now_secs, Ordering::Relaxed);
    }

    /// Record a read operation for a region.
    /// Only records when mvcc_versions_scanned exceeds threshold.
    pub fn record_read(&self, region_id: u64, mvcc_versions_scanned: u64) {
        let threshold = self.mvcc_scan_threshold.load(Ordering::Relaxed);
        if mvcc_versions_scanned <= threshold {
            return;
        }
        self.stats
            .entry(region_id)
            .or_default()
            .add_mvcc_versions(mvcc_versions_scanned);
    }

    /// Get MVCC versions scanned per second for a region
    pub fn get_mvcc_versions_scanned(&self, region_id: u64) -> u64 {
        self.stats
            .get(&region_id)
            .map(|stats| {
                let total_versions = stats.redundant_versions_scanned.load(Ordering::Relaxed);
                if total_versions == 0 {
                    return 0;
                }

                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let start_time_secs = self.reset_time_secs.load(Ordering::Relaxed);
                let elapsed_secs = now_secs.saturating_sub(start_time_secs);

                if elapsed_secs == 0 {
                    return 0;
                }
                total_versions / elapsed_secs
            })
            .unwrap_or(0)
    }

    /// Check if MVCC read aware compaction is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Get the MVCC read weight for scoring
    pub fn get_mvcc_read_weight(&self) -> f64 {
        f64::from_bits(self.mvcc_read_weight.load(Ordering::Relaxed))
    }

    pub fn tracked_region_count(&self) -> usize {
        self.stats.len()
    }
}

impl Default for MvccReadTracker {
    fn default() -> Self {
        Self::new()
    }
}
