// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.
//! TrackedArc - A smart pointer for KeyHandle memory leak diagnosis
//!
//! This module provides TrackedArc<KeyHandle>, a wrapper around Arc<KeyHandle>
//! that tracks detailed diagnostic metadata for each instance. It's designed
//! specifically for debugging memory leaks in KeyHandle objects in production
//! environments.

use std::{
    collections::VecDeque,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use backtrace::Backtrace;
use dashmap::DashMap;
use lazy_static::lazy_static;
use tikv_util::{info, sys::thread::StdThreadBuildWrapper, warn};
use txn_types::Key;

use crate::{
    metrics::{TRACKED_ARC_CAPACITY_EXCEEDED, TRACKED_ARC_RECORD_NOT_FOUND},
    KeyHandle,
};

const MAX_HISTORY_SIZE: usize = 10;

/// Operation types for TrackedArc access tracking
#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    /// Prewrite operation
    Prewrite { start_ts: u64, for_update_ts: u64 },
    /// Commit operation
    Commit { start_ts: u64, commit_ts: u64 },
    /// Raw key guard for CDC/causal consistency
    RawKeyGuard { ts: u64 },
    /// Test operation
    Test,
    /// Clone operation (from perspective)
    CloneFrom,
    /// Clone operation (to perspective)
    CloneTo,
    /// Weak reference upgrade
    Upgrade,
    /// TrackedArc drop
    Drop,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Prewrite {
                start_ts,
                for_update_ts,
            } => {
                write!(
                    f,
                    "prewrite_start_ts_{}_for_update_ts_{}",
                    start_ts, for_update_ts
                )
            }
            Operation::Commit {
                start_ts,
                commit_ts,
            } => {
                write!(f, "commit_start_ts_{}_commit_ts_{}", start_ts, commit_ts)
            }
            Operation::RawKeyGuard { ts } => write!(f, "raw_key_guard_ts_{}", ts),
            Operation::Test => write!(f, "test"),
            Operation::CloneFrom => write!(f, "clone_from"),
            Operation::CloneTo => write!(f, "clone_to"),
            Operation::Upgrade => write!(f, "upgrade"),
            Operation::Drop => write!(f, "drop"),
        }
    }
}

/// Helper function to get current thread name with fallback to thread ID
fn current_thread_name() -> String {
    std::thread::current()
        .name()
        .map(|name| name.to_string())
        .unwrap_or_else(|| format!("thread-{:?}", std::thread::current().id()))
}

/// Access information for a specific TrackedArc instance
#[derive(Clone)]
pub struct AccessInfo {
    /// Unique ID for the TrackedArc instance that performed this access
    /// (per-KeyHandle)
    pub arc_id: u32,
    /// The operation that was performed
    pub operation: Operation,
    /// When this access occurred
    pub timestamp: Instant,
    /// Where this access occurred in the code
    pub backtrace: Backtrace,
    /// Which thread performed this access
    pub thread_name: String,
}

/// Diagnostic metadata for a tracked KeyHandle
/// Tracks the complete access history across all TrackedArc instances
pub struct TrackedInfo {
    /// A unique ID for this KeyHandle (shared across all TrackedArc instances)
    pub key_handle_id: u64,

    /// The raw Key object for precise identification
    pub key: Key,

    /// Creation timestamp - never changes, used for leak detection age
    /// calculation
    pub creation_time: Instant,

    /// Complete access history across ALL TrackedArc instances of this
    /// KeyHandle This is a bounded queue to prevent unbounded memory growth
    pub access_history: Mutex<std::collections::VecDeque<AccessInfo>>,
}

/// Global, thread-safe singleton registry for all active TrackedInfo objects
pub struct LeakDetector {
    /// Core registry using DashMap for high-performance concurrent access
    registry: DashMap<u64, Arc<TrackedInfo>>,
    /// Atomic counter for generating unique KeyHandle IDs
    next_keyhandle_id: AtomicU64,
    /// Sampling rate (1 = always track, N = track every Nth instance)
    sampling_rate: AtomicU32,
    /// Counter for sampling decisions
    sampling_counter: AtomicU32,
    /// Background leak detection enabled
    leak_detection_enabled: AtomicBool,
    /// Minimum age for considering a KeyHandle as potentially leaked (in
    /// seconds)
    leak_detection_threshold: AtomicU64,
    /// Maximum number of tracked KeyHandles (to prevent unbounded memory
    /// growth)
    max_registry_capacity: AtomicU64,
    /// Maximum number of leaked KeyHandles to log per check (to prevent log
    /// spam)
    max_leaks_to_log: AtomicU64,
}

impl LeakDetector {
    /// Create a new LeakDetector instance
    fn new() -> Self {
        let detector = Self {
            registry: DashMap::new(),
            next_keyhandle_id: AtomicU64::new(1),
            sampling_rate: AtomicU32::new(1),
            sampling_counter: AtomicU32::new(0),
            leak_detection_enabled: AtomicBool::new(false),
            leak_detection_threshold: AtomicU64::new(180),
            max_registry_capacity: AtomicU64::new(10000),
            max_leaks_to_log: AtomicU64::new(2),
        };

        // Start background leak detection thread
        detector.start_leak_detection_thread();
        detector
    }

    /// Get the global singleton instance
    pub fn instance() -> &'static LeakDetector {
        &LEAK_DETECTOR
    }

    /// Generate a unique ID for a new KeyHandle
    pub fn generate_keyhandle_id(&self) -> u64 {
        self.next_keyhandle_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Set the sampling rate (1 = always track, N = track every Nth instance)
    pub fn set_sampling_rate(&self, rate: u32) {
        self.sampling_rate.store(rate, Ordering::Relaxed);
        // Reset counter when changing sampling rate
        self.sampling_counter.store(0, Ordering::Relaxed);
    }

    /// Get current registry size for testing
    pub fn registry_size(&self) -> usize {
        self.registry.len()
    }

    /// Enable/disable background leak detection
    pub fn set_leak_detection_enabled(&self, enabled: bool) {
        self.leak_detection_enabled
            .store(enabled, Ordering::Relaxed);
    }

    /// Set the threshold for considering a KeyHandle as potentially leaked (in
    /// seconds)
    pub fn set_leak_detection_threshold(&self, threshold_seconds: u64) {
        self.leak_detection_threshold
            .store(threshold_seconds, Ordering::Relaxed);
    }

    /// Set the maximum registry capacity (to prevent unbounded memory growth)
    pub fn set_max_registry_capacity(&self, max_capacity: u64) {
        self.max_registry_capacity
            .store(max_capacity, Ordering::Relaxed);
    }

    /// Set the maximum number of leaked KeyHandles to log per check (to prevent
    /// log spam)
    pub fn set_max_leaks_to_log(&self, max_leaks: u64) {
        self.max_leaks_to_log.store(max_leaks, Ordering::Relaxed);
    }

    /// Check if this instance should be tracked based on sampling rate
    pub fn should_track(&self) -> bool {
        let rate = self.sampling_rate.load(Ordering::Relaxed);
        if rate == 0 {
            return false; // No tracking when rate is 0
        }
        if rate == 1 {
            return true; // Always track when rate is 1
        }

        let count = self.sampling_counter.fetch_add(1, Ordering::Relaxed);
        count % rate == 0
    }

    /// Register a new TrackedInfo in the registry
    pub fn register(&self, info: Arc<TrackedInfo>) {
        let max_capacity = self.max_registry_capacity.load(Ordering::Relaxed) as usize;

        // Check capacity before inserting
        if self.registry.len() >= max_capacity {
            // Use metric instead of log to avoid spam
            TRACKED_ARC_CAPACITY_EXCEEDED.inc();
            return;
        }

        self.registry.insert(info.key_handle_id, info);
    }

    /// Record an access event for a KeyHandle
    pub fn record_access(&self, key_handle_id: u64, access_info: AccessInfo) {
        if let Some(tracked_info) = self.registry.get(&key_handle_id) {
            if let Ok(mut history) = tracked_info.access_history.lock() {
                history.push_back(access_info);
                if history.len() > MAX_HISTORY_SIZE {
                    history.pop_front();
                }
            }
        } else {
            // This should only happen if there's a bug in the tracking logic
            // since we check is_tracked before calling record_access
            // Use metric instead of log to avoid spam
            TRACKED_ARC_RECORD_NOT_FOUND.inc();
        }
    }

    /// Remove a TrackedInfo from the registry
    pub fn unregister(&self, id: u64) {
        self.registry.remove(&id);
    }

    /// Retrieve a reference to a TrackedInfo by ID
    pub fn get_info(
        &self,
        id: u64,
    ) -> Option<dashmap::mapref::one::Ref<'_, u64, Arc<TrackedInfo>>> {
        self.registry.get(&id)
    }

    /// Get the current number of tracked objects
    pub fn count(&self) -> usize {
        self.registry.len()
    }

    /// Get all tracked object IDs (useful for debugging)
    pub fn get_all_ids(&self) -> Vec<u64> {
        self.registry.iter().map(|entry| *entry.key()).collect()
    }

    /// Get summary information about all tracked objects
    pub fn get_summary(&self) -> Vec<(u64, String, usize)> {
        self.registry
            .iter()
            .map(|entry| {
                let info = entry.value();
                let access_count = info.access_history.lock().map(|h| h.len()).unwrap_or(0);
                (info.key_handle_id, format!("{:?}", info.key), access_count)
            })
            .collect()
    }

    /// Get detailed information about a specific tracked object including
    /// access history
    pub fn get_detailed_info(&self, key_handle_id: u64) -> Option<String> {
        let info_ref = self.get_info(key_handle_id)?;
        let info = info_ref.value();

        let mut result = format!(
            "KeyHandle ID: {}\n\
             Key: {:?}\n",
            info.key_handle_id, info.key
        );

        if let Ok(access_history) = info.access_history.lock() {
            result.push_str(&format!(
                "Access History ({} events):\n",
                access_history.len()
            ));
            for (i, access) in access_history.iter().enumerate() {
                result.push_str(&format!(
                    "  #{}: Arc-{} [-{}ms] {} (thread: {})\n",
                    i + 1,
                    access.arc_id,
                    access.timestamp.elapsed().as_millis(),
                    access.operation,
                    access.thread_name
                ));
            }
        }

        Some(result)
    }

    /// Start the background leak detection thread
    fn start_leak_detection_thread(&self) {
        std::thread::Builder::new()
            .name("leak-detector".to_string())
            .spawn_wrapper(|| {
                let mut loop_counter: u32 = 0;
                loop {
                    std::thread::sleep(Duration::from_secs(60));

                    let detector = LeakDetector::instance();
                    if !detector.leak_detection_enabled.load(Ordering::Relaxed) {
                        continue;
                    }

                    detector.check_for_leaks_internal();
                    loop_counter = loop_counter.wrapping_add(1);
                    // TODO: make this configurable
                    if loop_counter % 10 == 0 {
                        // every some time, we clear the tracked info to get some new data.
                        info!("clear tracked KeyHandles");
                        detector.registry.clear();
                    }
                }
            })
            .expect("Failed to start leak detection thread");
    }

    /// Check for potentially leaked KeyHandles and log them
    fn check_for_leaks_internal(&self) {
        let threshold_secs = self.leak_detection_threshold.load(Ordering::Relaxed);
        let max_leaks_to_log = self.max_leaks_to_log.load(Ordering::Relaxed) as usize;
        let now = Instant::now();

        // Collect all leaked KeyHandles with their ages
        let mut leaked_handles = Vec::new();

        for entry in self.registry.iter() {
            let tracked_info = entry.value();

            // Check if this KeyHandle has been alive longer than threshold
            // Use creation_time instead of first access time for accurate age calculation
            let age = now.duration_since(tracked_info.creation_time);

            if age.as_secs() > threshold_secs {
                leaked_handles.push((tracked_info.clone(), age));
            }
        }

        if leaked_handles.is_empty() {
            return;
        }

        // Sort by age (oldest first)
        leaked_handles.sort_by_key(|(_, age)| *age);

        let total_leaked = leaked_handles.len();
        let max_per_category = max_leaks_to_log / 2; // Split between oldest and newest

        // Log oldest leaked KeyHandles
        let oldest_count = std::cmp::min(max_per_category, total_leaked);
        if oldest_count > 0 {
            warn!("{} OLDEST Leaked KeyHandles", oldest_count);
            for (tracked_info, age) in leaked_handles.iter().take(oldest_count) {
                if let Ok(mut history) = tracked_info.access_history.lock() {
                    warn!(
                        "OLDEST Leak: ID={}, Age={}s, Key={:?}, Access History:\n{}",
                        tracked_info.key_handle_id,
                        age.as_secs(),
                        tracked_info.key,
                        Self::format_access_history_for_leak(&mut history)
                    );
                }
            }
        }

        // Log newest leaked KeyHandles (if we have more than max_per_category)
        if total_leaked > max_per_category {
            let newest_start =
                std::cmp::max(total_leaked.saturating_sub(max_per_category), oldest_count);
            let newest_count = total_leaked - newest_start;

            if newest_count > 0 {
                warn!(
                    "{} NEWEST Leaked KeyHandles (except those rejected by capacity)",
                    newest_count
                );
                for (tracked_info, age) in leaked_handles.iter().skip(newest_start) {
                    if let Ok(mut history) = tracked_info.access_history.lock() {
                        warn!(
                            "NEWEST Leak: ID={}, Age={}s, Key={:?}, Access History:\n{}",
                            tracked_info.key_handle_id,
                            age.as_secs(),
                            tracked_info.key,
                            Self::format_access_history_for_leak(&mut history)
                        );
                    }
                }
            }
        }

        // Summary
        let logged_count = oldest_count
            + if total_leaked > max_per_category {
                total_leaked
                    - std::cmp::max(total_leaked.saturating_sub(max_per_category), oldest_count)
            } else {
                0
            };

        let summary_msg = if total_leaked > max_leaks_to_log {
            format!(
                "Leak detection summary: {} potentially leaked KeyHandles found ({} oldest + {} newest logged, {} middle suppressed) (threshold: {}s, total tracked: {})",
                total_leaked,
                oldest_count,
                logged_count - oldest_count,
                total_leaked - logged_count,
                threshold_secs,
                self.registry.len()
            )
        } else {
            format!(
                "Leak detection summary: {} potentially leaked KeyHandles found (threshold: {}s, total tracked: {})",
                total_leaked,
                threshold_secs,
                self.registry.len()
            )
        };
        warn!("{}", summary_msg);
    }

    /// Format access history for leak logging
    fn format_access_history_for_leak(history: &mut VecDeque<AccessInfo>) -> String {
        let mut result = String::new();
        for (i, access) in history.iter_mut().enumerate() {
            access.backtrace.resolve();
            result.push_str(&format!(
                "  #{}: Arc-{} [-{}ms] {} (thread: {}) (backtrace: {:?})\n",
                i + 1,
                access.arc_id,
                access.timestamp.elapsed().as_millis(),
                access.operation,
                access.thread_name,
                access.backtrace
            ));
        }
        result
    }
}

lazy_static! {
    static ref LEAK_DETECTOR: LeakDetector = LeakDetector::new();
}

/// A smart pointer that wraps Arc<KeyHandle> with diagnostic tracking
/// capabilities
pub struct TrackedArc {
    /// The wrapped Arc<KeyHandle>
    inner: Arc<KeyHandle>,
    /// Unique ID for this TrackedArc instance (per-KeyHandle)
    arc_id: u32,
}

impl TrackedArc {
    /// Create a new TrackedArc with diagnostic tracking
    #[track_caller]
    pub fn new(key_handle: KeyHandle, operation: Operation) -> Self {
        let detector = LeakDetector::instance();
        let arc_id = key_handle.next_arc_id();

        // Get the key_handle_id from the KeyHandle itself
        let key_handle_id = key_handle.key_handle_id;
        let key = key_handle.key.clone();

        // Check if this KeyHandle is being tracked
        if key_handle.is_tracked {
            // Check if we already have TrackedInfo for this KeyHandle
            if !detector.registry.contains_key(&key_handle_id) {
                let tracked_info = Arc::new(TrackedInfo {
                    key_handle_id,
                    key,
                    creation_time: Instant::now(),
                    access_history: Mutex::new(std::collections::VecDeque::new()),
                });
                detector.register(tracked_info);
            }

            // Record creation as the first access
            let access_info = AccessInfo {
                arc_id,
                operation,
                timestamp: Instant::now(),
                backtrace: Backtrace::new_unresolved(),
                thread_name: current_thread_name(),
            };

            detector.record_access(key_handle_id, access_info);
        }

        TrackedArc {
            inner: Arc::new(key_handle),
            arc_id,
        }
    }

    /// Get the unique Arc ID of this TrackedArc instance
    pub fn arc_id(&self) -> u32 {
        self.arc_id
    }

    /// Get the KeyHandle ID (shared across all TrackedArc instances of the same
    /// KeyHandle)
    pub fn key_handle_id(&self) -> u64 {
        self.inner.key_handle_id
    }

    /// Get access to the TrackedInfo for this instance
    pub fn tracked_info(&self) -> Option<dashmap::mapref::one::Ref<'_, u64, Arc<TrackedInfo>>> {
        LeakDetector::instance().get_info(self.inner.key_handle_id)
    }

    /// Get the key associated with this TrackedArc
    pub fn get_key(&self) -> Option<Key> {
        Some(self.tracked_info()?.key.clone())
    }

    /// Get the number of access events for this KeyHandle
    pub fn get_access_count(&self) -> Option<usize> {
        if !self.inner.is_tracked {
            return None;
        }
        Some(self.tracked_info()?.access_history.lock().ok()?.len())
    }

    /// Get formatted debug information for this TrackedArc
    pub fn debug_info(&self) -> Option<String> {
        LeakDetector::instance().get_detailed_info(self.inner.key_handle_id)
    }

    /// Create a weak reference to this TrackedArc
    pub fn downgrade(&self) -> TrackedWeak {
        TrackedWeak {
            inner: Arc::downgrade(&self.inner),
            key_handle_id: self.inner.key_handle_id,
        }
    }

    /// Record an access event for this TrackedArc instance
    pub fn record_access(&self, operation: Operation) {
        // Only record if this KeyHandle is being tracked
        if self.inner.is_tracked {
            let access_info = AccessInfo {
                arc_id: self.arc_id,
                operation,
                timestamp: Instant::now(),
                backtrace: Backtrace::new_unresolved(),
                thread_name: current_thread_name(),
            };

            LeakDetector::instance().record_access(self.inner.key_handle_id, access_info);
        }
    }

    /// Lock the KeyHandle and return a KeyHandleGuard
    /// This method consumes the TrackedArc and returns a guard
    pub async fn lock(self) -> crate::KeyHandleGuard {
        // Pass the TrackedArc directly to KeyHandle::lock
        crate::KeyHandle::lock(self).await
    }
}

impl Clone for TrackedArc {
    fn clone(&self) -> Self {
        let detector = LeakDetector::instance();
        let new_arc_id = self.inner.next_arc_id();

        // Only record clone events if KeyHandle is tracked
        if self.inner.is_tracked {
            // Record clone event for original instance
            let now = Instant::now();
            let backtrace = Backtrace::new_unresolved();
            let clone_from_access = AccessInfo {
                arc_id: self.arc_id,
                operation: Operation::CloneFrom,
                timestamp: now,
                backtrace: backtrace.clone(),
                thread_name: current_thread_name(),
            };
            detector.record_access(self.inner.key_handle_id, clone_from_access);

            // Record creation event for new instance
            let clone_to_access = AccessInfo {
                arc_id: new_arc_id,
                operation: Operation::CloneTo,
                timestamp: now,
                backtrace,
                thread_name: current_thread_name(),
            };
            detector.record_access(self.inner.key_handle_id, clone_to_access);
        }

        // Create the cloned TrackedArc
        TrackedArc {
            inner: self.inner.clone(),
            arc_id: new_arc_id,
        }
    }
}

impl Drop for TrackedArc {
    fn drop(&mut self) {
        // Only record drop event if KeyHandle is tracked
        if self.inner.is_tracked {
            let access_info = AccessInfo {
                arc_id: self.arc_id,
                operation: Operation::Drop,
                timestamp: Instant::now(),
                backtrace: Backtrace::new_unresolved(),
                thread_name: current_thread_name(),
            };

            LeakDetector::instance().record_access(self.inner.key_handle_id, access_info);
        }

        // Clean up TrackedInfo if this was the last instance and it was tracked
        if self.inner.is_tracked && Arc::strong_count(&self.inner) == 1 {
            LeakDetector::instance().unregister(self.inner.key_handle_id);
        }
    }
}

impl Deref for TrackedArc {
    type Target = KeyHandle;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// Implement Send and Sync since KeyHandle implements them
unsafe impl Send for TrackedArc {}
unsafe impl Sync for TrackedArc {}

/// A weak reference to a TrackedArc<KeyHandle>
pub struct TrackedWeak {
    /// The wrapped Weak<KeyHandle>
    inner: Weak<KeyHandle>,
    /// KeyHandle ID (shared across all instances)
    key_handle_id: u64,
}

impl TrackedWeak {
    /// Attempt to upgrade the weak reference to a strong reference
    pub fn upgrade(&self) -> Option<TrackedArc> {
        let inner = self.inner.upgrade()?;

        let arc_id = inner.next_arc_id();

        // Only record upgrade event if KeyHandle is tracked
        if inner.is_tracked {
            let detector = LeakDetector::instance();
            let access_info = AccessInfo {
                arc_id,
                operation: Operation::Upgrade,
                timestamp: Instant::now(),
                backtrace: Backtrace::new_unresolved(),
                thread_name: current_thread_name(),
            };

            detector.record_access(self.key_handle_id, access_info);
        }

        Some(TrackedArc { inner, arc_id })
    }

    /// Get the KeyHandle ID (shared across all instances)
    pub fn key_handle_id(&self) -> u64 {
        self.key_handle_id
    }

    /// Check if the weak reference can still be upgraded
    pub fn strong_count(&self) -> usize {
        self.inner.strong_count()
    }

    /// Check if the weak reference can still be upgraded
    pub fn weak_count(&self) -> usize {
        self.inner.weak_count()
    }

    /// Check if two TrackedWeak references point to the same object
    pub fn ptr_eq(&self, other: &TrackedWeak) -> bool {
        self.inner.ptr_eq(&other.inner)
    }
}

impl Clone for TrackedWeak {
    fn clone(&self) -> Self {
        TrackedWeak {
            inner: self.inner.clone(),
            key_handle_id: self.key_handle_id,
        }
    }
}

// Implement Send and Sync since Weak<KeyHandle> implements them
unsafe impl Send for TrackedWeak {}
unsafe impl Sync for TrackedWeak {}

#[cfg(test)]
mod tests {
    use txn_types::Key;

    use super::*;

    #[test]
    fn test_access_based_tracking() {
        let key = Key::from_raw(b"test_key");
        let key_handle = KeyHandle::new(key);
        let tracked = TrackedArc::new(key_handle, Operation::Test);

        // Verify basic functionality
        assert!(tracked.arc_id() > 0);
        assert!(tracked.key_handle_id() > 0);

        // Record some access events
        tracked.record_access(Operation::Test);
        tracked.record_access(Operation::Test);

        // Verify access count
        assert_eq!(tracked.get_access_count().unwrap(), 3); // create + 2 operations

        // Test cloning
        let cloned = tracked.clone();
        assert_ne!(tracked.arc_id(), cloned.arc_id()); // Different arc IDs
        assert_eq!(tracked.key_handle_id(), cloned.key_handle_id()); // Same KeyHandle ID

        // Should have more access events now (clone_from, clone_to)
        assert!(tracked.get_access_count().unwrap() >= 5);

        // Test debug info
        let debug_info = tracked.debug_info().unwrap();
        assert!(debug_info.contains("KeyHandle ID:"));
        assert!(debug_info.contains("Access History"));
        assert!(debug_info.contains("test"));
        assert!(debug_info.contains("test"));
    }

    #[test]
    fn test_weak_reference_tracking() {
        let key = Key::from_raw(b"weak_test_key");
        let key_handle = KeyHandle::new(key);
        let tracked = TrackedArc::new(key_handle, Operation::Test);

        let weak = tracked.downgrade();
        assert_eq!(weak.key_handle_id(), tracked.key_handle_id());

        let upgraded = weak.upgrade().unwrap();
        assert_eq!(upgraded.key_handle_id(), tracked.key_handle_id());
        assert_ne!(upgraded.arc_id(), tracked.arc_id()); // Different arc ID

        // Should have upgrade event recorded
        assert!(tracked.get_access_count().unwrap() >= 2); // create + upgrade
    }

    #[test]
    fn test_phase2_comprehensive_tracking() {
        // Test 1: Basic TrackedArc creation and access tracking
        let key_handle = KeyHandle::new(Key::from_raw(b"test_key_phase2"));
        let tracked = TrackedArc::new(
            key_handle,
            Operation::Prewrite {
                start_ts: 100,
                for_update_ts: 100,
            },
        );

        // Verify initial state
        assert_eq!(tracked.arc_id(), 1);
        assert_eq!(tracked.get_access_count().unwrap(), 1); // Creation counts as access

        // Test 2: Multiple access operations
        tracked.record_access(Operation::Test);
        tracked.record_access(Operation::RawKeyGuard { ts: 200 });
        assert_eq!(tracked.get_access_count().unwrap(), 3);

        // Test 3: Clone tracking with per-KeyHandle arc IDs
        let cloned1 = tracked.clone();
        assert_eq!(cloned1.arc_id(), 2); // Should be next ID for same KeyHandle
        assert_eq!(tracked.get_access_count().unwrap(), 5); // +2 for clone_from/clone_to

        let cloned2 = tracked.clone();
        assert_eq!(cloned2.arc_id(), 3); // Should be next ID
        assert_eq!(tracked.get_access_count().unwrap(), 7); // +2 more

        // Test 4: Different KeyHandles have independent arc ID counters
        let key_handle2 = KeyHandle::new(Key::from_raw(b"different_key"));
        let tracked2 = TrackedArc::new(key_handle2, Operation::Test);
        assert_eq!(tracked2.arc_id(), 1); // Should restart at 1 for new KeyHandle

        // Test 5: Weak reference tracking
        let weak = TrackedArc::downgrade(&tracked);
        let upgraded = weak.upgrade().unwrap();
        assert_eq!(upgraded.arc_id(), 4); // Should be next ID for original KeyHandle

        // Test 6: Debug info format validation (before adding too many events)
        let debug_info = tracked.debug_info().unwrap();

        assert!(debug_info.contains("KeyHandle ID:"));
        assert!(debug_info.contains("Arc-1"));
        assert!(debug_info.contains("Arc-2"));
        assert!(debug_info.contains("prewrite_start_ts_100"));

        // Test 7: Access history bounds (max 10 events)
        for i in 0..15 {
            tracked.record_access(Operation::RawKeyGuard { ts: 300 + i });
        }
        assert_eq!(tracked.get_access_count().unwrap(), 10); // Should be capped at 10

        // Test 8: Verify bounded history
        let debug_info_after = tracked.debug_info().unwrap();
        assert!(debug_info_after.contains("Access History (10 events):"));
    }

    #[test]
    fn test_phase2_concurrent_access() {
        use std::{sync::Arc as StdArc, thread};

        // Ensure sampling is set to always track for this test
        let detector = LeakDetector::instance();
        detector.set_sampling_rate(1);

        let key_handle = KeyHandle::new(Key::from_raw(b"concurrent_test"));
        let tracked = StdArc::new(TrackedArc::new(key_handle, Operation::Test));

        let mut handles = vec![];

        // Spawn multiple threads to access the same TrackedArc
        for i in 0..10 {
            let tracked_clone = StdArc::clone(&tracked);
            let handle = thread::spawn(move || {
                for j in 0..5 {
                    tracked_clone.record_access(Operation::RawKeyGuard {
                        ts: (i * 10 + j) as u64,
                    });
                    // Small delay to increase chance of concurrent access
                    thread::sleep(std::time::Duration::from_millis(1));
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify that access count is bounded (should be 10 max due to VecDeque limit)
        let access_count = tracked.get_access_count().unwrap();
        assert_eq!(access_count, 10, "Access count should be bounded at 10");
    }

    #[test]
    fn test_leak_detection_configuration() {
        let detector = LeakDetector::instance();

        // Test leak detection configuration
        detector.set_leak_detection_enabled(true);
        detector.set_leak_detection_threshold(10); // 10 seconds

        // Create a tracked KeyHandle
        detector.set_sampling_rate(1); // Always track
        let key_handle = KeyHandle::new(Key::from_raw(b"leak_test_key"));
        let tracked = TrackedArc::new(key_handle, Operation::Test);

        // Verify it's tracked
        assert!(tracked.inner.is_tracked);
        assert!(detector.registry_size() > 0);

        // Test configuration getters would go here if we add them
        // For now, just verify the KeyHandle is properly tracked

        // Clean up
        drop(tracked);
        detector.set_leak_detection_enabled(false);
    }

    #[test]
    #[ignore]
    fn test_output_formatting() {
        // init logger for test
        test_util::init_log_for_test();

        let detector = LeakDetector::instance();

        // Test leak detection configuration
        detector.set_leak_detection_enabled(true);
        detector.set_leak_detection_threshold(1);

        // Create a tracked KeyHandle
        detector.set_sampling_rate(1); // Always track
        // 100 key handles
        let tracked_arcs = (0..100)
            .map(|i| {
                let key_handle =
                    KeyHandle::new(Key::from_raw(format!("leak_test_key_{}", i).as_bytes()));
                TrackedArc::new(key_handle, Operation::Test)
            })
            .collect::<Vec<_>>();
        for _ in 0..120 {
            for tracked in tracked_arcs.iter() {
                let weak = tracked.downgrade();
                let _ = weak.upgrade().unwrap();
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}
