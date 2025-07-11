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
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use backtrace::Backtrace;
use dashmap::DashMap;
use lazy_static::lazy_static;
use online_config::{ConfigChange, ConfigManager, OnlineConfig};
use tikv_util::{error, info, sys::thread::StdThreadBuildWrapper, warn};
use txn_types::Key;

use crate::{
    metrics::{TRACKED_ARC_CAPACITY_EXCEEDED, TRACKED_ARC_RECORD_NOT_FOUND},
    KeyHandle,
};

/// Configuration structure for TrackedArc debugging framework
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TrackedArcConfig {
    /// Sampling rate: 0=disabled, 1=all, N=1/N sampling
    pub sampling_rate: u32,
    /// Enable/disable leak detection
    pub leak_detection_enabled: bool,
    /// Minimum age for considering a KeyHandle as potentially leaked (in
    /// seconds)
    pub leak_detection_threshold_secs: u64,
    /// Maximum number of tracked KeyHandles
    pub max_registry_capacity: u64,
    /// Maximum access history size per KeyHandle
    pub max_history_size: usize,
    /// Maximum number of leaked KeyHandles to log per check
    pub max_leaks_to_log: u64,
    /// Registry cleanup period in seconds (0 = disabled)
    /// When enabled, performs total registry reset for fresh debugging data
    pub registry_cleanup_period_secs: u64,
    /// Maximum entries returned by dump API
    pub max_dump_entries: usize,
}

impl Default for TrackedArcConfig {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            sampling_rate: 1,
            #[cfg(not(test))]
            sampling_rate: 0,
            leak_detection_enabled: true,
            leak_detection_threshold_secs: 180,
            max_registry_capacity: 10000,
            max_history_size: 20,
            max_leaks_to_log: 2,
            registry_cleanup_period_secs: 3600,
            max_dump_entries: 50,
        }
    }
}

/// Operation types for TrackedArc access tracking
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Operation {
    /// Prewrite operation
    Prewrite { start_ts: u64, for_update_ts: u64 },
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
    /// Counter for sampling decisions
    sampling_counter: AtomicU32,

    /// Configuration fields as individual atomics for lock-free access
    sampling_rate: AtomicU32,
    leak_detection_enabled: AtomicBool,
    leak_detection_threshold_secs: AtomicU64,
    max_registry_capacity: AtomicU64,
    max_history_size: AtomicUsize,
    max_leaks_to_log: AtomicU64,
    registry_cleanup_period_secs: AtomicU64,
    max_dump_entries: AtomicUsize,

    /// Last cleanup timestamp (for period tracking)
    last_cleanup_time: AtomicU64,
}

impl LeakDetector {
    /// Create a new LeakDetector instance
    fn new() -> Self {
        let config = TrackedArcConfig::default();
        let detector = Self {
            registry: DashMap::new(),
            next_keyhandle_id: AtomicU64::new(1),
            sampling_counter: AtomicU32::new(0),
            last_cleanup_time: AtomicU64::new(0),

            // Initialize all config fields from default config
            sampling_rate: AtomicU32::new(config.sampling_rate),
            leak_detection_enabled: AtomicBool::new(config.leak_detection_enabled),
            leak_detection_threshold_secs: AtomicU64::new(config.leak_detection_threshold_secs),
            max_registry_capacity: AtomicU64::new(config.max_registry_capacity),
            max_history_size: AtomicUsize::new(config.max_history_size),
            max_leaks_to_log: AtomicU64::new(config.max_leaks_to_log),
            registry_cleanup_period_secs: AtomicU64::new(config.registry_cleanup_period_secs),
            max_dump_entries: AtomicUsize::new(config.max_dump_entries),
        };

        // Start background leak detection thread
        detector.start_leak_detection_thread();
        // make sure configs are consistent
        detector.update_config(&config);
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
    #[cfg(test)]
    fn set_sampling_rate(&self, rate: u32) {
        self.sampling_rate.store(rate, Ordering::Relaxed);
        // Reset counter when changing sampling rate
        self.sampling_counter.store(0, Ordering::Relaxed);
    }

    pub fn registry_size(&self) -> usize {
        self.registry.len()
    }

    pub fn update_config(&self, new_config: &TrackedArcConfig) {
        self.sampling_rate
            .store(new_config.sampling_rate, Ordering::Relaxed);
        self.leak_detection_enabled
            .store(new_config.leak_detection_enabled, Ordering::Relaxed);
        self.leak_detection_threshold_secs
            .store(new_config.leak_detection_threshold_secs, Ordering::Relaxed);
        self.max_registry_capacity
            .store(new_config.max_registry_capacity, Ordering::Relaxed);
        self.max_history_size
            .store(new_config.max_history_size, Ordering::Relaxed);
        self.max_leaks_to_log
            .store(new_config.max_leaks_to_log, Ordering::Relaxed);
        self.registry_cleanup_period_secs
            .store(new_config.registry_cleanup_period_secs, Ordering::Relaxed);
        self.max_dump_entries
            .store(new_config.max_dump_entries, Ordering::Relaxed);
        info!("TrackedArc configuration updated"; "config" => ?new_config);
    }

    /// Enable/disable background leak detection
    #[cfg(test)]
    fn set_leak_detection_enabled(&self, enabled: bool) {
        self.leak_detection_enabled
            .store(enabled, Ordering::Relaxed);
    }

    /// Set the threshold for considering a KeyHandle as potentially leaked (in
    /// seconds)
    #[cfg(test)]
    fn set_leak_detection_threshold(&self, threshold_seconds: u64) {
        self.leak_detection_threshold_secs
            .store(threshold_seconds, Ordering::Relaxed);
    }

    /// Get current configuration as a struct
    pub fn get_config(&self) -> TrackedArcConfig {
        // Read all config values from atomic fields - no locks needed!
        TrackedArcConfig {
            sampling_rate: self.sampling_rate.load(Ordering::Relaxed),
            leak_detection_enabled: self.leak_detection_enabled.load(Ordering::Relaxed),
            leak_detection_threshold_secs: self
                .leak_detection_threshold_secs
                .load(Ordering::Relaxed),
            max_registry_capacity: self.max_registry_capacity.load(Ordering::Relaxed),
            max_history_size: self.max_history_size.load(Ordering::Relaxed),
            max_leaks_to_log: self.max_leaks_to_log.load(Ordering::Relaxed),
            registry_cleanup_period_secs: self.registry_cleanup_period_secs.load(Ordering::Relaxed),
            max_dump_entries: self.max_dump_entries.load(Ordering::Relaxed),
        }
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

    /// Record an access event for a KeyHandle
    /// SAFETY: Avoids deadlock by cloning the Arc before acquiring Mutex
    pub fn record_access(&self, key_handle_id: u64, access_info: AccessInfo) {
        // SAFE: Get Arc<TrackedInfo> and release DashMap lock immediately
        let tracked_info = match self.registry.get(&key_handle_id) {
            Some(entry) => entry.value().clone(), // Clone Arc, release DashMap lock
            None => {
                // the record may have been cleared
                TRACKED_ARC_RECORD_NOT_FOUND.inc();
                return;
            }
        };

        // Now safely acquire Mutex without holding DashMap lock
        let mut history = match tracked_info.access_history.lock() {
            Ok(history) => history,
            Err(_) => {
                error!("lock is poisoned");
                return;
            }
        };

        history.push_back(access_info);

        let max_size = self.max_history_size.load(Ordering::Relaxed);

        if history.len() > max_size {
            history.pop_front();
        }
    }

    /// Remove a TrackedInfo from the registry
    pub fn unregister(&self, id: u64) {
        self.registry.remove(&id);
    }

    /// Retrieve a cloned Arc<TrackedInfo> by ID (deadlock-safe)
    /// SAFETY: Returns cloned Arc, immediately releases DashMap lock
    pub fn get_info(&self, id: u64) -> Option<Arc<TrackedInfo>> {
        self.registry.get(&id).map(|entry| entry.value().clone())
    }

    /// Get summary information about all tracked objects
    pub fn get_summary(&self) -> Vec<(u64, String, usize)> {
        self.registry
            .iter()
            .map(|entry| entry.value().clone())
            .map(|info| {
                let access_count = info.access_history.lock().map(|h| h.len()).unwrap_or(0);
                (info.key_handle_id, format!("{:?}", info.key), access_count)
            })
            .collect()
    }

    /// Get detailed information about a specific tracked object including
    /// access history
    pub fn get_detailed_info(&self, key_handle_id: u64) -> Option<String> {
        // do not hold references into the dashmap
        let info: Arc<TrackedInfo> = self.get_info(key_handle_id)?;

        let mut result = format!(
            "KeyHandle ID: {}\n\
             Key: {:?}\n",
            info.key_handle_id, info.key
        );

        if let Ok(access_history) = info.access_history.lock() {
            result.push_str(&Self::format_access_history(&access_history, false));
        }

        Some(result)
    }

    /// Start the background leak detection thread
    fn start_leak_detection_thread(&self) {
        std::thread::Builder::new()
            .name("leak-detector".to_string())
            .spawn_wrapper(|| {
                loop {
                    std::thread::sleep(Duration::from_secs(60));

                    let detector = LeakDetector::instance();
                    if !detector.leak_detection_enabled.load(Ordering::Relaxed) {
                        continue;
                    }

                    detector.check_for_leaks_internal();

                    // Perform registry cleanup if enabled
                    detector.cleanup_old_entries_if_needed();
                }
            })
            .expect("Failed to start leak detection thread");
    }

    /// Check for potentially leaked KeyHandles and log them
    fn check_for_leaks_internal(&self) {
        let threshold_secs = self.leak_detection_threshold_secs.load(Ordering::Relaxed);
        let max_leaks_to_log = self.max_leaks_to_log.load(Ordering::Relaxed) as usize;
        let now = Instant::now();

        // SAFE: Collect all TrackedInfo Arcs first, releasing DashMap locks
        let tracked_infos: Vec<Arc<TrackedInfo>> = self.registry
            .iter()
            .map(|entry| entry.value().clone()) // Clone Arc, release DashMap lock
            .collect();

        let mut leaked_handles = Vec::new();
        for tracked_info in tracked_infos {
            // Check if this KeyHandle has been alive longer than threshold
            // Use creation_time instead of first access time for accurate age calculation
            let age = now.duration_since(tracked_info.creation_time);

            if age.as_secs() > threshold_secs {
                leaked_handles.push((tracked_info, age));
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

    /// Common function to format access history
    /// `include_backtrace`: whether to include backtrace information
    fn format_access_history(history: &VecDeque<AccessInfo>, include_backtrace: bool) -> String {
        let mut result = format!("Access History ({} events):\n", history.len());

        for (i, access) in history.iter().enumerate() {
            if include_backtrace {
                // For leak detection, include backtrace (note: backtrace should be resolved by
                // caller)
                result.push_str(&format!(
                    "  #{}: Arc-{} [-{}ms] {} (thread: {}) (backtrace: {:?})\n",
                    i + 1,
                    access.arc_id,
                    access.timestamp.elapsed().as_millis(),
                    access.operation,
                    access.thread_name,
                    access.backtrace
                ));
            } else {
                // For general info, no backtrace needed
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

        result
    }

    /// Format access history for leak logging (resolves backtraces)
    fn format_access_history_for_leak(history: &mut VecDeque<AccessInfo>) -> String {
        // Resolve backtraces first
        for access in history.iter_mut() {
            access.backtrace.resolve();
        }

        // Use common formatting function with backtrace enabled
        Self::format_access_history(history, true)
    }

    /// Cleanup old entries if cleanup is enabled and period has elapsed
    fn cleanup_old_entries_if_needed(&self) {
        let cleanup_period = self.registry_cleanup_period_secs.load(Ordering::Relaxed);

        if cleanup_period == 0 {
            return; // Cleanup disabled
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let last_cleanup = self.last_cleanup_time.load(Ordering::Relaxed);

        // Check if enough time has passed since last cleanup
        if now.saturating_sub(last_cleanup) >= cleanup_period {
            self.cleanup_old_entries();
            self.last_cleanup_time.store(now, Ordering::Relaxed);
        }
    }

    /// Cleanup registry - performs a total reset for fresh debugging data
    /// Returns the number of entries removed
    pub fn cleanup_old_entries(&self) {
        self.registry.clear();
        info!("Registry cleanup");
    }
}

lazy_static! {
    static ref LEAK_DETECTOR: LeakDetector = LeakDetector::new();
}

pub struct TrackedArcConfigManager;

impl ConfigManager for TrackedArcConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        let detector = LeakDetector::instance();

        // Get current config and apply changes using OnlineConfig derive
        let mut current_config = detector.get_config();
        current_config.update(change)?;

        // Apply the updated config to atomic fields
        detector.update_config(&current_config);
        Ok(())
    }
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

        if key_handle.is_tracked {
            let max_capacity = detector.max_registry_capacity.load(Ordering::Relaxed) as usize;

            if detector.registry.len() >= max_capacity {
                TRACKED_ARC_CAPACITY_EXCEEDED.inc();
            } else {
                detector.registry.entry(key_handle_id).or_insert_with(|| {
                    Arc::new(TrackedInfo {
                        key_handle_id,
                        key,
                        creation_time: Instant::now(),
                        access_history: Mutex::new(VecDeque::new()),
                    })
                });
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

    /// Get a cloned Arc<TrackedInfo> for this instance (deadlock-safe)
    pub fn tracked_info_safe(&self) -> Option<Arc<TrackedInfo>> {
        LeakDetector::instance().get_info(self.inner.key_handle_id)
    }

    /// Get the key associated with this TrackedArc
    pub fn get_key(&self) -> Option<Key> {
        Some(self.tracked_info_safe()?.key.clone())
    }

    /// Get the number of access events for this KeyHandle
    /// SAFETY: Uses deadlock-safe TrackedInfo access
    pub fn get_access_count(&self) -> Option<usize> {
        if !self.inner.is_tracked {
            return None;
        }
        let tracked_info = self.tracked_info_safe()?;
        let history = tracked_info.access_history.lock().ok()?;
        Some(history.len())
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

        // Test 7: Access history bounds
        let instance = LeakDetector::instance();
        let max_history_size = instance.max_history_size.load(Ordering::Relaxed);
        for i in 0..max_history_size * 2 {
            tracked.record_access(Operation::RawKeyGuard { ts: 300 + i as u64 });
        }
        assert_eq!(tracked.get_access_count().unwrap(), max_history_size);
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

    #[test]
    fn test_unified_configuration() {
        let detector = LeakDetector::instance();

        // Test unified configuration update
        let new_config = TrackedArcConfig {
            sampling_rate: 50,
            leak_detection_enabled: true,
            leak_detection_threshold_secs: 600,
            max_registry_capacity: 20000,
            max_history_size: 15,
            max_leaks_to_log: 5,
            registry_cleanup_period_secs: 7200,
            max_dump_entries: 100,
        };

        detector.update_config(&new_config);

        // Verify configuration was applied
        let retrieved_config = detector.get_config();
        assert_eq!(retrieved_config.sampling_rate, 50);
        assert_eq!(retrieved_config.leak_detection_enabled, true);
        assert_eq!(retrieved_config.leak_detection_threshold_secs, 600);
        assert_eq!(retrieved_config.max_registry_capacity, 20000);
        assert_eq!(retrieved_config.max_leaks_to_log, 5);
        assert_eq!(retrieved_config.registry_cleanup_period_secs, 7200);
        assert_eq!(retrieved_config.max_dump_entries, 100);

        // Test individual setters still work
        detector.set_leak_detection_threshold(300);
        let updated_config = detector.get_config();
        assert_eq!(updated_config.leak_detection_threshold_secs, 300);

        // Reset to defaults
        detector.update_config(&TrackedArcConfig::default());
    }

    #[test]
    fn test_tikv_config_integration() {
        use std::collections::HashMap;

        use online_config::ConfigValue;

        let detector = LeakDetector::instance();
        let mut config_manager = TrackedArcConfigManager;

        // Test config change through ConfigManager
        let mut change = HashMap::new();
        change.insert("sampling_rate".to_string(), ConfigValue::U32(200));
        change.insert(
            "leak_detection_enabled".to_string(),
            ConfigValue::Bool(true),
        );
        change.insert(
            "leak_detection_threshold_secs".to_string(),
            ConfigValue::U64(600),
        );

        // Apply config change
        config_manager.dispatch(change).unwrap();

        // Verify changes were applied
        let config = detector.get_config();
        assert_eq!(config.sampling_rate, 200);
        assert_eq!(config.leak_detection_enabled, true);
        assert_eq!(config.leak_detection_threshold_secs, 600);

        // Reset to defaults
        detector.update_config(&TrackedArcConfig::default());
    }
}
