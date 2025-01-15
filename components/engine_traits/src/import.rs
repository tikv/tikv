// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
};

use crate::{errors::Result, Range};

/// Currently used to ensure mutual exclusion between
/// compaction filter GC and ingest SST operations.
#[derive(Debug, Default)]
pub struct RangeLatch {
    /// A BTreeMap storing active range locks.
    /// Key: The start key of the range (sorted order).
    /// Value: A tuple of:
    ///   - `Arc<Mutex<()>>`: The latch object for this range.
    ///   - `(Vec<u8>, Vec<u8>)`: The actual range definition (start_key,
    ///     end_key).
    range_latches: RwLock<BTreeMap<Vec<u8>, (Arc<Mutex<()>>, (Vec<u8>, Vec<u8>))>>,
}

impl RangeLatch {
    pub fn new() -> Self {
        Self {
            range_latches: RwLock::new(BTreeMap::new()),
        }
    }

    /// Acquires a range latch for the specified `[start_key, end_key)`.
    /// This function ensures that a range latch is only granted when there are
    /// no overlapping ranges already latched. If overlaps exist, the
    /// function waits until those latches are released before proceeding.
    ///
    /// The concurrency is very low because only a few compaction filter
    /// threads (limited by `max_background_jobs`) and a single region
    /// worker thread run concurrently, with each thread holding at most one
    /// latch.
    ///
    /// Overlaps are determined by iterating through all active latches stored
    /// in `range_latches` and checking whether the specified range
    /// conflicts with any of them. While this approach may seem
    /// inefficient, the overhead of linear iteration is negligible in practice
    /// due to the low concurrency of the system.
    ///
    /// Live-locks can occur in this implementation. For example, a thread
    /// attempting to acquire a latch for the range `[a, z)` might encounter
    /// that `[a, b)` and `[b, c)` are already latched by other threads. The
    /// thread will first wait for `[a, b)` to be released, then proceed to wait
    /// for `[b, c)`. However, during the wait for `[b, c)` to be released,
    /// another thread might acquire `[a, b)` again, forcing the original
    /// thread to wait for `[a, b)` once more. This process could repeat
    /// indefinitely if threads continuously "jump the queue." Despite this,
    /// such live-locks are unlikely to persist for long due to the system's low
    /// concurrency, as conflicting latches are eventually released, allowing
    /// threads to make progress naturally.
    pub fn acquire(
        self: &Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<RangeLatchGuard> {
        loop {
            // Collect all overlapping ranges
            let overlapping_ranges = {
                let range_latches = self.range_latches.read().unwrap();

                range_latches
                    .iter()
                    .filter(|(_, (_, (existing_start, existing_end)))| {
                        !(end_key <= *existing_start || start_key >= *existing_end)
                    })
                    .map(|(key, (mutex, range))| (key.clone(), (mutex.clone(), range.clone())))
                    .collect::<Vec<_>>()
            };

            // If no conflicts, insert the new range and return the guard
            if overlapping_ranges.is_empty() {
                let mutex = Arc::new(Mutex::new(()));
                {
                    let mut range_latches = self.range_latches.write().unwrap();
                    range_latches.insert(
                        start_key.clone(),
                        (mutex.clone(), (start_key.clone(), end_key.clone())),
                    );
                }
                // Now acquire the lock after releasing the write guard
                let lock = mutex.lock().unwrap();
                let lock = unsafe { std::mem::transmute(lock) };

                return Ok(RangeLatchGuard {
                    start_key,
                    end_key,
                    _lock: lock,
                    latch: self.clone(),
                });
            }

            // Wait for all overlapping ranges to be released
            for (_, (mutex, (overlap_start_key, _))) in overlapping_ranges {
                let _guard = mutex.lock().unwrap();

                // Check if the range can be removed after unlocking
                let mut range_latches = self.range_latches.write().unwrap();
                if let Some((existing_mutex, _)) = range_latches.get(&overlap_start_key) {
                    if Arc::strong_count(existing_mutex) == 2 {
                        // Remove since it is only held by this thread: one reference is held by
                        // `_guard`, and the other by `range_latches`.
                        range_latches.remove(&overlap_start_key);
                    }
                }
            }
        }
    }
}

/// A guard that holds the range lock.
#[derive(Debug)]
pub struct RangeLatchGuard {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    /// Hold the mutex guard to prevent concurrent access to the same range.
    _lock: std::sync::MutexGuard<'static, ()>,
    /// Hold a reference to RangeLatch to release the lock when the guard is
    /// dropped.
    latch: Arc<RangeLatch>,
}

impl Drop for RangeLatchGuard {
    fn drop(&mut self) {
        let mut range_latches = self.latch.range_latches.write().unwrap();

        if let Some((mutex, _)) = range_latches.get_mut(&self.start_key) {
            // Remove the lock if no other references exist
            if Arc::strong_count(mutex) == 1 {
                range_latches.remove(&self.start_key);
            }
        }
    }
}

pub trait ImportExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        files: &[&str],
        range: Option<Range<'_>>,
    ) -> Result<()>;

    fn acquire_ingest_latch(&self, range: Range<'_>) -> Result<RangeLatchGuard>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);

    fn allow_write(&mut self, f: bool);
}
