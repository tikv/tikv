// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{errors::Result, Range};

/// A structure used to manage range-based latch with mutual exclusion.
///
/// Currently used to ensure mutual exclusion between compaction filter and
/// ingest SST operations.
///
/// This implementation is designed for scenarios with low concurrency.
/// In the current scenario, compaction filter threads are limited by
/// `RocksDB.max_background_jobs`, and region worker threads typically run one
/// at a time. Due to this low level of concurrency, the overhead of range
/// locking is minimal, and potential conflicts are rare.
///
/// Additionally, both the compaction filter and ingest SST operations that
/// use this `RangeLatch` enforce self-mutual exclusion. While this might
/// appear somewhat overkill, its impact on performance is minimal due to
/// the system's low concurrency and the brief duration of lock holding.
#[derive(Debug, Default)]
pub struct RangeLatch {
    /// A BTreeMap storing active range locks.
    /// Key: The start key of the range (sorted order).
    /// Value: A tuple of:
    ///   - `Arc<Mutex<()>>`: The latch object for this range.
    ///   - `(Vec<u8>, Vec<u8>)`: The actual range definition (start_key,
    ///     end_key).
    range_latches: Mutex<BTreeMap<Vec<u8>, (Arc<Mutex<()>>, (Vec<u8>, Vec<u8>))>>,
}

impl RangeLatch {
    pub fn new() -> Self {
        Self {
            range_latches: Mutex::new(BTreeMap::new()),
        }
    }

    /// Acquires a range latch for the specified `[start_key, end_key)`.
    /// This function ensures that a range latch is only granted when there are
    /// no overlapping ranges already latched. If overlaps exist, the
    /// function waits until those latches are released before proceeding.
    ///
    /// Overlaps are determined by iterating through all active latches stored
    /// in `range_latches` and checking whether the specified range
    /// conflicts with any of them. While this approach may seem
    /// inefficient, the overhead of linear iteration is negligible in practice
    /// due to the low concurrency of the users.
    ///
    /// Live-locks can occur in this implementation. For example, a thread
    /// attempting to acquire a latch for the range `[a, z)` might encounter
    /// that `[a, b)` and `[b, c)` are already latched by other threads. The
    /// thread will first wait for `[a, b)` to be released, then proceed to wait
    /// for `[b, c)`. However, during the wait for `[b, c)` to be released,
    /// another thread might acquire `[a, b)` again, forcing the original
    /// thread to wait for `[a, b)` once more. This process could repeat
    /// indefinitely if threads continuously "jump the queue." Despite this,
    /// such live-locks are unlikely to persist for long due to the user's low
    /// concurrency, as conflicting latches are eventually released, allowing
    /// threads to make progress naturally.
    pub fn acquire(
        self: &Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<RangeLatchGuard> {
        loop {
            let mut range_latches = self.range_latches.lock().unwrap();

            // Collect all overlapping ranges
            let overlapping_ranges = range_latches
                .iter()
                .filter(|(_, (_, (existing_start, existing_end)))| {
                    !(end_key <= *existing_start || start_key >= *existing_end)
                })
                .map(|(key, (mutex, range))| (key.clone(), (mutex.clone(), range.clone())))
                .collect::<Vec<_>>();

            // If no conflicts, insert the new range and return the guard
            if overlapping_ranges.is_empty() {
                let mutex = Arc::new(Mutex::new(()));
                range_latches.insert(
                    start_key.clone(),
                    (mutex.clone(), (start_key.clone(), end_key.clone())),
                );

                // Now acquire the lock after releasing the write guard
                let lock = mutex.lock().unwrap();
                // The lifetime of the lock returned by `mutex.lock()` is tied to the `_guard`
                // variable, but we need to extend its lifetime to `'static` so it can be stored
                // in the `RangeLatchGuard` struct. This is safe because:
                // - The `lock` is managed by the `RangeLatchGuard`, ensuring proper drop
                //   semantics.
                // - The lifetime extension does not result in use-after-free, as the lock's
                //   underlying resources are valid for the lifetime of the `mutex`.
                // - The design guarantees that `RangeLatchGuard` will hold the lock for its
                //   entire lifetime, preventing invalid access.
                let lock = unsafe { std::mem::transmute(lock) };

                return Ok(RangeLatchGuard {
                    start_key,
                    _lock: lock,
                    latch: self.clone(),
                });
            }
            drop(range_latches);

            // Wait for all overlapping ranges to be released
            for (_, (mutex, (overlap_start_key, _))) in overlapping_ranges {
                let _guard = mutex.lock().unwrap();

                // Check if the range can be removed after unlocking
                let mut range_latches = self.range_latches.lock().unwrap();
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
    /// Hold the mutex guard to prevent concurrent access to the same range.
    _lock: std::sync::MutexGuard<'static, ()>,
    /// Hold a reference to RangeLatch to release the lock when the guard is
    /// dropped.
    latch: Arc<RangeLatch>,
}

impl Drop for RangeLatchGuard {
    fn drop(&mut self) {
        let mut range_latches = self.latch.range_latches.lock().unwrap();

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

    /// Ingests external files into the specified column family.
    ///
    /// If the range is specified, it enables `RocksDB
    /// IngestExternalFileOptions.allow_write` and locks the
    /// specified range.  
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

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use super::*;

    #[test]
    fn test_single_thread_non_overlapping_ranges() {
        let latch = Arc::new(RangeLatch::new());

        // Acquire non-overlapping ranges
        let guard1 = latch.acquire(vec![0], vec![10]).unwrap();
        let guard2 = latch.acquire(vec![20], vec![30]).unwrap();
        let guard3 = latch.acquire(vec![30], vec![50]).unwrap();

        // Drop guards to release locks
        drop(guard1);
        drop(guard2);
        drop(guard3);
    }

    #[test]
    fn test_two_threads_overlapping_ranges() {
        fn do_test(range1: (Vec<u8>, Vec<u8>), range2: (Vec<u8>, Vec<u8>)) {
            let latch = Arc::new(RangeLatch::new());
            let latch_clone = Arc::clone(&latch);
            let is_thread_done = Arc::new(AtomicBool::new(false));
            let is_thread_done_clone = Arc::clone(&is_thread_done);

            // Main thread acquires the first lock
            let guard = latch.acquire(range1.0, range1.1).unwrap();

            let handle = thread::spawn(move || {
                let _guard = latch_clone.acquire(range2.0, range2.1).unwrap();
                is_thread_done_clone.store(true, Ordering::SeqCst);
            });

            thread::sleep(Duration::from_millis(500));
            // Verify that the second thread is still waiting
            assert!(!is_thread_done.load(Ordering::SeqCst));

            // Release the first lock
            drop(guard);

            // Wait a bit to let the second thread complete
            thread::sleep(Duration::from_millis(100));
            assert!(is_thread_done.load(Ordering::SeqCst));

            // Ensure the second thread completes successfully
            handle.join().unwrap();
        }

        // Test different overlapping cases
        do_test((vec![0], vec![10]), (vec![5], vec![15])); // Overlap at the end
        do_test((vec![5], vec![15]), (vec![0], vec![10])); // Overlap at the begin
        do_test((vec![0], vec![10]), (vec![4], vec![5])); // Contained within
        do_test((vec![5], vec![15]), (vec![0], vec![20])); // Fully contained
    }
}
