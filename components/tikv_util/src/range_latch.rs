use std::{
    collections::{
        BTreeMap,
        Bound::{Excluded, Unbounded},
    },
    sync::{Arc, Mutex},
};

/// A structure used to manage range-based latch with mutual exclusion.
///
/// Currently used to ensure mutual exclusion between compaction filter and
/// ingest SST operations. When using RocksDB
/// IngestExternalFileOptions.allow_write = true for ingest SST, concurrent
/// overlapping writes must be avoided during ingestion to ensure
/// sequence number consistency across LSM-tree levels in RocksDB. However, the
/// compaction filter might concurrently write overlapping keys to the default
/// column family. For example, if a region is migrated out and then migrated
/// back, it might cause concurrent compaction filter overlaps with the
/// apply-snapshot-ingest process. Therefore, it must be made mutually exclusive
/// with the ingest latch.
//
/// This implementation is designed for scenarios with low concurrency.
/// In the current scenario, the number of compaction filter threads are limited
/// by `RocksDB.max_background_jobs`, and the region worker, which handles
/// apply-snapshot-ingest or destroy-peer-ingest, operates as a single thread.
/// Due to this low level of concurrency, the overhead of range latch is
/// minimal, and potential conflicts are rare.
///
/// NOTE: Both the compaction filter and ingest SST operations that
/// use this `RangeLatch` enforce self-mutual exclusion. While this might
/// appear somewhat overkill, its impact on performance is minimal due to
/// the system's low concurrency and the brief duration of latch holding.
///
/// NOTE: Why do we use a range-latch instead of a region-id latch? This is
/// because region-id is mutable and there is currently no mechanism to notify
/// the compaction filter in real time. For example, if a region is migrated
/// out, split, and then migrated back, the compaction filter might hold the old
/// region-id while the apply-snapshot-ingest process holds the new region-id.
#[derive(Debug, Default)]
pub struct RangeLatch {
    /// A BTreeMap storing active range latches.
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
    /// Live-locks can occur in this implementation. For example, a thread
    /// attempting to acquire a latch for the range `[a, z)` might encounter
    /// that `[a, b)` and `[b, c)` are already latched by other threads. The
    /// thread will first wait for [a, b) to be released and successfully
    /// acquire it. However, while waiting for [b, c) to be released,
    /// another thread might acquire [a, b) again, forcing the original thread
    /// to wait for [a, b) once more. This process could repeat
    /// indefinitely if threads continuously "jump the queue.". Fortunately,
    /// such live-locks are unlikely to persist for long due to the user's low
    /// concurrency, as conflicting latches are eventually released, allowing
    /// threads to make progress naturally.
    ///
    /// Deadlocks cannot occur in the current scenario, as each caller thread
    /// holds at most one lock at a time.
    pub fn acquire<'a>(
        self: &'a Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> RangeLatchGuard<'a> {
        loop {
            let mut range_latches = self.range_latches.lock().unwrap();

            // Collect all overlapping ranges
            let overlapping_ranges = range_latches
                .range((Unbounded::<Vec<u8>>, Excluded(end_key.clone())))
                .filter(|(_, (_, (existing_start, existing_end)))| {
                    !(end_key <= *existing_start || start_key >= *existing_end)
                })
                .map(|(key, (mutex, range))| (key.clone(), (mutex.clone(), range.clone())))
                .collect::<Vec<_>>();

            // If no conflicts, insert the new range and return the guard
            if overlapping_ranges.is_empty() {
                let mutex = Arc::new(Mutex::new(()));
                let previous_value = range_latches.insert(
                    start_key.clone(),
                    (mutex.clone(), (start_key.clone(), end_key.clone())),
                );
                debug_assert!(previous_value.is_none());

                // Now acquire the latch after releasing the write guard
                let mutex_guard = mutex.lock().unwrap();
                // Safety: `_mutex_guard` is declared before `handle` in `KeyHandleGuard`.
                // So the mutex guard will be released earlier than the `Arc<KeyHandle>`.
                // Then we can make sure the mutex guard doesn't point to released memory.
                let mutex_guard = unsafe { std::mem::transmute(mutex_guard) };

                return RangeLatchGuard {
                    start_key,
                    _mutex_guard: mutex_guard,
                    handle: self,
                };
            }
            drop(range_latches);

            // Wait for all overlapping ranges to be released
            for (_, (mutex, (..))) in overlapping_ranges {
                let _guard = mutex.lock().unwrap();
            }
        }
    }
}

/// A guard that holds the range latch.
#[derive(Debug)]
pub struct RangeLatchGuard<'a> {
    start_key: Vec<u8>,
    /// Hold the mutex guard to prevent concurrent access to the same range.
    ///
    /// This field must be declared before `handle` so it will be dropped before
    /// `handle`.
    _mutex_guard: std::sync::MutexGuard<'a, ()>,
    /// Holds a reference to RangeLatch to release the latch when the guard is
    /// dropped.
    handle: &'a RangeLatch,
}

impl<'a> Drop for RangeLatchGuard<'a> {
    fn drop(&mut self) {
        let mut range_latches = self.handle.range_latches.lock().unwrap();
        range_latches.remove(&self.start_key);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use rand::Rng;

    use super::*;

    #[test]
    fn test_single_thread_non_overlapping_ranges() {
        let latch = Arc::new(RangeLatch::new());

        // Acquire non-overlapping ranges
        let guard1 = latch.acquire(vec![0], vec![10]);
        let guard2 = latch.acquire(vec![20], vec![30]);
        let guard3 = latch.acquire(vec![30], vec![50]);

        // Drop guards to release latches
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

            // Main thread acquires the first latch
            let guard = latch.acquire(range1.0, range1.1);

            let handle = thread::spawn(move || {
                let _guard = latch_clone.acquire(range2.0, range2.1);
                is_thread_done_clone.store(true, Ordering::SeqCst);
            });

            thread::sleep(Duration::from_millis(500));
            // Verify that the second thread is still waiting
            assert!(!is_thread_done.load(Ordering::SeqCst));

            // Release the first latch
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

    fn concurrent_random_ranges_test(num_threads: usize, num_latches: usize) {
        // Shared latch and active ranges tracker
        let latch = Arc::new(RangeLatch::new());
        let active_ranges = Arc::new(Mutex::new(HashSet::new()));

        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let latch_clone = Arc::clone(&latch);
            let active_ranges_clone = Arc::clone(&active_ranges);

            handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();

                for _ in 0..num_latches {
                    let start: u8 = rng.gen_range(0..200);
                    let end = rng.gen_range(start + 1..201);

                    // Acquire the range latch
                    let _guard = latch_clone.acquire(vec![start], vec![end]);

                    // Check for overlap
                    {
                        let mut ranges = active_ranges_clone.lock().unwrap();
                        for (existing_start, existing_end) in ranges.iter() {
                            assert!(
                                end <= *existing_start || start >= *existing_end,
                                "Overlapping range detected: [{}, {}) overlaps with [{}, {})",
                                start,
                                end,
                                existing_start,
                                existing_end
                            );
                        }

                        // Add the current range to the active set
                        ranges.insert((start, end));
                    }

                    // Hold the latch for a short period
                    thread::sleep(Duration::from_millis(10));

                    // Remove the range from the active set
                    {
                        let mut ranges = active_ranges_clone.lock().unwrap();
                        ranges.remove(&(start, end));
                    }
                }
            }));
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_random_ranges_many_latches() {
        concurrent_random_ranges_test(5, 100);
    }

    #[test]
    fn test_concurrent_random_ranges_many_threads() {
        concurrent_random_ranges_test(100, 5);
    }
}
