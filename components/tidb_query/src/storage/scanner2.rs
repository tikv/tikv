// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::buffer_vec::BufferVec;

use super::range::*;
use super::ranges_iter::*;
use super::Storage;
use crate::error::StorageError;

pub struct RangesScanner2<T> {
    storage: T,
    ranges_iter: RangesIterator,

    is_key_only: bool,
    is_invalid_state: bool,

    keys_buffer: BufferVec,
    values_buffer: BufferVec,
    scanned_rows_per_range: Vec<usize>,
}

pub struct RangesScanner2Options<T> {
    pub storage: T,
    pub ranges: Vec<Range>,
    pub is_key_only: bool,
}

impl<T: Storage> RangesScanner2<T> {
    pub fn new(
        RangesScanner2Options {
            storage,
            ranges,
            is_key_only,
        }: RangesScanner2Options<T>,
    ) -> RangesScanner2<T> {
        let ranges_len = ranges.len();
        let ranges_iter = RangesIterator::new(ranges);
        RangesScanner2 {
            storage,
            ranges_iter,
            is_key_only,
            is_invalid_state: false,
            keys_buffer: BufferVec::with_capacity(1024, 1024 * 64),
            values_buffer: BufferVec::with_capacity(1024, 1024 * 128),
            scanned_rows_per_range: Vec::with_capacity(ranges_len),
        }
    }

    pub fn next_batch(&mut self, n: usize) -> Result<usize, StorageError> {
        // Protect from errors
        if unlikely!(self.is_invalid_state) {
            return Ok(0);
        }
        let r = self.next_batch_inner(n);
        if r.is_err() {
            self.is_invalid_state = true;
        }
        r
    }

    fn next_batch_inner(&mut self, n: usize) -> Result<usize, StorageError> {
        self.keys_buffer.clear();
        self.values_buffer.clear();
        let mut scanned_pairs = 0;
        while scanned_pairs < n {
            let range = self.ranges_iter.next();
            let this_iter_scanned = match range {
                IterStatus::NewRange(Range::Point(r)) => {
                    self.ranges_iter.notify_drained();
                    self.scanned_rows_per_range.push(0);
                    let some_kv = self.storage.get(self.is_key_only, r)?;
                    if let Some((key, value)) = some_kv {
                        self.keys_buffer.push(&key);
                        self.values_buffer.push(&value);
                        1
                    } else {
                        continue;
                    }
                }
                IterStatus::NewRange(Range::Interval(r)) => {
                    self.scanned_rows_per_range.push(0);
                    self.storage.begin_range_scan(self.is_key_only, r)?;
                    assert!(scanned_pairs <= n);
                    let pairs_to_scan = n - scanned_pairs;
                    let scanned = self.storage.range_scan_next_batch(
                        pairs_to_scan,
                        &mut self.keys_buffer,
                        &mut self.values_buffer,
                    )?;
                    assert!(scanned <= pairs_to_scan);
                    if scanned < pairs_to_scan {
                        self.ranges_iter.notify_drained();
                    }
                    scanned
                }
                IterStatus::Continue => {
                    assert!(scanned_pairs <= n);
                    let pairs_to_scan = n - scanned_pairs;
                    let scanned = self.storage.range_scan_next_batch(
                        pairs_to_scan,
                        &mut self.keys_buffer,
                        &mut self.values_buffer,
                    )?;
                    assert!(scanned <= pairs_to_scan);
                    if scanned < pairs_to_scan {
                        self.ranges_iter.notify_drained();
                    }
                    scanned
                }
                IterStatus::Drained => {
                    return Ok(scanned_pairs); // drained
                }
            };
            if this_iter_scanned > 0 {
                if let Some(r) = self.scanned_rows_per_range.last_mut() {
                    *r += this_iter_scanned;
                }
            }
            scanned_pairs += this_iter_scanned;
            assert!(scanned_pairs <= n);
        }
        Ok(scanned_pairs)
    }

    /// Appends storage statistics collected so far to the given container and clears the
    /// collected statistics.
    pub fn collect_storage_stats(&mut self, dest: &mut T::Statistics) {
        self.storage.collect_statistics(dest)
    }

    /// Appends scanned rows of each range so far to the given container and clears the
    /// collected statistics.
    pub fn collect_scanned_rows_per_range(&mut self, dest: &mut Vec<usize>) {
        dest.append(&mut self.scanned_rows_per_range);
        self.scanned_rows_per_range.push(0);
    }

    #[inline]
    pub fn valid(&self) -> bool {
        !self.is_invalid_state
    }

    #[inline]
    pub fn keys(&self) -> &BufferVec {
        assert!(self.valid());
        &self.keys_buffer
    }

    #[inline]
    pub fn values(&self) -> &BufferVec {
        assert!(self.valid());
        &self.values_buffer
    }
}
