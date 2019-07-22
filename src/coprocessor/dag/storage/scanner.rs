// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::range::*;
use super::ranges_iter::*;
use super::Storage;
use crate::coprocessor::Error;

/// A scanner that scan over multiple ranges.
pub struct RangesScanner<T> {
    storage: T,
    ranges_iter: RangesIterator,

    scan_backward_in_range: bool,
    is_key_only: bool,

    scanned_rows_per_range: Vec<usize>,

    // The following fields are only used for calculating scanned range.
    is_scanned_range_aware: bool,
    current_range_start_key: Vec<u8>,
    last_scanned_key: Vec<u8>,
}

impl<T: Storage> RangesScanner<T> {
    pub fn new(
        storage: T,
        ranges: Vec<Range>,
        scan_backward_in_range: bool, // TODO: This can be const generics
        is_key_only: bool,            // TODO: This can be const generics
        is_scanned_range_aware: bool, // TODO: This can be const generics
    ) -> RangesScanner<T> {
        let ranges_len = ranges.len();
        let ranges_iter = RangesIterator::new(ranges);
        RangesScanner {
            storage,
            ranges_iter,
            scan_backward_in_range,
            is_key_only,
            scanned_rows_per_range: Vec::with_capacity(ranges_len),
            is_scanned_range_aware,
            current_range_start_key: Vec::with_capacity(64),
            last_scanned_key: Vec::with_capacity(64),
        }
    }

    // Note: This is not implemented over `Iterator` since it can fail.
    // TODO: Change to use reference.
    #[allow(clippy::type_complexity)]
    pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        loop {
            let range = self.ranges_iter.next();
            let some_row = match range {
                IterStatus::NewRange(Range::Point(r)) => {
                    if self.is_scanned_range_aware {
                        self.update_scanned_range_from_new_point(&r);
                    }
                    self.ranges_iter.notify_drained();
                    self.scanned_rows_per_range.push(0);
                    self.storage.get(self.is_key_only, r)?
                }
                IterStatus::NewRange(Range::Interval(r)) => {
                    if self.is_scanned_range_aware {
                        self.update_scanned_range_from_new_range(&r);
                    }
                    self.scanned_rows_per_range.push(0);
                    self.storage
                        .begin_scan(self.scan_backward_in_range, self.is_key_only, r)?;
                    self.storage.scan_next()?
                }
                IterStatus::Continue => self.storage.scan_next()?,
                IterStatus::Drained => {
                    return Ok(None); // drained
                }
            };
            if self.is_scanned_range_aware {
                self.update_scanned_range_from_scanned_row(&some_row);
            }
            if some_row.is_some() {
                // Retrieved one row from point range or interval range.
                match self.scanned_rows_per_range.last_mut() {
                    Some(last_scanned_row) => *last_scanned_row += 1,
                    None => {
                        self.scanned_rows_per_range.push(1);
                    }
                }

                return Ok(some_row);
            } else {
                // No more row in the range.
                self.ranges_iter.notify_drained();
            }
        }
    }

    pub fn collect_storage_stats(&mut self, dest: &mut T::Statistics) {
        self.storage.collect_statistics(dest)
    }

    pub fn collect_scanned_rows_per_range(&mut self, dest: &mut Vec<usize>) {
        dest.append(&mut self.scanned_rows_per_range);
    }

    pub fn take_scanned_range(&mut self) -> IntervalRange {
        assert!(self.is_scanned_range_aware);

        let mut range = IntervalRange::default();
        if !self.scan_backward_in_range {
            range
                .lower_inclusive
                .extend_from_slice(&self.current_range_start_key);
            range
                .upper_exclusive
                .extend_from_slice(&self.last_scanned_key);
            crate::coprocessor::util::convert_to_prefix_next(&mut range.upper_exclusive);
            self.current_range_start_key.clear();
            self.current_range_start_key
                .extend_from_slice(&self.last_scanned_key);
            crate::coprocessor::util::convert_to_prefix_next(&mut self.current_range_start_key);
        } else {
            range
                .upper_exclusive
                .extend_from_slice(&self.current_range_start_key);
            range
                .lower_inclusive
                .extend_from_slice(&self.last_scanned_key);
            self.current_range_start_key.clear();
            self.current_range_start_key
                .extend_from_slice(&self.last_scanned_key);
        }

        range
    }

    fn update_scanned_range_from_new_point(&mut self, point: &PointRange) {
        if self.current_range_start_key.is_empty() {
            self.current_range_start_key.extend_from_slice(&point.0);
            if self.scan_backward_in_range {
                crate::coprocessor::util::convert_to_prefix_next(&mut self.current_range_start_key);
            }
        }
    }

    fn update_scanned_range_from_new_range(&mut self, range: &IntervalRange) {
        if self.current_range_start_key.is_empty() {
            if !self.scan_backward_in_range {
                self.current_range_start_key
                    .extend_from_slice(&range.lower_inclusive);
            } else {
                self.current_range_start_key
                    .extend_from_slice(&range.upper_exclusive);
            }
        }
    }

    fn update_scanned_range_from_scanned_row(&mut self, some_row: &Option<(Vec<u8>, Vec<u8>)>) {
        if let Some((key, _)) = some_row {
            self.last_scanned_key.clear();
            self.last_scanned_key.extend_from_slice(key.as_slice());
        }
    }
}
