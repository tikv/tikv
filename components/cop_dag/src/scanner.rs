// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::coprocessor::KeyRange;

use crate::codec::table::truncate_as_row_key;
use crate::storage::{Key, Scanner as KvScanner, Statistics, Store, Value};
use crate::util;
use crate::Result;
use tikv_util::{escape, set_panic_mark};

const MIN_KEY_BUFFER_CAPACITY: usize = 256;

#[derive(Copy, Clone, PartialEq)]
pub enum ScanOn {
    Table,
    Index,
}

fn create_range_scanner<S: Store>(
    store: &S,
    desc: bool,
    key_only: bool,
    range: &KeyRange,
) -> Result<S::Scanner> {
    let lower_bound = Some(Key::from_raw(range.get_start()));
    let upper_bound = Some(Key::from_raw(range.get_end()));
    Ok(
        match store.scanner(desc, key_only, lower_bound, upper_bound) {
            Ok(val) => val,
            Err(e) => return Err(e.into()),
        },
    )
}

// `Scanner` is a helper struct to wrap all common scan operations
// for `TableScanExecutor` and `IndexScanExecutor`
pub struct Scanner<S: Store> {
    pub desc: bool, // It's public for tests.
    scan_on: ScanOn,
    key_only: bool,
    last_scanned_key: Vec<u8>,
    scanner: S::Scanner,
    range: KeyRange,
    no_more: bool,
    /// `reset_range` may re-initialize a StoreScanner, so we need to backlog statistics.
    statistics_backlog: Statistics,
}

impl<S: Store> Scanner<S> {
    pub fn new(
        store: &S,
        scan_on: ScanOn,
        desc: bool,
        key_only: bool,
        range: KeyRange,
    ) -> Result<Self> {
        Ok(Self {
            desc,
            scan_on,
            key_only,
            last_scanned_key: Vec::with_capacity(MIN_KEY_BUFFER_CAPACITY),
            scanner: create_range_scanner(store, desc, key_only, &range)?,
            range,
            no_more: false,
            statistics_backlog: Statistics::default(),
        })
    }

    pub fn reset_range(&mut self, range: KeyRange, store: &S) -> Result<()> {
        // TODO: Recreating range scanner is a time consuming operation. We need to provide
        // operation to reset range for scanners.
        self.range = range;
        self.no_more = false;
        self.last_scanned_key.clear();
        self.statistics_backlog.add(&self.scanner.take_statistics());
        self.scanner = create_range_scanner(store, self.desc, self.key_only, &self.range)?;
        Ok(())
    }

    pub fn next_row(&mut self) -> Result<Option<(Vec<u8>, Value)>> {
        if self.no_more {
            return Ok(None);
        }

        let kv = match self.scanner.next() {
            Ok(val) => val,
            Err(e) => return Err(e.into()),
        };

        let (key, value) = match kv {
            Some((k, v)) => (box_try!(k.into_raw()), v),
            None => {
                self.no_more = true;
                return Ok(None);
            }
        };

        if self.range.start > key || self.range.end <= key {
            set_panic_mark();
            panic!(
                "key: {} out of range, start: {}, end: {}",
                escape(&key),
                escape(self.range.get_start()),
                escape(self.range.get_end())
            );
        }

        // `Vec::clear()` produce 2 more instructions than `set_len(0)`, so we directly use
        // `set_len()` here.
        unsafe {
            self.last_scanned_key.set_len(0);
        }
        self.last_scanned_key.extend_from_slice(key.as_slice());

        Ok(Some((key, value)))
    }

    pub fn start_scan(&self, range: &mut KeyRange) {
        assert!(!self.no_more);
        if self.last_scanned_key.is_empty() {
            // Happens when new() -> start_scan().
            if !self.desc {
                range.set_start(self.range.get_start().to_owned());
            } else {
                range.set_end(self.range.get_end().to_owned());
            }
        } else {
            // Happens when new() -> start_scan() -> next_row() ... -> stop_scan() -> start_scan().
            if !self.desc {
                // In `stop_scan`, we will `next(last_scanned_key)`, so we don't need to next()
                // again here.
                range.set_start(self.last_scanned_key.clone());
            } else {
                // In `stop_scan`, we have already truncated to row key, so we don't need to
                // do it again here.
                range.set_end(self.last_scanned_key.clone());
            }
        }
    }

    pub fn stop_scan(&mut self, range: &mut KeyRange) -> bool {
        if self.no_more {
            return false;
        }
        // `next_row()` should have been called when calling `stop_scan()` after `start_scan()`.
        assert!(!self.last_scanned_key.is_empty());
        if !self.desc {
            // Make range_end exclusive. Note that next `start_scan` will also use this
            // key as the range_start.
            util::convert_to_prefix_next(&mut self.last_scanned_key);
            range.set_end(self.last_scanned_key.clone());
        } else {
            // TODO: We may don't need `truncate_as_row_key`. Needs investigation.
            if self.scan_on == ScanOn::Table {
                let row_key_len = truncate_as_row_key(&self.last_scanned_key).unwrap().len();
                self.last_scanned_key.truncate(row_key_len);
            }
            range.set_start(self.last_scanned_key.clone());
        }
        true
    }

    pub fn collect_statistics_into(&mut self, stats: &mut Statistics) {
        stats.add(&self.statistics_backlog);
        stats.add(&self.scanner.take_statistics());
        self.statistics_backlog = Statistics::default();
    }
}
