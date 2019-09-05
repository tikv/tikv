// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::coprocessor::dag::storage::{IntervalRange, OwnedKvPair, PointRange, Storage};
use crate::coprocessor::Result;
use crate::storage::Statistics;
use crate::storage::{Key, Scanner, Store};

/// A `Storage` implementation over TiKV's storage.
pub struct TiKVStorage<S: Store> {
    store: S,
    scanner: Option<S::Scanner>,
    cf_stats_backlog: Statistics,
}

impl<S: Store> TiKVStorage<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            scanner: None,
            cf_stats_backlog: Statistics::default(),
        }
    }
}

impl<S: Store> From<S> for TiKVStorage<S> {
    fn from(store: S) -> Self {
        TiKVStorage::new(store)
    }
}

impl<S: Store> Storage for TiKVStorage<S> {
    type Statistics = Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        range: IntervalRange,
    ) -> Result<()> {
        if let Some(scanner) = &mut self.scanner {
            self.cf_stats_backlog.add(&scanner.take_statistics());
        }
        let lower = Some(Key::from_raw(&range.lower_inclusive));
        let upper = Some(Key::from_raw(&range.upper_exclusive));
        self.scanner = Some(
            self.store
                .scanner(is_backward_scan, is_key_only, lower, upper)?,
        );
        Ok(())
    }

    fn scan_next(&mut self) -> Result<Option<OwnedKvPair>> {
        // Unwrap is fine because we must have called `reset_range` before calling `scan_next`.
        let kv = self.scanner.as_mut().unwrap().next()?;
        Ok(kv.map(|(k, v)| (k.into_raw().unwrap(), v)))
    }

    fn get(&mut self, _is_key_only: bool, range: PointRange) -> Result<Option<OwnedKvPair>> {
        // TODO: Default CF does not need to be accessed if KeyOnly.
        let key = range.0;
        let value = self
            .store
            .get(&Key::from_raw(&key), &mut self.cf_stats_backlog)?;
        Ok(value.map(move |v| (key, v)))
    }

    fn collect_statistics(&mut self, dest: &mut Statistics) {
        if let Some(scanner) = &mut self.scanner {
            self.cf_stats_backlog.add(&scanner.take_statistics());
        }
        dest.add(&self.cf_stats_backlog);
        self.cf_stats_backlog = Statistics::default();
    }
}
