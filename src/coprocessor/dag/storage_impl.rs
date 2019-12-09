// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query::storage::{IntervalRange, OwnedKvPair, PointRange, Result as QEResult, Storage};

use crate::coprocessor::Error;
use crate::storage::Statistics;
use crate::storage::{Scanner, Store};
use keys::Key;

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
    ) -> QEResult<()> {
        if let Some(scanner) = &mut self.scanner {
            self.cf_stats_backlog.add(&scanner.take_statistics());
        }
        let lower = Some(Key::from_raw(&range.lower_inclusive));
        let upper = Some(Key::from_raw(&range.upper_exclusive));
        self.scanner = Some(
            self.store
                .scanner(is_backward_scan, is_key_only, lower, upper)
                .map_err(Error::from)?,
            // There is no transform from storage error to QE's StorageError,
            // so an intermediate error is needed.
        );
        Ok(())
    }

    fn scan_next(&mut self) -> QEResult<Option<OwnedKvPair>> {
        // Unwrap is fine because we must have called `reset_range` before calling `scan_next`.
        let kv = self.scanner.as_mut().unwrap().next().map_err(Error::from)?;
        Ok(kv.map(|(k, v)| (k.into_raw().unwrap(), v)))
    }

    fn get(&mut self, _is_key_only: bool, range: PointRange) -> QEResult<Option<OwnedKvPair>> {
        // TODO: Default CF does not need to be accessed if KeyOnly.
        let key = range.0;
        let value = self
            .store
            .incremental_get(&Key::from_raw(&key))
            .map_err(Error::from)?;
        Ok(value.map(move |v| (key, v)))
    }

    fn collect_statistics(&mut self, dest: &mut Statistics) {
        self.cf_stats_backlog
            .add(&self.store.incremental_get_take_statistics());
        if let Some(scanner) = &mut self.scanner {
            self.cf_stats_backlog.add(&scanner.take_statistics());
        }
        dest.add(&self.cf_stats_backlog);
        self.cf_stats_backlog = Statistics::default();
    }
}
