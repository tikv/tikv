// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query::storage::{IntervalRange, OwnedKvPair, PointRange, Result as QEResult, Storage};

use crate::coprocessor::Error;
use crate::storage::Statistics;
use crate::storage::{Key, Scanner, Store};

/// A `Storage` implementation over TiKV's storage.
pub struct TiKVStorage<S: Store> {
    store: S,
    scanner: Option<S::Scanner>,
    cf_stats_backlog: Statistics,
    last_key: Option<Key>,
}

impl<S: Store> TiKVStorage<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            scanner: None,
            cf_stats_backlog: Statistics::default(),
            last_key: None,
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

    fn scan_next(&mut self) -> QEResult<Option<Vec<u8>>> {
        // Unwrap is fine because we must have called `reset_range` before calling `scan_next`.
        let key = self
            .scanner
            .as_mut()
            .unwrap()
            .next_ref()
            .map_err(Error::from)?;
        self.last_key = key;
        Ok(self.last_key.as_ref().map(|k| k.to_raw().unwrap()))
    }

    fn last_scan_value(&self) -> &[u8] {
        self.scanner.as_ref().unwrap().value()
    }

    fn scan_next_finalize(&mut self) -> QEResult<()> {
        Ok(self
            .scanner
            .as_mut()
            .unwrap()
            .next_ref_finalize(self.last_key.as_ref().unwrap())
            .map_err(Error::from)?)
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
