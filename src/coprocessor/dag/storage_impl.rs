// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_common::storage::{
    IntervalRange, OwnedKvPair, PointRange, Result as QEResult, Storage,
};

use crate::coprocessor::Error;
use crate::storage::mvcc::NewerTsCheckState;
use crate::storage::Statistics;
use crate::storage::{Scanner, Store};
use txn_types::Key;

/// A `Storage` implementation over TiKV's storage.
pub struct TiKvStorage<S: Store> {
    store: S,
    scanner: Option<S::Scanner>,
    cf_stats_backlog: Statistics,
    met_newer_ts_data_backlog: NewerTsCheckState,
}

impl<S: Store> TiKvStorage<S> {
    pub fn new(store: S, check_can_be_cached: bool) -> Self {
        Self {
            store,
            scanner: None,
            cf_stats_backlog: Statistics::default(),
            met_newer_ts_data_backlog: if check_can_be_cached {
                NewerTsCheckState::NotMetYet
            } else {
                NewerTsCheckState::Unknown
            },
        }
    }
}

impl<S: Store> Storage for TiKvStorage<S> {
    type Statistics = Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        range: IntervalRange,
    ) -> QEResult<()> {
        if let Some(scanner) = &mut self.scanner {
            self.cf_stats_backlog.add(&scanner.take_statistics());
            if scanner.met_newer_ts_data() == NewerTsCheckState::Met {
                // always override if we met newer ts data
                self.met_newer_ts_data_backlog = NewerTsCheckState::Met;
            }
        }
        let lower = Some(Key::from_raw(&range.lower_inclusive));
        let upper = Some(Key::from_raw(&range.upper_exclusive));
        self.scanner = Some(
            self.store
                .scanner(
                    is_backward_scan,
                    is_key_only,
                    self.met_newer_ts_data_backlog == NewerTsCheckState::NotMetYet,
                    lower,
                    upper,
                )
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
        // TODO: No need to check newer ts data if self.scanner has met newer ts data.
        let key = range.0;
        let value = self
            .store
            .incremental_get(&Key::from_raw(&key))
            .map_err(Error::from)?;
        Ok(value.map(move |v| (key, v)))
    }

    #[inline]
    fn met_uncacheable_data(&self) -> Option<bool> {
        if let Some(scanner) = &self.scanner {
            if scanner.met_newer_ts_data() == NewerTsCheckState::Met {
                return Some(true);
            }
        }
        if self.store.incremental_get_met_newer_ts_data() == NewerTsCheckState::Met {
            return Some(true);
        }
        match self.met_newer_ts_data_backlog {
            NewerTsCheckState::Unknown => None,
            NewerTsCheckState::Met => Some(true),
            NewerTsCheckState::NotMetYet => Some(false),
        }
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
