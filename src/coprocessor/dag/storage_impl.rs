// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_common::storage::{
    IntervalRange, OwnedKvPairEntry, PointRange, Result as QeResult, Storage,
};
use txn_types::Key;

use crate::{
    coprocessor::Error,
    storage::{Scanner, Statistics, Store, mvcc::NewerTsCheckState},
};

/// A `Storage` implementation over TiKV's storage.
pub struct TikvStorage<S: Store> {
    store: S,
    scanner: Option<S::Scanner>,
    cf_stats_backlog: Statistics,
    met_newer_ts_data_backlog: NewerTsCheckState,
}

impl<S: Store> TikvStorage<S> {
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

impl<S: Store> Storage for TikvStorage<S> {
    type Statistics = Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        load_commit_ts: bool,
        range: IntervalRange,
    ) -> QeResult<()> {
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
                    load_commit_ts,
                    lower,
                    upper,
                )
                .map_err(Error::from)?,
            // There is no transform from storage error to QE's StorageError,
            // so an intermediate error is needed.
        );
        Ok(())
    }

    fn scan_next_entry(&mut self) -> QeResult<Option<OwnedKvPairEntry>> {
        // Unwrap is fine because we must have called `reset_range` before calling
        // `scan_next`.
        let kv = self
            .scanner
            .as_mut()
            .unwrap()
            .next_entry()
            .map_err(Error::from)?;
        Ok(kv.map(|(k, v)| OwnedKvPairEntry {
            key: k.into_raw().unwrap(),
            value: v.value,
            commit_ts: v.commit_ts.map(|ts| ts.into_inner()),
        }))
    }

    fn get_entry(
        &mut self,
        _is_key_only: bool,
        load_commit_ts: bool,
        range: PointRange,
    ) -> QeResult<Option<OwnedKvPairEntry>> {
        // TODO: Default CF does not need to be accessed if KeyOnly.
        // TODO: No need to check newer ts data if self.scanner has met newer ts data.
        let key = range.0;
        let entry = self
            .store
            .incremental_get_entry(&Key::from_raw(&key), load_commit_ts)
            .map_err(Error::from)?;
        Ok(entry.map(move |e| OwnedKvPairEntry {
            key,
            value: e.value,
            commit_ts: e.commit_ts.map(|ts| ts.into_inner()),
        }))
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
