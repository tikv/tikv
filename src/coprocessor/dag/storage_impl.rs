// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_common::storage::{
    IntervalRange, OwnedKvPairEntry, PointRange, Result as QeResult, Storage,
};
use txn_types::Key;

use crate::{
    coprocessor::Error,
    storage::{Scanner, Statistics, Store, kv::kv_processed_size, mvcc::NewerTsCheckState},
};

/// A `Storage` implementation over TiKV's storage.
pub struct TikvStorage<S: Store> {
    store: S,
    scanner: Option<S::Scanner>,
    cf_stats_backlog: Statistics,
    met_newer_ts_data_backlog: NewerTsCheckState,
    /// Cumulative scanned bytes (encoded key + value of every returned entry),
    /// mirroring `Statistics::processed_size`: counts only visible returned
    /// entries, not skipped MVCC versions (old versions/tombstones/write
    /// records) — i.e. processed_size, not physical MVCC scanned bytes. Never
    /// drained; only peeked via `scanned_bytes` for the `paging_size_bytes`
    /// budget, separate from the `collect_statistics` path.
    scanned_bytes: usize,
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
            scanned_bytes: 0,
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
        Ok(kv.map(|(k, v)| {
            // Mirror `Statistics::processed_size`: encoded key + value bytes.
            self.scanned_bytes += kv_processed_size(k.len(), v.value.len());
            OwnedKvPairEntry {
                key: k.into_raw().unwrap(),
                value: v.value,
                commit_ts: v.commit_ts.map(|ts| ts.into_inner()),
            }
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
        let encoded_key = Key::from_raw(&key);
        let entry = self
            .store
            .incremental_get_entry(&encoded_key, load_commit_ts)
            .map_err(Error::from)?;
        if let Some(e) = &entry {
            // Mirror `Statistics::processed_size` for point gets: encoded key +
            // value bytes.
            self.scanned_bytes += kv_processed_size(encoded_key.len(), e.value.len());
        }
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

    #[inline]
    fn scanned_bytes(&self) -> usize {
        self.scanned_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use txn_types::ValueEntry;

    use super::*;
    use crate::storage::txn::FixtureStore;

    #[test]
    fn test_scanned_bytes_accumulation() {
        // scanned_bytes accumulates kv_processed_size (encoded key + value)
        // for both the interval-scan and the point-get paths, never resets,
        // and stays independent of the draining collect_statistics path.
        let rows: &[(&[u8], &[u8])] = &[
            (b"row_a", b"value_1"),
            (b"row_b", b"v2"),
            (b"row_c", b"another_value"),
        ];
        let data: BTreeMap<_, _> = rows
            .iter()
            .map(|(k, v)| (Key::from_raw(k), Ok(ValueEntry::from_value(v.to_vec()))))
            .collect();
        let mut storage = TikvStorage::new(FixtureStore::new(data), false);

        assert_eq!(storage.scanned_bytes(), 0);

        // Interval scan: every returned entry adds encoded key + value bytes.
        storage
            .begin_scan(false, false, false, IntervalRange::from(("row_a", "row_z")))
            .unwrap();
        let mut expected = 0;
        for _ in 0..rows.len() {
            let entry = storage.scan_next_entry().unwrap().unwrap();
            expected += kv_processed_size(Key::from_raw(&entry.key).len(), entry.value.len());
            assert_eq!(storage.scanned_bytes(), expected);
        }
        assert!(storage.scan_next_entry().unwrap().is_none());
        assert_eq!(storage.scanned_bytes(), expected);

        // Point get: a hit adds encoded key + value bytes, a miss adds
        // nothing.
        let hit = storage
            .get_entry(false, false, PointRange::from("row_b"))
            .unwrap()
            .unwrap();
        expected += kv_processed_size(Key::from_raw(b"row_b").len(), hit.value.len());
        assert_eq!(storage.scanned_bytes(), expected);
        assert!(
            storage
                .get_entry(false, false, PointRange::from("row_x"))
                .unwrap()
                .is_none()
        );
        assert_eq!(storage.scanned_bytes(), expected);

        // Draining statistics must not reset the cumulative counter.
        let mut stats = Statistics::default();
        storage.collect_statistics(&mut stats);
        assert_eq!(storage.scanned_bytes(), expected);
    }
}
