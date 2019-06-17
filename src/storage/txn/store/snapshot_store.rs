// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::IsolationLevel;

use crate::storage::metrics::*;
use crate::storage::mvcc::MvccReader;
use crate::storage::mvcc::{Scanner as MvccScanner, ScannerBuilder};
use crate::storage::txn::{Error, Result};
use crate::storage::{Key, Snapshot, Statistics, Value};

pub struct SnapshotStore<S: Snapshot> {
    snapshot: S,
    start_ts: u64,
    isolation_level: IsolationLevel,
    fill_cache: bool,
}

impl<S: Snapshot> super::Store for SnapshotStore<S> {
    type Scanner = MvccScanner<S>;

    #[inline]
    fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>> {
        let mut reader = MvccReader::new(
            self.snapshot.clone(),
            None,
            self.fill_cache,
            None,
            None,
            self.isolation_level,
        );
        let v = reader.get(key, self.start_ts)?;
        statistics.add(reader.get_statistics());
        Ok(v)
    }

    #[inline]
    fn batch_get(&self, keys: &[Key], statistics: &mut Statistics) -> Vec<Result<Option<Value>>> {
        // TODO: sort the keys and use ScanMode::Forward
        let mut reader = MvccReader::new(
            self.snapshot.clone(),
            None,
            self.fill_cache,
            None,
            None,
            self.isolation_level,
        );
        let mut results = Vec::with_capacity(keys.len());
        for k in keys {
            results.push(reader.get(k, self.start_ts).map_err(Error::from));
        }
        statistics.add(reader.get_statistics());
        results
    }

    #[inline]
    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<MvccScanner<S>> {
        // Check request bounds with physical bound
        self.verify_range(&lower_bound, &upper_bound)?;
        let scanner = ScannerBuilder::new(self.snapshot.clone(), self.start_ts, desc)
            .range(lower_bound, upper_bound)
            .omit_value(key_only)
            .fill_cache(self.fill_cache)
            .isolation_level(self.isolation_level)
            .build()?;

        Ok(scanner)
    }
}

impl<S: Snapshot> SnapshotStore<S> {
    pub fn new(
        snapshot: S,
        start_ts: u64,
        isolation_level: IsolationLevel,
        fill_cache: bool,
    ) -> Self {
        SnapshotStore {
            snapshot,
            start_ts,
            isolation_level,
            fill_cache,
        }
    }

    fn verify_range(&self, lower_bound: &Option<Key>, upper_bound: &Option<Key>) -> Result<()> {
        if let Some(ref l) = lower_bound {
            if let Some(b) = self.snapshot.lower_bound() {
                if !b.is_empty() && l.as_encoded().as_slice() < b {
                    REQUEST_EXCEED_BOUND.inc();
                    return Err(Error::InvalidReqRange {
                        start: Some(l.as_encoded().clone()),
                        end: upper_bound.as_ref().map(|ref b| b.as_encoded().clone()),
                        lower_bound: Some(b.to_vec()),
                        upper_bound: self.snapshot.upper_bound().map(|b| b.to_vec()),
                    });
                }
            }
        }
        if let Some(ref u) = upper_bound {
            if let Some(b) = self.snapshot.upper_bound() {
                if !b.is_empty() && (u.as_encoded().as_slice() > b || u.as_encoded().is_empty()) {
                    REQUEST_EXCEED_BOUND.inc();
                    return Err(Error::InvalidReqRange {
                        start: lower_bound.as_ref().map(|ref b| b.as_encoded().clone()),
                        end: Some(u.as_encoded().clone()),
                        lower_bound: self.snapshot.lower_bound().map(|b| b.to_vec()),
                        upper_bound: Some(b.to_vec()),
                    });
                }
            }
        }

        Ok(())
    }
}
