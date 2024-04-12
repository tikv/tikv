// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;

use engine_traits::{
    CfNamesExt, IterMetricsCollector, IterOptions, Iterable, Iterator, MetricsExt, Peekable,
    ReadOptions, Result, Snapshot, SnapshotMiscExt,
};

use crate::{db_vector::PanicDbVector, engine::PanicEngine};

#[derive(Clone, Debug)]
pub struct PanicSnapshot;

impl Snapshot for PanicSnapshot {}

impl Peekable for PanicSnapshot {
    type DbVector = PanicDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        panic!()
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        panic!()
    }
}

impl Iterable for PanicSnapshot {
    type Iterator = PanicSnapshotIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

impl CfNamesExt for PanicSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}

pub struct PanicSnapshotIterator;

impl Iterator for PanicSnapshotIterator {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        panic!()
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        panic!()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}

impl SnapshotMiscExt for PanicSnapshot {
    fn sequence_number(&self) -> u64 {
        panic!()
    }
}

pub struct PanicSnapshotIterMetricsCollector;

impl IterMetricsCollector for PanicSnapshotIterMetricsCollector {
    fn internal_delete_skipped_count(&self) -> usize {
        panic!()
    }

    fn internal_key_skipped_count(&self) -> usize {
        panic!()
    }
}

impl MetricsExt for PanicSnapshotIterator {
    type Collector = PanicSnapshotIterMetricsCollector;
    fn metrics_collector(&self) -> Self::Collector {
        panic!()
    }
}
