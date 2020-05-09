// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::PanicDBVector;
use crate::engine::PanicEngine;
use engine_traits::{
    IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, Snapshot, SyncSnapshot,
};
use std::ops::Deref;

#[derive(Clone, Debug)]
pub struct PanicSnapshot;

impl Snapshot for PanicSnapshot {
    type SyncSnapshot = PanicSyncSnapshot;

    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
    fn into_sync(self) -> Self::SyncSnapshot {
        panic!()
    }
}

impl Peekable for PanicSnapshot {
    type DBVector = PanicDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        panic!()
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        panic!()
    }
}

impl Iterable for PanicSnapshot {
    type Iterator = PanicSnapshotIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct PanicSnapshotIterator;

impl Iterator for PanicSnapshotIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
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

#[derive(Clone, Debug)]
pub struct PanicSyncSnapshot;

impl SyncSnapshot<PanicSnapshot> for PanicSyncSnapshot {}

impl Deref for PanicSyncSnapshot {
    type Target = PanicSnapshot;

    fn deref(&self) -> &Self::Target {
        panic!()
    }
}
