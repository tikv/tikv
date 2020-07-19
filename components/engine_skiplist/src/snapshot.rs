// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SkiplistDBVector;
use crate::engine::SkiplistEngine;
use crossbeam_skiplist::map::SkipMap;
use engine_traits::{
    IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, Snapshot,
};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SkiplistSnapshot {
    inner: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
}

impl Snapshot for SkiplistSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}

impl Peekable for SkiplistSnapshot {
    type DBVector = SkiplistDBVector;

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

impl Iterable for SkiplistSnapshot {
    type Iterator = SkiplistSnapshotIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct SkiplistSnapshotIterator;

impl Iterator for SkiplistSnapshotIterator {
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
