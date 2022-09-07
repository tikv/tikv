// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;

use engine_traits::{IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, Snapshot};

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
