// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::PanicDBVector;
use crate::snapshot::PanicSnapshot;
use crate::write_batch::PanicWriteBatch;
use engine_traits::{
    IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions, Result, SeekKey, SyncMutable,
    WriteOptions,
};

#[derive(Clone, Debug)]
pub struct PanicEngine;

impl KvEngine for PanicEngine {
    type Snapshot = PanicSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        panic!()
    }
    fn sync(&self) -> Result<()> {
        panic!()
    }
}

impl Peekable for PanicEngine {
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

impl SyncMutable for PanicEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
}

impl Iterable for PanicEngine {
    type Iterator = PanicEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct PanicEngineIterator;

impl Iterator for PanicEngineIterator {
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
