// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::PanicDBVector;
use crate::snapshot::PanicSnapshot;
use crate::write_batch::PanicWriteBatch;
use engine_traits::{
    IterOptions, Iterable, Iterator, KvEngine, Mutable, Peekable, ReadOptions, Result, SeekKey,
    WriteOptions,
};

#[derive(Clone, Debug)]
pub struct PanicEngine;

impl KvEngine for PanicEngine {
    type Snapshot = PanicSnapshot;
    type WriteBatch = PanicWriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::WriteBatch) -> Result<()> {
        panic!()
    }
    fn write_batch(&self) -> Self::WriteBatch {
        panic!()
    }
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        panic!()
    }
    fn snapshot(&self) -> Self::Snapshot {
        panic!()
    }
    fn sync(&self) -> Result<()> {
        panic!()
    }
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
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

impl Mutable for PanicEngine {
    fn put_opt(&self, opts: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_cf_opt(&self, opts: &WriteOptions, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete_opt(&self, opts: &WriteOptions, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_cf_opt(&self, opts: &WriteOptions, cf: &str, key: &[u8]) -> Result<()> {
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
