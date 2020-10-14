// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SkiplistDBVector;
use crate::engine::{SkiplistEngine, SkiplistEngineIterator};
use crossbeam_skiplist::map::SkipMap;
use engine_traits::{
    CFNamesExt, IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, Snapshot,
    CF_DEFAULT,
};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SkiplistSnapshot {
    engine: SkiplistEngine,
    version: u64,
}

impl SkiplistSnapshot {
    pub fn new(engine: SkiplistEngine, version: u64) -> Self {
        Self { engine, version }
    }
}

impl Snapshot for SkiplistSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.engine.cf_names()
    }
}

impl Peekable for SkiplistSnapshot {
    type DBVector = SkiplistDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        let value = self.engine.get_version(opts, cf, key, Some(self.version))?;
        Ok(value.map(|(v, _)| SkiplistDBVector(v)))
    }
}

impl Iterable for SkiplistSnapshot {
    type Iterator = SkiplistEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        self.iterator_cf_opt(CF_DEFAULT, opts)
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let engine = self.engine.get_cf_engine(cf)?.clone();
        let lower_bound = opts.lower_bound().map(|e| e.to_vec());
        let upper_bound = opts.upper_bound().map(|e| e.to_vec());
        Ok(SkiplistEngineIterator::new(
            self.engine.name,
            engine,
            lower_bound,
            upper_bound,
            self.version,
        ))
    }
}
