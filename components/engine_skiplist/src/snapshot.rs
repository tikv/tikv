// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SkiplistDBVector;
use crate::engine::{SkiplistEngine, SkiplistEngineIterator};
use crossbeam_skiplist::map::SkipMap;
use engine_traits::{
    CFNamesExt, IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, Snapshot,
};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SkiplistSnapshot {
    engine: SkiplistEngine,
}

impl Snapshot for SkiplistSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.engine.cf_names()
    }
}

impl Peekable for SkiplistSnapshot {
    type DBVector = SkiplistDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        self.engine.get_value_opt(opts, key)
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        self.engine.get_value_cf_opt(opts, cf, key)
    }
}

impl Iterable for SkiplistSnapshot {
    type Iterator = SkiplistEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        self.engine.iterator_opt(opts)
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        self.engine.iterator_cf_opt(cf, opts)
    }
}
