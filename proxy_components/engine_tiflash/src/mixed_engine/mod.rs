// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub mod elementary;
pub mod write_batch;

use std::{
    fmt::{self, Debug, Formatter},
    ops::Deref,
};

use engine_rocks::{RocksDbVector, RocksEngineIterator};
use engine_traits::{DbVector, IterOptions, Iterable, Peekable, ReadOptions, Result, SyncMutable};
use tikv_util::Either;

use crate::RocksEngine;

pub type Vector = Either<RocksDbVector, Vec<u8>>;

pub struct MixedDbVector(Vector);

impl MixedDbVector {
    pub fn from_raw(raw: Vector) -> MixedDbVector {
        MixedDbVector(raw)
    }

    pub fn from_raw_rocks(raw: RocksDbVector) -> MixedDbVector {
        MixedDbVector(Vector::Left(raw))
    }

    pub fn from_raw_ps(raw: Vec<u8>) -> MixedDbVector {
        MixedDbVector(Vector::Right(raw))
    }
}

impl DbVector for MixedDbVector {}

impl Deref for MixedDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self.0.as_ref() {
            Either::Left(r) => r.deref(),
            Either::Right(p) => p.deref(),
        }
    }
}

impl Debug for MixedDbVector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for MixedDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

impl Peekable for RocksEngine {
    type DbVector = MixedDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<MixedDbVector>> {
        if let Some(e) = self.element_engine.as_ref() {
            e.get_value_opt(opts, key)
        } else {
            Err(tikv_util::box_err!("mixed engine not inited"))
        }
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<MixedDbVector>> {
        if let Some(e) = self.element_engine.as_ref() {
            e.get_value_cf_opt(opts, cf, key)
        } else {
            Err(tikv_util::box_err!("mixed engine not inited"))
        }
    }
}

impl SyncMutable for RocksEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.element_engine.as_ref().unwrap().put(key, value);
        }
        Ok(())
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            return self.element_engine.as_ref().unwrap().put_cf(cf, key, value);
        }
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.element_engine.as_ref().unwrap().delete(key);
        }
        Ok(())
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            return self.element_engine.as_ref().unwrap().delete_cf(cf, key);
        }
        Ok(())
    }

    fn delete_range(&self, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn delete_range_cf(&self, _cf: &str, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }
}

impl Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;
    fn scan<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut f = f;
        self.element_engine
            .as_ref()
            .unwrap()
            .scan(cf, start_key, end_key, fill_cache, &mut f)
    }

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        self.element_engine.as_ref().unwrap().iterator_opt(cf, opts)
    }
}
