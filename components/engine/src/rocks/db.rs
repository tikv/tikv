// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::option::Option;

use super::{util, DBIterator, DBVector, WriteBatch, DB};
use crate::{IterOption, Iterable, Mutable, Peekable, Result};
use keys::{BasicPhysicalKey, BasicPhysicalKeySlice, PhysicalKeySlice, ToPhysicalKeySlice};

impl Peekable for DB {
    type Key = BasicPhysicalKey;

    fn get_value(
        &self,
        key: impl ToPhysicalKeySlice<BasicPhysicalKeySlice>,
    ) -> Result<Option<DBVector>> {
        let pk_slice = key.to_physical_slice_container();
        let v = self.get(pk_slice.as_physical_std_slice())?;
        Ok(v)
    }

    fn get_value_cf(
        &self,
        cf: &str,
        key: impl ToPhysicalKeySlice<BasicPhysicalKeySlice>,
    ) -> Result<Option<DBVector>> {
        let handle = util::get_cf_handle(self, cf)?;
        let pk_slice = key.to_physical_slice_container();
        let v = self.get_cf(handle, pk_slice.as_physical_std_slice())?;
        Ok(v)
    }
}

impl Iterable for DB {
    fn new_iterator(&self, iter_opt: IterOption) -> DBIterator<&DB> {
        self.iter_opt(iter_opt.build_read_opts())
    }

    fn new_iterator_cf(&self, cf: &str, iter_opt: IterOption) -> Result<DBIterator<&DB>> {
        let handle = util::get_cf_handle(self, cf)?;
        let readopts = iter_opt.build_read_opts();
        Ok(DBIterator::new_cf(self, handle, readopts))
    }
}

impl Mutable for DB {}
impl Mutable for WriteBatch {}
