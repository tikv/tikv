// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::option::Option;

use super::{util, DBIterator, DBVector, WriteBatch, DB};
use crate::iterable::IterOptionsExt;
use crate::{IterOption, Iterable, Mutable, Peekable, Result};

impl Peekable for DB {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        let v = self.get(key)?;
        Ok(v)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>> {
        let handle = util::get_cf_handle(self, cf)?;
        let v = self.get_cf(handle, key)?;
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
