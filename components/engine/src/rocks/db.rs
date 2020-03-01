// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::{util, DBIterator, DB};
use crate::iterable::IterOptionsExt;
use crate::{IterOption, Iterable, Result};

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
