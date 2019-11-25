// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::DATA_KEY_PREFIX_LEN;
pub use crate::rocks::{DBIterator, ReadOptions, DB};
use crate::Result;
use tikv_util::keybuilder::KeyBuilder;

pub use engine_traits::IterOptions as IterOption;
pub use engine_traits::SeekMode;

pub trait IterOptionsExt {
    fn build_read_opts(self) -> ReadOptions;
}

impl IterOptionsExt for IterOption {
    fn build_read_opts(self) -> ReadOptions {
        let mut opts = ReadOptions::new();
        opts.fill_cache(self.fill_cache());
        if self.key_only() {
            opts.set_titan_key_only(true);
        }
        if self.total_order_seek_used() {
            opts.set_total_order_seek(true);
        } else if self.prefix_same_as_start() {
            opts.set_prefix_same_as_start(true);
        }
        let (lower, upper) = self.build_bounds();
        if let Some(lower) = lower {
            opts.set_iterate_lower_bound(lower);
        }
        if let Some(upper) = upper {
            opts.set_iterate_upper_bound(upper);
        }
        opts
    }
}

// TODO: refactor this trait into rocksdb trait.
pub trait Iterable {
    fn new_iterator(&self, iter_opt: IterOption) -> DBIterator<&DB>;
    fn new_iterator_cf(&self, _: &str, iter_opt: IterOption) -> Result<DBIterator<&DB>>;
    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_KEY_PREFIX_LEN, 0);
        let end = KeyBuilder::from_slice(end_key, DATA_KEY_PREFIX_LEN, 0);
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
        scan_impl(self.new_iterator(iter_opt), start_key, f)
    }

    // like `scan`, only on a specific column family.
    fn scan_cf<F>(
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
        let start = KeyBuilder::from_slice(start_key, DATA_KEY_PREFIX_LEN, 0);
        let end = KeyBuilder::from_slice(end_key, DATA_KEY_PREFIX_LEN, 0);
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
        scan_impl(self.new_iterator_cf(cf, iter_opt)?, start_key, f)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator(IterOption::default());
        iter.seek(key.into());
        Ok(iter.kv())
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek_cf(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator_cf(cf, IterOption::default())?;
        iter.seek(key.into());
        Ok(iter.kv())
    }
}

fn scan_impl<F>(mut it: DBIterator<&DB>, start_key: &[u8], mut f: F) -> Result<()>
where
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    it.seek(start_key.into());
    while it.valid() {
        let r = f(it.key(), it.value())?;

        if !r || !it.next() {
            break;
        }
    }

    it.status().map_err(From::from)
}
