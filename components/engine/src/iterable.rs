// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::DATA_KEY_PREFIX_LEN;
pub use crate::rocks::{DBIterator, ReadOptions, DB};
use crate::Result;
use tikv_util::keybuilder::KeyBuilder;

#[derive(Clone, PartialEq)]
enum SeekMode {
    TotalOrder,
    Prefix,
}

pub struct IterOption {
    lower_bound: Option<KeyBuilder>,
    upper_bound: Option<KeyBuilder>,
    prefix_same_as_start: bool,
    fill_cache: bool,
    // only supported when Titan enabled, otherwise it doesn't take effect.
    titan_key_only: bool,
    seek_mode: SeekMode,
}

impl IterOption {
    pub fn new(
        lower_bound: Option<KeyBuilder>,
        upper_bound: Option<KeyBuilder>,
        fill_cache: bool,
    ) -> IterOption {
        IterOption {
            lower_bound,
            upper_bound,
            prefix_same_as_start: false,
            fill_cache,
            titan_key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }

    #[inline]
    pub fn use_prefix_seek(mut self) -> IterOption {
        self.seek_mode = SeekMode::Prefix;
        self
    }

    #[inline]
    pub fn total_order_seek_used(&self) -> bool {
        self.seek_mode == SeekMode::TotalOrder
    }

    #[inline]
    pub fn fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }

    #[inline]
    pub fn titan_key_only(&mut self, v: bool) {
        self.titan_key_only = v;
    }

    #[inline]
    pub fn lower_bound(&self) -> Option<&[u8]> {
        self.lower_bound.as_ref().map(|v| v.as_slice())
    }

    #[inline]
    pub fn set_lower_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = KeyBuilder::from_slice(bound, reserved_prefix_len, 0);
        self.lower_bound = Some(builder);
    }

    pub fn set_vec_lower_bound(&mut self, bound: Vec<u8>) {
        self.lower_bound = Some(KeyBuilder::from_vec(bound, 0, 0));
    }

    pub fn set_lower_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.lower_bound {
            builder.set_prefix(prefix);
        }
    }

    #[inline]
    pub fn upper_bound(&self) -> Option<&[u8]> {
        self.upper_bound.as_ref().map(|v| v.as_slice())
    }

    #[inline]
    pub fn set_upper_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = KeyBuilder::from_slice(bound, reserved_prefix_len, 0);
        self.upper_bound = Some(builder);
    }

    pub fn set_vec_upper_bound(&mut self, bound: Vec<u8>) {
        self.upper_bound = Some(KeyBuilder::from_vec(bound, 0, 0));
    }

    pub fn set_upper_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.upper_bound {
            builder.set_prefix(prefix);
        }
    }

    #[inline]
    pub fn set_prefix_same_as_start(mut self, enable: bool) -> IterOption {
        self.prefix_same_as_start = enable;
        self
    }

    pub fn build_read_opts(self) -> ReadOptions {
        let mut opts = ReadOptions::new();
        opts.fill_cache(self.fill_cache);
        if self.titan_key_only {
            opts.set_titan_key_only(true);
        }
        if self.total_order_seek_used() {
            opts.set_total_order_seek(true);
        } else if self.prefix_same_as_start {
            opts.set_prefix_same_as_start(true);
        }
        if let Some(builder) = self.lower_bound {
            opts.set_iterate_lower_bound(builder.build());
        }
        if let Some(builder) = self.upper_bound {
            opts.set_iterate_upper_bound(builder.build());
        }
        opts
    }
}

impl Default for IterOption {
    fn default() -> IterOption {
        IterOption {
            lower_bound: None,
            upper_bound: None,
            prefix_same_as_start: false,
            fill_cache: true,
            titan_key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
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

    // Seek the first key >= given key, if no found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator(IterOption::default());
        iter.seek(key.into());
        Ok(iter.kv())
    }

    // Seek the first key >= given key, if no found, return None.
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
