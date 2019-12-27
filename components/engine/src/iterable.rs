// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::DATA_KEY_PREFIX_LEN;
pub use crate::rocks::{DBIterator, ReadOptions, TableFilter, TableProperties, DB};
use crate::Result;
use std::ops::Bound;
use tikv_util::codec::number;
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
    // hint for we will only scan data with commit ts >= hint_min_ts
    hint_min_ts: Option<u64>,
    // hint for we will only scan data with commit ts <= hint_max_ts
    hint_max_ts: Option<u64>,
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
            hint_min_ts: None,
            hint_max_ts: None,
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

    #[inline]
    pub fn set_hint_min_ts(&mut self, bound_ts: Bound<u64>) {
        match bound_ts {
            Bound::Included(ts) => self.hint_min_ts = Some(ts),
            Bound::Excluded(ts) => self.hint_min_ts = Some(ts + 1),
            Bound::Unbounded => self.hint_min_ts = None,
        }
    }

    #[inline]
    pub fn hint_min_ts(&self) -> Option<u64> {
        self.hint_min_ts
    }

    #[inline]
    pub fn hint_max_ts(&self) -> Option<u64> {
        self.hint_max_ts
    }

    #[inline]
    pub fn set_hint_max_ts(&mut self, bound_ts: Bound<u64>) {
        match bound_ts {
            Bound::Included(ts) => self.hint_max_ts = Some(ts),
            Bound::Excluded(ts) => self.hint_max_ts = Some(ts - 1),
            Bound::Unbounded => self.hint_max_ts = None,
        }
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

        if self.hint_min_ts().is_some() || self.hint_max_ts().is_some() {
            let ts_filter = TsFilter::new(self.hint_min_ts(), self.hint_max_ts());
            opts.set_table_filter(Box::new(ts_filter))
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
            hint_min_ts: None,
            hint_max_ts: None,
            prefix_same_as_start: false,
            fill_cache: true,
            titan_key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }
}

struct TsFilter {
    hint_min_ts: Option<u64>,
    hint_max_ts: Option<u64>,
}

impl TsFilter {
    fn new(hint_min_ts: Option<u64>, hint_max_ts: Option<u64>) -> TsFilter {
        TsFilter {
            hint_min_ts,
            hint_max_ts,
        }
    }
}

impl TableFilter for TsFilter {
    fn table_filter(&self, props: &TableProperties) -> bool {
        if self.hint_max_ts.is_none() && self.hint_min_ts.is_none() {
            return true;
        }

        let user_props = props.user_collected_properties();

        if let Some(hint_min_ts) = self.hint_min_ts {
            // TODO avoid hard code after refactor MvccProperties from
            // tikv/src/raftstore/coprocessor/ into some component about engine.
            if let Some(mut p) = user_props.get("tikv.max_ts") {
                if let Ok(get_max) = number::decode_u64(&mut p) {
                    if get_max < hint_min_ts {
                        return false;
                    }
                }
            }
        }

        if let Some(hint_max_ts) = self.hint_max_ts {
            // TODO avoid hard code after refactor MvccProperties from
            // tikv/src/raftstore/coprocessor/ into some component about engine.
            if let Some(mut p) = user_props.get("tikv.min_ts") {
                if let Ok(get_min) = number::decode_u64(&mut p) {
                    if get_min > hint_max_ts {
                        return false;
                    }
                }
            }
        }

        true
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
    // TODO: Make it zero-copy.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator(IterOption::default());
        if iter.seek(key.into())? {
            let (k, v) = (iter.key().to_vec(), iter.value().to_vec());
            return Ok(Some((k, v)));
        }
        Ok(None)
    }

    // Seek the first key >= given key, if not found, return None.
    // TODO: Make it zero-copy.
    fn seek_cf(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator_cf(cf, IterOption::default())?;
        if iter.seek(key.into())? {
            let (k, v) = (iter.key().to_vec(), iter.value().to_vec());
            return Ok(Some((k, v)));
        }
        Ok(None)
    }
}

fn scan_impl<F>(mut it: DBIterator<&DB>, start_key: &[u8], mut f: F) -> Result<()>
where
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    let mut remained = it.seek(start_key.into())?;
    while remained {
        remained = f(it.key(), it.value())? && it.next()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    #[test]
    fn test_hint_ts() {
        let mut ops = IterOption::default();
        assert_eq!(ops.hint_min_ts(), None);
        assert_eq!(ops.hint_max_ts(), None);

        ops.set_hint_min_ts(Bound::Included(1));
        ops.set_hint_max_ts(Bound::Included(10));
        assert_eq!(ops.hint_min_ts(), Some(1));
        assert_eq!(ops.hint_max_ts(), Some(10));

        ops.set_hint_min_ts(Bound::Excluded(1));
        ops.set_hint_max_ts(Bound::Excluded(10));
        assert_eq!(ops.hint_min_ts(), Some(2));
        assert_eq!(ops.hint_max_ts(), Some(9));
    }
}
