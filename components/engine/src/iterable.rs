// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::DATA_KEY_PREFIX_LEN;
pub use crate::rocks::{DBIterator, ReadOptions, TableFilter, TableProperties, DB};
use crate::Result;
use tikv_util::codec::number;
use tikv_util::keybuilder::KeyBuilder;

pub use engine_traits::IterOptions as IterOption;
pub use engine_traits::SeekMode;

pub trait IterOptionsExt {
    fn build_read_opts(self) -> ReadOptions;
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

        if self.hint_min_ts().is_some() || self.hint_max_ts().is_some() {
            let ts_filter = TsFilter::new(self.hint_min_ts(), self.hint_max_ts());
            opts.set_table_filter(Box::new(ts_filter))
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
