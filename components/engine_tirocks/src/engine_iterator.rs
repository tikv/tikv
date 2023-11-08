// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::Result;
use tikv_util::codec::number;
use tirocks::{
    option::ReadOptions, properties::table::builtin::TableProperties, table_filter::TableFilter,
    Db, Iterator, Snapshot,
};

use crate::r2e;

pub struct RocksIterator<'a, D>(Iterator<'a, D>);

impl<'a, D> RocksIterator<'a, D> {
    pub fn from_raw(iter: Iterator<'a, D>) -> Self {
        RocksIterator(iter)
    }

    pub fn sequence(&self) -> Option<u64> {
        self.0.sequence_number()
    }
}

impl<'a, D: Send> engine_traits::Iterator for RocksIterator<'a, D> {
    #[inline]
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.0.seek(key);
        self.valid()
    }

    #[inline]
    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.0.seek_for_prev(key);
        self.valid()
    }

    #[inline]
    fn seek_to_first(&mut self) -> Result<bool> {
        self.0.seek_to_first();
        self.valid()
    }

    #[inline]
    fn seek_to_last(&mut self) -> Result<bool> {
        self.0.seek_to_last();
        self.valid()
    }

    #[inline]
    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e(tirocks::Status::with_code(
                tirocks::Code::kInvalidArgument,
            )));
        }
        self.0.prev();
        self.valid()
    }

    #[inline]
    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e(tirocks::Status::with_code(
                tirocks::Code::kInvalidArgument,
            )));
        }
        self.0.next();
        self.valid()
    }

    #[inline]
    fn key(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.key()
    }

    #[inline]
    fn value(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.value()
    }

    #[inline]
    fn valid(&self) -> Result<bool> {
        if self.0.valid() {
            Ok(true)
        } else {
            self.0.check().map_err(r2e)?;
            Ok(false)
        }
    }
}

/// A filter that will only read blocks which have versions overlapping with
/// [`hint_min_ts, `hint_max_ts`].
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
    fn filter(&self, props: &TableProperties) -> bool {
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

/// Convert an `IterOptions` to rocksdb `ReadOptions`.
pub fn to_tirocks_opt(iter_opt: engine_traits::IterOptions) -> ReadOptions {
    let mut opt = ReadOptions::default();
    opt.set_fill_cache(iter_opt.fill_cache())
        .set_max_skippable_internal_keys(iter_opt.max_skippable_internal_keys());
    if iter_opt.key_only() {
        opt.set_key_only(true);
    }
    if iter_opt.total_order_seek_used() {
        opt.set_total_order_seek(true);
        // TODO: enable it.
        opt.set_auto_prefix_mode(false);
    } else if iter_opt.prefix_same_as_start() {
        opt.set_prefix_same_as_start(true);
    }
    // TODO: enable it.
    opt.set_adaptive_readahead(false);

    if iter_opt.hint_min_ts().is_some() || iter_opt.hint_max_ts().is_some() {
        opt.set_table_filter(TsFilter::new(
            iter_opt.hint_min_ts(),
            iter_opt.hint_max_ts(),
        ));
    }

    let (lower, upper) = iter_opt.build_bounds();
    if let Some(lower) = lower {
        opt.set_iterate_lower_bound(lower);
    }
    if let Some(upper) = upper {
        opt.set_iterate_upper_bound(upper);
    }
    opt
}

pub type RocksEngineIterator = RocksIterator<'static, Arc<Db>>;
pub type RocksSnapIterator = RocksIterator<'static, Arc<Snapshot<'static, Arc<Db>>>>;
