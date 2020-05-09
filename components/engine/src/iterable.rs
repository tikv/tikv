// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::rocks::{DBIterator, ReadOptions, TableFilter, DB};
use rocksdb::TableProperties;
use tikv_util::codec::number;

use engine_traits::IterOptions as IterOption;

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
