// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

use crate::server::metrics::TTL_CHECKER_ACTIONS_COUNTER_VEC;
use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
    CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter,
};
use engine_rocks::{RocksTtlProperties, RocksUserCollectedPropertiesNoRc};
use engine_traits::util::{current_ts, get_expire_ts};

pub struct TTLCompactionFilterFactory;

impl CompactionFilterFactory for TTLCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let current = current_ts();

        let mut min_expire_ts = u64::MAX;
        for i in 0..context.file_numbers().len() {
            let table_props = context.table_properties(i);
            let user_props = unsafe {
                &*(table_props.user_collected_properties() as *const _
                    as *const RocksUserCollectedPropertiesNoRc)
            };
            if let Ok(props) = RocksTtlProperties::decode(user_props) {
                if props.min_expire_ts != 0 {
                    min_expire_ts = std::cmp::min(min_expire_ts, props.min_expire_ts);
                }
            }
        }
        if min_expire_ts > current {
            return std::ptr::null_mut();
        }

        let name = CString::new("ttl_compaction_filter").unwrap();
        let filter = Box::new(TTLCompactionFilter { ts: current });
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct TTLCompactionFilter {
    ts: u64,
}

impl CompactionFilter for TTLCompactionFilter {
    fn featured_filter(
        &mut self,
        _level: usize,
        key: &[u8],
        _sequence: u64,
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if value_type != CompactionFilterValueType::Value {
            return CompactionFilterDecision::Keep;
        }
        // only consider data keys
        if !key.starts_with(keys::DATA_PREFIX_KEY) {
            return CompactionFilterDecision::Keep;
        }

        let expire_ts = get_expire_ts(&value).unwrap_or_else(|_| {
            TTL_CHECKER_ACTIONS_COUNTER_VEC
                .with_label_values(&["ts_error"])
                .inc();
            error!("unexpected ttl key:{:?}, value:{:?}", key, value);
            0
        });
        if expire_ts == 0 {
            return CompactionFilterDecision::Keep;
        }
        if expire_ts <= self.ts {
            CompactionFilterDecision::Remove
        } else {
            CompactionFilterDecision::Keep
        }
    }
}
