// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterFactory,
    DBCompactionFilter,
};
use engine_rocks::{RocksTTLProperties, RocksUserCollectedPropertiesNoRc};
use engine_traits::util::get_expire_ts;
use engine_traits::{Iterator, MiscExt, Mutable, MvccProperties, SeekKey, WriteBatch, CF_WRITE};
use prometheus::{local::*, *};
use tikv_util::time::UnixSecs;

const TEST_CURRENT_TS: u64 = 100;

pub struct TTLCompactionFilterFactory;

impl TTLCompactionFilterFactory {
    #[cfg(not(test))]
    #[inline]
    fn current_ts(&self) -> u64 {
        UnixSecs::now().into_inner()
    }

    #[cfg(test)]
    #[inline]
    fn current_ts(&self) -> u64 {
        TEST_CURRENT_TS
    }
}

impl CompactionFilterFactory for TTLCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let current = self.current_ts();

        let mut min_expire_ts = u64::MAX;
        for i in 0..context.file_numbers().len() {
            let table_props = context.table_properties(i);
            let user_props = unsafe {
                &*(table_props.user_collected_properties() as *const _
                    as *const RocksUserCollectedPropertiesNoRc)
            };
            if let Ok(props) = RocksTTLProperties::decode(user_props) {
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
    fn filter(
        &mut self,
        _level: usize,
        _key: &[u8],
        value: &[u8],
        _new_value: &mut Vec<u8>,
        _value_changed: &mut bool,
    ) -> bool {
        let expire_ts = get_expire_ts(&value).unwrap_or(0);
        if expire_ts == 0 {
            return false;
        }
        expire_ts < self.ts
    }
}
