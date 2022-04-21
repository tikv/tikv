// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::marker::PhantomData;

use crate::server::metrics::TTL_CHECKER_ACTIONS_COUNTER_VEC;
use api_version::{KeyMode, KvFormat, RawValue};
use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
    CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter,
};
use engine_rocks::RocksTtlProperties;
use engine_traits::raw_ttl::ttl_current_ts;

#[derive(Default)]
pub struct TtlCompactionFilterFactory<F: KvFormat> {
    _phantom: PhantomData<F>,
}

impl<F: KvFormat> CompactionFilterFactory for TtlCompactionFilterFactory<F> {
    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let current = ttl_current_ts();

        let mut min_expire_ts = u64::MAX;
        for i in 0..context.file_numbers().len() {
            let table_props = context.table_properties(i);
            let user_props = table_props.user_collected_properties();
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
        let filter = TtlCompactionFilter::<F> {
            ts: current,
            _phantom: PhantomData,
        };
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct TtlCompactionFilter<F: KvFormat> {
    ts: u64,
    _phantom: PhantomData<F>,
}

impl<F: KvFormat> CompactionFilter for TtlCompactionFilter<F> {
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
        // Only consider data keys.
        if !key.starts_with(keys::DATA_PREFIX_KEY) {
            return CompactionFilterDecision::Keep;
        }
        // Only consider raw keys.
        if F::parse_key_mode(&key[keys::DATA_PREFIX_KEY.len()..]) != KeyMode::Raw {
            return CompactionFilterDecision::Keep;
        }

        match F::decode_raw_value(value) {
            Ok(RawValue {
                expire_ts: Some(expire_ts),
                ..
            }) if expire_ts <= self.ts => CompactionFilterDecision::Remove,
            Err(err) => {
                TTL_CHECKER_ACTIONS_COUNTER_VEC
                    .with_label_values(&["ts_error"])
                    .inc();
                error!(
                    "unexpected ttl key";
                    "key" => log_wrappers::Value::key(key),
                    "value" => log_wrappers::Value::value(value),
                    "err" => %err,
                );
                CompactionFilterDecision::Keep
            }
            _ => CompactionFilterDecision::Keep,
        }
    }
}
