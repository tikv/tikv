// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

use crate::server::metrics::TTL_CHECKER_ACTIONS_COUNTER_VEC;
use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
    CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter,
};
use engine_rocks::RocksTtlProperties;
use engine_traits::key_prefix::{self};
use engine_traits::raw_value::{ttl_current_ts, RawValue};
use kvproto::kvrpcpb::ApiVersion;

pub struct TTLCompactionFilterFactory {
    pub api_version: ApiVersion,
}

impl CompactionFilterFactory for TTLCompactionFilterFactory {
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
        let filter = TTLCompactionFilter {
            ts: current,
            api_version: self.api_version,
        };
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct TTLCompactionFilter {
    ts: u64,
    api_version: ApiVersion,
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
        // Only consider data keys.
        if !key.starts_with(keys::DATA_PREFIX_KEY) {
            return CompactionFilterDecision::Keep;
        }
        // Only consider raw keys.
        match self.api_version {
            // TTL is not enabled in V1.
            ApiVersion::V1 => unreachable!(),
            // In V1TTL, txnkv is disabled, so all data keys are raw keys.
            ApiVersion::V1ttl => (),
            ApiVersion::V2 => {
                let origin_key = &key[keys::DATA_PREFIX_KEY.len()..];
                if !key_prefix::is_raw_key(origin_key) {
                    return CompactionFilterDecision::Keep;
                }
            }
        }

        match RawValue::from_bytes(value, self.api_version) {
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
