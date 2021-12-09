// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use crate::decode_properties::DecodeProperties;
use crate::{RocksEngine, UserProperties};
use engine_traits::key_prefix;
use engine_traits::raw_value::RawValue;
use engine_traits::{Range, Result, TtlProperties, TtlPropertiesExt};
use kvproto::kvrpcpb::ApiVersion;
use rocksdb::{DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory};
use tikv_util::error;

const PROP_MAX_EXPIRE_TS: &str = "tikv.max_expire_ts";
const PROP_MIN_EXPIRE_TS: &str = "tikv.min_expire_ts";

pub struct RocksTtlProperties;

impl RocksTtlProperties {
    pub fn encode(ttl_props: &TtlProperties) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_MAX_EXPIRE_TS, ttl_props.max_expire_ts);
        props.encode_u64(PROP_MIN_EXPIRE_TS, ttl_props.min_expire_ts);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<TtlProperties> {
        let res = TtlProperties {
            max_expire_ts: props.decode_u64(PROP_MAX_EXPIRE_TS)?,
            min_expire_ts: props.decode_u64(PROP_MIN_EXPIRE_TS)?,
        };
        Ok(res)
    }
}

impl TtlPropertiesExt for RocksEngine {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>> {
        let range = Range::new(start_key, end_key);
        let collection = self.get_properties_of_tables_in_range(cf, &[range])?;
        if collection.is_empty() {
            return Ok(vec![]);
        }

        let mut res = Vec::new();
        for (file_name, v) in collection.iter() {
            let prop = match RocksTtlProperties::decode(v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            res.push((file_name.to_string(), prop));
        }
        Ok(res)
    }
}

/// Can only be used for default CF.
pub struct TtlPropertiesCollector {
    api_version: ApiVersion,
    prop: TtlProperties,
}

impl TablePropertiesCollector for TtlPropertiesCollector {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        if entry_type != DBEntryType::Put {
            return;
        }
        // Only consider data keys.
        if !key.starts_with(keys::DATA_PREFIX_KEY) {
            return;
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
                    return;
                }
            }
        }

        match RawValue::from_bytes(value, self.api_version) {
            Ok(RawValue {
                expire_ts: Some(expire_ts),
                ..
            }) => {
                self.prop.max_expire_ts = std::cmp::max(self.prop.max_expire_ts, expire_ts);
                if self.prop.min_expire_ts == 0 {
                    self.prop.min_expire_ts = expire_ts;
                } else {
                    self.prop.min_expire_ts = std::cmp::min(self.prop.min_expire_ts, expire_ts);
                }
            }
            Err(err) => {
                error!(
                    "failed to get expire ts";
                    "key" => log_wrappers::Value::key(key),
                    "value" => log_wrappers::Value::value(value),
                    "err" => %err,
                );
            }
            _ => {}
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        if self.prop.max_expire_ts == 0 && self.prop.min_expire_ts == 0 {
            return HashMap::default();
        }
        RocksTtlProperties::encode(&self.prop).0
    }
}

pub struct TtlPropertiesCollectorFactory {
    pub api_version: ApiVersion,
}

impl TablePropertiesCollectorFactory<TtlPropertiesCollector> for TtlPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> TtlPropertiesCollector {
        TtlPropertiesCollector {
            api_version: self.api_version,
            prop: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikv_util::time::UnixSecs;

    #[test]
    fn test_ttl_properties() {
        test_ttl_properties_impl(ApiVersion::V1ttl);
        test_ttl_properties_impl(ApiVersion::V2);
    }

    fn test_ttl_properties_impl(api_version: ApiVersion) {
        let get_properties = |case: &[(&'static str, u64)]| -> Result<TtlProperties> {
            let mut collector = TtlPropertiesCollector {
                api_version,
                prop: Default::default(),
            };
            for &(k, ts) in case {
                let v = RawValue {
                    user_value: &[0; 10][..],
                    expire_ts: Some(ts),
                };
                collector.add(
                    k.as_bytes(),
                    &v.to_bytes(api_version),
                    DBEntryType::Put,
                    0,
                    0,
                );
            }
            for &(k, _) in case {
                let v = vec![0; 10];
                collector.add(k.as_bytes(), &v, DBEntryType::Other, 0, 0);
            }
            let result = UserProperties(collector.finish());
            RocksTtlProperties::decode(&result)
        };

        let case1 = [
            ("zr\0a", 0),
            ("zr\0b", UnixSecs::now().into_inner()),
            ("zr\0c", 1),
            ("zr\0d", u64::MAX),
            ("zr\0e", 0),
        ];
        let props = get_properties(&case1).unwrap();
        assert_eq!(props.max_expire_ts, u64::MAX);
        match api_version {
            ApiVersion::V1 => unreachable!(),
            ApiVersion::V1ttl => assert_eq!(props.min_expire_ts, 1),
            // expire_ts = 0 is no longer a special case in API V2
            ApiVersion::V2 => assert_eq!(props.min_expire_ts, 0),
        }

        let case2 = [("zr\0a", 0)];
        assert!(get_properties(&case2).is_err());

        let case3 = [];
        assert!(get_properties(&case3).is_err());

        let case4 = [("zr\0a", 1)];
        let props = get_properties(&case4).unwrap();
        assert_eq!(props.max_expire_ts, 1);
        assert_eq!(props.min_expire_ts, 1);
    }
}
