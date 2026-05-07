// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, marker::PhantomData};

use api_version::{KeyMode, KvFormat, RawValue};
use engine_traits::{Range, Result, TtlProperties, TtlPropertiesExt};
use rocksdb::{DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory};
use tikv_util::error;

use crate::{decode_properties::DecodeProperties, RocksEngine, UserProperties};

const PROP_MAX_EXPIRE_TS: &str = "tikv.max_expire_ts";
const PROP_MIN_EXPIRE_TS: &str = "tikv.min_expire_ts";

pub struct RocksTtlProperties;

impl RocksTtlProperties {
    pub fn encode_to(ttl_props: &TtlProperties, user_props: &mut UserProperties) {
        if let Some(max_expire_ts) = ttl_props.max_expire_ts {
            user_props.encode_u64(PROP_MAX_EXPIRE_TS, max_expire_ts);
        }
        if let Some(min_expire_ts) = ttl_props.min_expire_ts {
            user_props.encode_u64(PROP_MIN_EXPIRE_TS, min_expire_ts);
        }
    }

    pub fn encode(ttl_props: &TtlProperties) -> UserProperties {
        let mut props = UserProperties::new();
        Self::encode_to(ttl_props, &mut props);
        props
    }

    pub fn decode_from<T: DecodeProperties>(ttl_props: &mut TtlProperties, props: &T) {
        ttl_props.max_expire_ts = props.decode_u64(PROP_MAX_EXPIRE_TS).ok();
        ttl_props.min_expire_ts = props.decode_u64(PROP_MIN_EXPIRE_TS).ok();
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> TtlProperties {
        let mut res = TtlProperties::default();
        Self::decode_from(&mut res, props);
        res
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
            let prop = RocksTtlProperties::decode(v.user_collected_properties());
            if prop.is_some() {
                res.push((file_name.to_string(), prop));
            }
        }
        Ok(res)
    }
}

/// Can only be used for default CF.
pub struct TtlPropertiesCollector<F: KvFormat> {
    prop: TtlProperties,
    _phantom: PhantomData<F>,
}

impl<F: KvFormat> TablePropertiesCollector for TtlPropertiesCollector<F> {
    fn add(&mut self, key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        // DBEntryType::BlobIndex will be skipped because we can't parse the value.
        if entry_type != DBEntryType::Put {
            return;
        }
        // Only consider data keys.
        if !key.starts_with(keys::DATA_PREFIX_KEY) {
            return;
        }
        // Only consider raw keys.
        if F::parse_key_mode(&key[keys::DATA_PREFIX_KEY.len()..]) != KeyMode::Raw {
            return;
        }

        match F::decode_raw_value(value) {
            Ok(RawValue {
                expire_ts: Some(expire_ts),
                ..
            }) => {
                self.prop.add(expire_ts);
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
        RocksTtlProperties::encode(&self.prop).0
    }
}

#[derive(Default)]
pub struct TtlPropertiesCollectorFactory<F: KvFormat> {
    _phantom: PhantomData<F>,
}

impl<F: KvFormat> TablePropertiesCollectorFactory<TtlPropertiesCollector<F>>
    for TtlPropertiesCollectorFactory<F>
{
    fn create_table_properties_collector(&mut self, _: u32) -> TtlPropertiesCollector<F> {
        TtlPropertiesCollector {
            prop: Default::default(),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use api_version::test_kv_format_impl;
    use kvproto::kvrpcpb::ApiVersion;
    use tikv_util::time::UnixSecs;

    use super::*;

    #[test]
    fn test_ttl_properties() {
        test_kv_format_impl!(test_ttl_properties_impl<ApiV1Ttl ApiV2>);
    }

    fn test_ttl_properties_impl<F: KvFormat>() {
        let get_properties = |case: &[(&'static str, u64)]| -> TtlProperties {
            let mut collector = TtlPropertiesCollector::<F> {
                prop: Default::default(),
                _phantom: PhantomData,
            };
            for &(k, ts) in case {
                let v = RawValue {
                    user_value: &[0; 10][..],
                    expire_ts: Some(ts),
                    is_delete: false,
                };
                collector.add(
                    k.as_bytes(),
                    &F::encode_raw_value(v),
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

        // NOTE: expire_ts=0 is considered as no TTL in `ApiVersion::V1ttl`
        let case1 = [
            ("zr\0a", 0),
            ("zr\0b", UnixSecs::now().into_inner()),
            ("zr\0c", 1),
            ("zr\0d", u64::MAX),
            ("zr\0e", 0),
        ];
        let props = get_properties(&case1);
        assert_eq!(props.max_expire_ts, Some(u64::MAX));
        match F::TAG {
            ApiVersion::V1 => unreachable!(),
            ApiVersion::V1ttl => assert_eq!(props.min_expire_ts, Some(1)),
            ApiVersion::V2 => assert_eq!(props.min_expire_ts, Some(0)),
        }

        let case2 = [("zr\0a", 0)];
        match F::TAG {
            ApiVersion::V1 => unreachable!(),
            ApiVersion::V1ttl => assert!(get_properties(&case2).is_none()),
            ApiVersion::V2 => assert_eq!(props.min_expire_ts, Some(0)),
        }

        let case3 = [];
        assert!(get_properties(&case3).is_none());

        let case4 = [("zr\0a", 1)];
        let props = get_properties(&case4);
        assert_eq!(props.max_expire_ts, Some(1));
        assert_eq!(props.min_expire_ts, Some(1));
    }

    #[test]
    fn test_ttl_properties_codec() {
        let cases: Vec<(Option<u64>, Option<u64>, Vec<(&[u8], u64)>)> = vec![
            (
                Some(0),                                                      // min_expire_ts
                Some(1),                                                      // max_expire_ts
                vec![(b"tikv.min_expire_ts", 0), (b"tikv.max_expire_ts", 1)], // UserProperties
            ),
            (None, None, vec![]),
            (Some(0), None, vec![(b"tikv.min_expire_ts", 0)]),
            (None, Some(0), vec![(b"tikv.max_expire_ts", 0)]),
        ];

        for (i, (min_expire_ts, max_expire_ts, expect_user_props)) in cases.into_iter().enumerate()
        {
            let ttl_props = TtlProperties {
                min_expire_ts,
                max_expire_ts,
            };
            let user_props = RocksTtlProperties::encode(&ttl_props);
            let expect_user_props = UserProperties(
                expect_user_props
                    .into_iter()
                    .map(|(name, value)| (name.to_vec(), value.to_be_bytes().to_vec()))
                    .collect::<HashMap<_, _>>(),
            );
            assert_eq!(user_props.0, expect_user_props.0, "case {}", i);

            let decoded = RocksTtlProperties::decode(&user_props);
            assert_eq!(decoded.max_expire_ts, ttl_props.max_expire_ts, "case {}", i);
            assert_eq!(decoded.min_expire_ts, ttl_props.min_expire_ts, "case {}", i);
        }
    }
}
