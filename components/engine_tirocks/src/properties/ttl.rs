// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ffi::CStr, marker::PhantomData};

use api_version::{KeyMode, KvFormat, RawValue};
use engine_traits::{Result, TtlProperties, TtlPropertiesExt};
use tikv_util::error;
use tirocks::properties::table::user::{
    Context, EntryType, SequenceNumber, TablePropertiesCollector, TablePropertiesCollectorFactory,
    UserCollectedProperties,
};

use super::{DecodeProperties, EncodeProperties};
use crate::RocksEngine;

const PROP_MAX_EXPIRE_TS: &str = "tikv.max_expire_ts";
const PROP_MIN_EXPIRE_TS: &str = "tikv.min_expire_ts";

fn encode_ttl(ttl_props: &TtlProperties, props: &mut impl EncodeProperties) {
    props.encode_u64(PROP_MAX_EXPIRE_TS, ttl_props.max_expire_ts);
    props.encode_u64(PROP_MIN_EXPIRE_TS, ttl_props.min_expire_ts);
}

pub(super) fn decode_ttl(props: &impl DecodeProperties) -> codec::Result<TtlProperties> {
    let res = TtlProperties {
        max_expire_ts: props.decode_u64(PROP_MAX_EXPIRE_TS)?,
        min_expire_ts: props.decode_u64(PROP_MIN_EXPIRE_TS)?,
    };
    Ok(res)
}

impl TtlPropertiesExt for RocksEngine {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>> {
        let collection = self.properties_of_tables_in_range(cf, &[(start_key, end_key)])?;
        if collection.is_empty() {
            return Ok(vec![]);
        }

        let mut res = Vec::new();
        for (file_name, v) in &*collection {
            let prop = match decode_ttl(v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            res.push((std::str::from_utf8(file_name).unwrap().to_string(), prop));
        }
        Ok(res)
    }
}

/// Can only be used for default CF.
pub struct TtlPropertiesCollector<F: KvFormat> {
    prop: TtlProperties,
    _phantom: PhantomData<F>,
}

impl<F: KvFormat> TtlPropertiesCollector<F> {
    fn finish(&mut self, properties: &mut impl EncodeProperties) {
        if self.prop.max_expire_ts == 0 && self.prop.min_expire_ts == 0 {
            return;
        }
        encode_ttl(&self.prop, properties);
    }
}

impl<F: KvFormat> TablePropertiesCollector for TtlPropertiesCollector<F> {
    fn name(&self) -> &CStr {
        ttl_properties_collector_name()
    }

    fn add(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        _: SequenceNumber,
        _: u64,
    ) -> tirocks::Result<()> {
        if entry_type != EntryType::kEntryPut {
            return Ok(());
        }
        // Only consider data keys.
        if !key.starts_with(keys::DATA_PREFIX_KEY) {
            return Ok(());
        }
        // Only consider raw keys.
        if F::parse_key_mode(&key[keys::DATA_PREFIX_KEY.len()..]) != KeyMode::Raw {
            return Ok(());
        }

        match F::decode_raw_value(value) {
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
        Ok(())
    }

    fn finish(&mut self, properties: &mut UserCollectedProperties) -> tirocks::Result<()> {
        self.finish(properties);
        Ok(())
    }
}

fn ttl_properties_collector_name() -> &'static CStr {
    CStr::from_bytes_with_nul(b"tikv.ttl-properties-collector\0").unwrap()
}

#[derive(Default)]
pub struct TtlPropertiesCollectorFactory<F: KvFormat> {
    _phantom: PhantomData<F>,
}

impl<F: KvFormat> TablePropertiesCollectorFactory for TtlPropertiesCollectorFactory<F> {
    type Collector = TtlPropertiesCollector<F>;

    fn name(&self) -> &CStr {
        ttl_properties_collector_name()
    }

    fn create_table_properties_collector(&self, _: Context) -> TtlPropertiesCollector<F> {
        TtlPropertiesCollector {
            prop: Default::default(),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use api_version::test_kv_format_impl;
    use collections::HashMap;
    use kvproto::kvrpcpb::ApiVersion;
    use tikv_util::time::UnixSecs;

    use super::*;

    #[test]
    fn test_ttl_properties() {
        test_kv_format_impl!(test_ttl_properties_impl<ApiV1Ttl ApiV2>);
    }

    fn test_ttl_properties_impl<F: KvFormat>() {
        let get_properties = |case: &[(&'static str, u64)]| -> codec::Result<TtlProperties> {
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
                collector
                    .add(
                        k.as_bytes(),
                        &F::encode_raw_value(v),
                        EntryType::kEntryPut,
                        0,
                        0,
                    )
                    .unwrap();
            }
            for &(k, _) in case {
                let v = vec![0; 10];
                collector
                    .add(k.as_bytes(), &v, EntryType::kEntryOther, 0, 0)
                    .unwrap();
            }
            let mut result = HashMap::default();
            collector.finish(&mut result);
            decode_ttl(&result)
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
        match F::TAG {
            ApiVersion::V1 => unreachable!(),
            ApiVersion::V1ttl => assert_eq!(props.min_expire_ts, 1),
            // expire_ts = 0 is no longer a special case in API V2
            ApiVersion::V2 => assert_eq!(props.min_expire_ts, 0),
        }

        let case2 = [("zr\0a", 0)];
        get_properties(&case2).unwrap_err();

        let case3 = [];
        get_properties(&case3).unwrap_err();

        let case4 = [("zr\0a", 1)];
        let props = get_properties(&case4).unwrap();
        assert_eq!(props.max_expire_ts, 1);
        assert_eq!(props.min_expire_ts, 1);
    }
}
