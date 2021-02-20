// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use crate::{RocksEngine, UserProperties};
use engine_traits::util::get_expire_ts;
use engine_traits::{
    DecodeProperties, Range, Result, TTLProperties, TTLPropertiesExt, TableProperties,
    TablePropertiesCollection, TablePropertiesExt,
};
use rocksdb::{DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory};

const PROP_MAX_EXPIRE_TS: &str = "tikv.max_expire_ts";
const PROP_MIN_EXPIRE_TS: &str = "tikv.min_expire_ts";

pub struct RocksTTLProperties;

impl RocksTTLProperties {
    pub fn encode(ttl_props: &TTLProperties) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_MAX_EXPIRE_TS, ttl_props.max_expire_ts);
        props.encode_u64(PROP_MIN_EXPIRE_TS, ttl_props.min_expire_ts);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<TTLProperties> {
        let mut res = TTLProperties::default();
        res.max_expire_ts = props.decode_u64(PROP_MAX_EXPIRE_TS)?;
        res.min_expire_ts = props.decode_u64(PROP_MIN_EXPIRE_TS)?;
        Ok(res)
    }
}

impl TTLPropertiesExt for RocksEngine {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TTLProperties)>> {
        let range = Range::new(start_key, end_key);
        let collection = self.get_properties_of_tables_in_range(cf, &[range])?;
        if collection.is_empty() {
            return Ok(vec![]);
        }

        let mut res = Vec::new();
        for (file_name, v) in collection.iter() {
            let prop = match RocksTTLProperties::decode(&v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            res.push((file_name.to_string(), prop));
        }
        Ok(res)
    }
}

#[derive(Default)]
/// Can only be used for default CF.
pub struct TTLPropertiesCollector {
    prop: TTLProperties,
}

impl TablePropertiesCollector for TTLPropertiesCollector {
    fn add(&mut self, _key: &[u8], value: &[u8], entry_type: DBEntryType, _: u64, _: u64) {
        if entry_type != DBEntryType::Put {
            return;
        }

        let expire_ts = get_expire_ts(&value).unwrap_or(0);
        if expire_ts > self.prop.max_expire_ts {
            self.prop.max_expire_ts = expire_ts;
        }

        if self.prop.min_expire_ts == 0 {
            self.prop.min_expire_ts = expire_ts;
        } else {
            self.prop.min_expire_ts = std::cmp::min(self.prop.min_expire_ts, expire_ts);
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        if self.prop.max_expire_ts == 0 && self.prop.min_expire_ts == 0 {
            return HashMap::default();
        }
        RocksTTLProperties::encode(&self.prop).0
    }
}

pub struct TTLPropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory for TTLPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(TTLPropertiesCollector::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::util::append_expire_ts;
    use tikv_util::time::UnixSecs;

    #[test]
    fn test_ttl_properties() {
        let get_properties = |case: &[(&'static str, u64)]| -> Result<TTLProperties> {
            let mut collector = TTLPropertiesCollector::default();
            for &(k, ts) in case {
                let mut v = vec![0; 10];
                append_expire_ts(&mut v, ts);
                collector.add(k.as_bytes(), &v, DBEntryType::Put, 0, 0);
            }
            for &(k, _) in case {
                let v = vec![0; 10];
                collector.add(k.as_bytes(), &v, DBEntryType::Other, 0, 0);
            }
            let result = UserProperties(collector.finish());
            RocksTTLProperties::decode(&result)
        };

        let case1 = [
            ("a", 0),
            ("b", UnixSecs::now().into_inner()),
            ("c", 1),
            ("d", u64::MAX),
        ];
        let props = get_properties(&case1).unwrap();
        assert_eq!(props.max_expire_ts, u64::MAX);
        assert_eq!(props.min_expire_ts, 1);

        let case2 = [("a", 0)];
        assert!(get_properties(&case2).is_err());

        let case3 = [];
        assert!(get_properties(&case3).is_err());

        let case4 = [("a", 1)];
        let props = get_properties(&case4).unwrap();
        assert_eq!(props.max_expire_ts, 1);
        assert_eq!(props.min_expire_ts, 1);
    }
}
