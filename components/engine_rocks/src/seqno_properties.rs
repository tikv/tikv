// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, collections::HashMap};

use engine_traits::{Range, Result, SeqnoProperties, SeqnoPropertiesExt};
use rocksdb::{DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory};

use crate::{decode_properties::DecodeProperties, RocksEngine, UserProperties};

const PROP_LARGEST_SEQNO: &str = "tikv.largest_seqno";
const PROP_SMALLEST_SEQNO: &str = "tikv.smallest_seqno";

// TODO: implement it in RocksDB.
pub struct RocksSeqnoProperties;

impl RocksSeqnoProperties {
    pub fn encode(seqno_props: &SeqnoProperties) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_LARGEST_SEQNO, seqno_props.largest_seqno);
        props.encode_u64(PROP_SMALLEST_SEQNO, seqno_props.smallest_seqno);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<SeqnoProperties> {
        let res = SeqnoProperties {
            largest_seqno: props.decode_u64(PROP_LARGEST_SEQNO)?,
            smallest_seqno: props.decode_u64(PROP_SMALLEST_SEQNO)?,
        };
        Ok(res)
    }
}

impl SeqnoPropertiesExt for RocksEngine {
    fn get_range_seqno_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<SeqnoProperties>> {
        let range = Range::new(start_key, end_key);
        let collection = self.get_properties_of_tables_in_range(cf, &[range])?;
        if collection.is_empty() {
            return Ok(None);
        }

        let mut res = SeqnoProperties::default();
        for (_, v) in collection.iter() {
            let prop = match RocksSeqnoProperties::decode(v.user_collected_properties()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            res.largest_seqno = cmp::max(res.largest_seqno, prop.largest_seqno);
            res.smallest_seqno = cmp::min(res.smallest_seqno, prop.smallest_seqno);
        }
        Ok(Some(res))
    }
}

/// Can only be used for default CF.
pub struct SeqnoPropertiesCollector {
    prop: SeqnoProperties,
}

impl TablePropertiesCollector for SeqnoPropertiesCollector {
    fn add(&mut self, _: &[u8], _: &[u8], _: DBEntryType, seqno: u64, _: u64) {
        self.prop.largest_seqno = cmp::max(self.prop.largest_seqno, seqno);
        self.prop.smallest_seqno = cmp::min(self.prop.smallest_seqno, seqno);
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        RocksSeqnoProperties::encode(&self.prop).0
    }
}

#[derive(Default)]
pub struct SeqnoPropertiesCollectorFactory;

impl TablePropertiesCollectorFactory<SeqnoPropertiesCollector> for SeqnoPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> SeqnoPropertiesCollector {
        SeqnoPropertiesCollector {
            prop: SeqnoProperties::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seqno_properties() {
        let mut collector = SeqnoPropertiesCollector {
            prop: SeqnoProperties::default(),
        };
        collector.add(b"k", b"v", DBEntryType::Put, 3, 0);
        collector.add(b"k", b"v", DBEntryType::Put, 2, 0);
        collector.add(b"k", b"v", DBEntryType::Put, 1, 0);
        collector.add(b"k", b"v", DBEntryType::Put, 4, 0);
        let result = UserProperties(collector.finish());
        let prop = RocksSeqnoProperties::decode(&result).unwrap();
        assert_eq!(prop.largest_seqno, 4);
        assert_eq!(prop.smallest_seqno, 1);
    }
}
