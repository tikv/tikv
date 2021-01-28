use std::collections::HashMap;
use std::ffi::CString;

use crate::{RocksUserCollectedPropertiesNoRc, UserProperties};
use engine_traits::util::get_expire_ts;
use engine_traits::DecodeProperties;
use rocksdb::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterFactory,
    DBCompactionFilter, DBEntryType, TablePropertiesCollector, TablePropertiesCollectorFactory,
};
use tikv_util::codec::Result;
use tikv_util::time::UnixSecs;

const PROP_MAX_EXPIRE_TS: &str = "tikv.max_expire_ts";
const PROP_MIN_EXPIRE_TS: &str = "tikv.min_expire_ts";

pub struct TTLCompactionFilterFactory;

impl CompactionFilterFactory for TTLCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let current = UnixSecs::now().into_inner();

        let mut min_expire_ts = u64::MAX;
        for i in 0..context.file_numbers().len() {
            let table_props = context.table_properties(i);
            let user_props = unsafe {
                &*(table_props.user_collected_properties() as *const _
                    as *const RocksUserCollectedPropertiesNoRc)
            };
            if let Ok(props) = TTLProperties::decode(user_props) {
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

#[derive(Debug, Default)]
pub struct TTLProperties {
    pub max_expire_ts: u64,
    pub min_expire_ts: u64,
}

impl TTLProperties {
    pub fn encode(&self) -> UserProperties {
        let mut props = UserProperties::new();
        props.encode_u64(PROP_MAX_EXPIRE_TS, self.max_expire_ts);
        props.encode_u64(PROP_MIN_EXPIRE_TS, self.min_expire_ts);
        props
    }

    pub fn decode<T: DecodeProperties>(props: &T) -> Result<TTLProperties> {
        let mut res = TTLProperties::default();
        res.max_expire_ts = props.decode_u64(PROP_MAX_EXPIRE_TS)?;
        res.min_expire_ts = props.decode_u64(PROP_MIN_EXPIRE_TS)?;
        Ok(res)
    }
}

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
        self.prop.encode().0
    }
}

pub struct TTLPropertiesCollectorFactory {}

impl TablePropertiesCollectorFactory for TTLPropertiesCollectorFactory {
    fn create_table_properties_collector(&mut self, _: u32) -> Box<dyn TablePropertiesCollector> {
        Box::new(TTLPropertiesCollector {
            prop: TTLProperties::default(),
        })
    }
}
