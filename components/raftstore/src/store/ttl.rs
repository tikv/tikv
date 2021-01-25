use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext,
    CompactionFilterFactory, DBCompactionFilter, DBIterator,
};
use engine_rocks::{
    RocksEngine, RocksEngineIterator, RocksMvccProperties, RocksUserCollectedPropertiesNoRc,
    RocksWriteBatch,
};

use tikv_util::time::UnixSecs;
use tikv_util::codec;
use tikv_util::codec::number::{self, NumberEncoder};

use std::ffi::CString;
use std::borrow::Cow;

pub struct ValueWithTTL<'a> {
    value: Cow<'a, [u8]>,
}

// u32::MAX = 2106-02-07 14:28:15
// impl ValueWithTTL<'_> {
//     pub fn new(value_with_ttl: &[u8]) -> Self {
//         ValueWithTTL {
//             value: Cow::from(value_with_ttl),
//         }
//     }

//     pub fn from(value: &[u8], expire_ts: u64) -> Self {
//         ValueWithTTL {
//             value: Cow::from(value.to_vec().encode_u64(expire_ts)),
//         }
//     }
    pub fn append_expire_ts(value: &mut Vec<u8>, expire_ts: u64) {
        value.encode_u64(expire_ts).unwrap();
    }

    pub fn get_expire_ts(value_with_ttl: &[u8]) -> Result<u64, codec::Error> {
        let len = value_with_ttl.len();
        if len < number::U64_SIZE {
            return Err(codec::Error::KeyLength);
        }
        let mut ts = &value_with_ttl[len - number::U64_SIZE..];
        Ok(number::decode_u64(&mut ts)?.into())
    }

    pub fn strip_expire_ts(value_with_ttl: &[u8]) -> &[u8] {
        let len = value_with_ttl.len();
        &value_with_ttl[..len - number::U64_SIZE]
    }
// }

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
            if let Ok(props) = RocksMvccProperties::decode(user_props) {
                if props.get_min_expire_ts() != 0 {
                    min_expire_ts = std::min(min_expire_ts, props.get_min_expire_ts());
                }
            }
        }
        if min_expire_ts > current {
            return std::ptr::null_mut();
        }

        let name = CString::new("ttl_compaction_filter").unwrap();
        let filter = Box::new(TTLCompactionFilter{ts:current});
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