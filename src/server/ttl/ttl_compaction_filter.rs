// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;

use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterFactory,
    DBCompactionFilter,
};
use engine_rocks::{RocksTTLProperties, RocksUserCollectedPropertiesNoRc};
use engine_traits::util::get_expire_ts;
#[cfg(not(test))]
use tikv_util::time::UnixSecs;

#[cfg(test)]
pub(crate) const TEST_CURRENT_TS: u64 = 100;

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

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::util::append_expire_ts;

    use crate::config::DbConfig;
    use crate::storage::kv::TestEngineBuilder;
    use engine_rocks::raw::CompactOptions;
    use engine_rocks::util::get_cf_handle;
    use engine_traits::{MiscExt, Peekable, SyncMutable, CF_DEFAULT};

    #[test]
    fn test_ttl_compaction_filter() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        let dir = tempfile::TempDir::new().unwrap();
        let builder = TestEngineBuilder::new().path(dir.path()).ttl(true);
        let engine = builder.build_with_cfg(&cfg).unwrap();
        let kvdb = engine.get_rocksdb();

        let key1 = b"key1";
        let mut value1 = vec![0; 10];
        append_expire_ts(&mut value1, 10);
        kvdb.put_cf(CF_DEFAULT, key1, &value1).unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        let db = kvdb.as_inner();
        let handle = get_cf_handle(db, CF_DEFAULT).unwrap();
        db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);

        assert!(kvdb.get_value_cf(CF_DEFAULT, key1).unwrap().is_none());

        let key2 = b"key2";
        let mut value2 = vec![0; 10];
        append_expire_ts(&mut value2, TEST_CURRENT_TS + 20);
        kvdb.put_cf(CF_DEFAULT, key2, &value2).unwrap();
        let key3 = b"key3";
        let mut value3 = vec![0; 10];
        append_expire_ts(&mut value3, 20);
        kvdb.put_cf(CF_DEFAULT, key3, &value3).unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        let key4 = b"key4";
        let mut value4 = vec![0; 10];
        append_expire_ts(&mut value4, 0);
        kvdb.put_cf(CF_DEFAULT, key4, &value4).unwrap();
        kvdb.flush_cf(CF_DEFAULT, true).unwrap();

        db.compact_range_cf_opt(handle, &CompactOptions::new(), None, None);
        assert!(kvdb.get_value_cf(CF_DEFAULT, key2).unwrap().is_some());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key3).unwrap().is_none());
        assert!(kvdb.get_value_cf(CF_DEFAULT, key4).unwrap().is_some());
    }
}
