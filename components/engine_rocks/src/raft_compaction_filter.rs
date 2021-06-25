// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::raft_engine::{RAFT_LOG_GC_INDEXES, RAFT_LOG_GC_ON_COMPACTION};
use crate::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
    CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter,
};
use std::ffi::CString;
use tikv_util::debug;

pub struct RaftLogCompactionFilterFactory {}

impl RaftLogCompactionFilterFactory {}

impl CompactionFilterFactory for RaftLogCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let filter = Box::new(RaftLogCompactionFilter::new(
            RAFT_LOG_GC_ON_COMPACTION.load(std::sync::atomic::Ordering::Acquire),
        ));
        let name = CString::new("raft_log_gc_compaction_filter").unwrap();
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct RaftLogCompactionFilter {
    enabled: bool,
    last_region: u64,
    gc_point: u64,
}

impl RaftLogCompactionFilter {
    fn new(enabled: bool) -> Self {
        RaftLogCompactionFilter {
            enabled,
            // for region id will NEVER be 0
            last_region: 0,
            gc_point: 0,
        }
    }
}

impl CompactionFilter for RaftLogCompactionFilter {
    fn featured_filter(
        &mut self,
        _level: usize,
        key: &[u8],
        _sequence: u64,
        _value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if !self.enabled {
            return CompactionFilterDecision::Keep;
        }
        if value_type != CompactionFilterValueType::Value {
            return CompactionFilterDecision::Keep;
        }
        let (current_region, current_index) = match keys::decode_raft_log_key(key) {
            Ok((rid, idx)) => (rid, idx),
            Err(_) => {
                debug!(
                    "keep non raft log key when raft log gc compaction";
                    "key"=>format!("{:?}",key),
                );
                return CompactionFilterDecision::Keep;
            }
        };

        if current_region != self.last_region {
            self.last_region = current_region;
            let indexes = RAFT_LOG_GC_INDEXES.read().unwrap();
            self.gc_point = indexes.get(&current_region).unwrap_or(&0).to_owned();
        }

        if current_index < self.gc_point {
            return CompactionFilterDecision::Remove;
        }
        CompactionFilterDecision::Keep
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use crate::RaftLogCompactionFilterFactory;

    use crate::raft_engine::{RAFT_LOG_GC_INDEXES, RAFT_LOG_GC_ON_COMPACTION};
    use crate::raw::{ColumnFamilyOptions, CompactionFilterFactory, DBOptions, Writable, DB};
    use std::ffi::CString;

    #[test]
    fn test_raft_log_gc_compaction_filter() {
        // init db & cf
        let mut cf_opts = ColumnFamilyOptions::default();
        let name = CString::new("test_raft_log_gc_compaction_filter_factory").unwrap();
        let factory =
            Box::new(RaftLogCompactionFilterFactory {}) as Box<dyn CompactionFilterFactory>;
        cf_opts
            .set_compaction_filter_factory(name, factory)
            .unwrap();
        cf_opts.set_disable_auto_compactions(true);
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        let path = tempfile::Builder::new()
            .prefix("test_factory_context_keys")
            .tempdir()
            .unwrap();
        let mut db = DB::open(opts, path.path().to_str().unwrap()).unwrap();
        db.create_cf(("test", cf_opts)).unwrap();
        let cfh = db.cf_handle("test").unwrap();

        // insert test data to db.cf
        const LOG_NUM: u64 = 100;
        let map: HashMap<u64, u64> = [(1, LOG_NUM), (3, LOG_NUM), (5, LOG_NUM)]
            .iter()
            .cloned()
            .collect();
        for (rid, num) in map.iter() {
            for idx in 0..*num {
                db.put_cf(
                    cfh,
                    &keys::raft_log_key(*rid, idx),
                    "VALUE-FOR-TEST".as_bytes(),
                )
                .unwrap();
            }
            db.put_cf(
                cfh,
                &keys::raft_state_key(*rid),
                "VALUE-FOR-TEST".as_bytes(),
            )
            .unwrap();
        }

        // update test gc point
        RAFT_LOG_GC_ON_COMPACTION.store(true, std::sync::atomic::Ordering::Release);
        let mut indexes = RAFT_LOG_GC_INDEXES.write().unwrap();
        indexes.insert(1, LOG_NUM / 2);
        indexes.insert(2, LOG_NUM / 2);
        indexes.insert(5, LOG_NUM * 2);
        drop(indexes);
        db.flush(true).unwrap();

        // trigger compact
        db.compact_range_cf(cfh, None, None);

        // check gc
        for (rid, num) in map.iter() {
            let indexes = RAFT_LOG_GC_INDEXES.read().unwrap();
            let gc_point = *indexes.get(&rid).unwrap_or(&0);
            drop(indexes);
            for idx in 0..*num {
                let lk = keys::raft_log_key(*rid, idx);
                let r = db.get_cf(cfh, &lk).unwrap();
                if idx < gc_point {
                    assert!(r.is_none());
                } else {
                    assert!(r.is_some());
                }
            }
            let sk = keys::raft_state_key(*rid);
            assert!(db.get_cf(cfh, &sk).unwrap().is_some());
        }
    }
}
