// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::raft_engine::RAFT_LOG_GC_INDEXES;
use crate::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
    CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter,
};
use std::{collections::HashMap, ffi::CString};
use tikv_util::debug;

pub struct RaftLogCompactionFilterFactory {}

impl RaftLogCompactionFilterFactory {}

impl CompactionFilterFactory for RaftLogCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let filter = Box::new(RaftLogCompactionFilter::new());
        let name = CString::new("raft_log_gc_compaction_filter").unwrap();
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct RaftLogCompactionFilter {
    map: HashMap<u64, u64>,
}

impl RaftLogCompactionFilter {
    fn new() -> Self {
        RaftLogCompactionFilter {
            map: HashMap::new(),
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
        if value_type != CompactionFilterValueType::Value {
            return CompactionFilterDecision::Keep;
        }
        let (rid, idx) = match keys::decode_raft_log_key(key) {
            Ok((rid, idx)) => (rid, idx),
            Err(_) => {
                debug!(
                    "keep non raft log key when raft log gc compaction";
                    "key"=>format!("{:?}",key),
                );
                return CompactionFilterDecision::Keep;
            }
        };

        let gc_point = match self.map.get(&rid) {
            Some(v) => v.to_owned(),
            None => {
                let indexes = RAFT_LOG_GC_INDEXES.read().unwrap();
                let gc_point = indexes.get(&rid).unwrap_or(&0).to_owned();
                self.map.insert(rid, gc_point);
                gc_point
            }
        };

        if idx >= gc_point {
            return CompactionFilterDecision::Keep;
        }
        CompactionFilterDecision::Remove
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use crate::RaftLogCompactionFilterFactory;

    use crate::raft_engine::RAFT_LOG_GC_INDEXES;
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
        let map: HashMap<u64, u64> = [(1, 100), (3, 100), (5, 100)].iter().cloned().collect();
        for rid in map.keys() {
            for idx in 0..map.get(rid).unwrap_or(&0).to_owned() {
                db.put_cf(
                    cfh,
                    &keys::raft_log_key(rid.to_owned(), idx.to_owned()),
                    "VALUE-FOR-TEST".as_bytes(),
                )
                .unwrap();
                db.put_cf(
                    cfh,
                    &keys::raft_state_key(rid.to_owned()),
                    "VALUE-FOR-TEST".as_bytes(),
                )
                .unwrap();
            }
        }

        // update test gc point
        let mut indexes = RAFT_LOG_GC_INDEXES.write().unwrap();
        indexes.insert(1, 50);
        indexes.insert(2, 50);
        indexes.insert(5, 200);
        drop(indexes);
        db.flush(true).unwrap();

        // trigger compact
        db.compact_range_cf(cfh, None, None);

        // check gc
        for rid in map.keys() {
            for idx in 0..map.get(rid).unwrap_or(&0).to_owned() {
                let lk = keys::raft_log_key(rid.to_owned(), idx.to_owned());
                let sk = keys::raft_state_key(rid.to_owned());
                if db.get_cf(cfh, &lk).unwrap().is_some() {
                    assert!(idx <= *map.get(&rid).unwrap_or(&0));
                }
                assert!(!db.get_cf(cfh, &sk).unwrap().is_none());
            }
        }
    }
}
