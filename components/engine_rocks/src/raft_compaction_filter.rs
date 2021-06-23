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

// TODO(MrCroxx): add tests.
