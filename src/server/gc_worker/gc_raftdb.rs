// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::raw::{new_compaction_filter_raw, CompactionFilter, CompactionFilterFactory};
use engine_rocks::RAFT_LOG_GC_CONTEXT;
use std::{collections::HashMap, ffi::CString};

pub struct RaftLogCompactionFilterFactory {}

impl RaftLogCompactionFilterFactory {}

impl CompactionFilterFactory for RaftLogCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _: &engine_rocks::raw::CompactionFilterContext,
    ) -> *mut engine_rocks::raw::DBCompactionFilter {
        let ctx = RAFT_LOG_GC_CONTEXT.read().unwrap();
        let filter = Box::new(RaftLogCompactionFilter::new(ctx.apply_idxs.clone()));
        let name = CString::new("").unwrap();
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct RaftLogCompactionFilter {
    map: HashMap<u64, u64>,
}

impl RaftLogCompactionFilter {
    fn new(map: HashMap<u64, u64>) -> Self {
        RaftLogCompactionFilter { map }
    }
}

impl CompactionFilter for RaftLogCompactionFilter {
    fn filter(
        &mut self,
        _level: usize,
        _key: &[u8],
        _value: &[u8],
        _new_value: &mut Vec<u8>,
        _value_changed: &mut bool,
    ) -> bool {
        let (rid, idx) = keys::decode_raft_log_key(_key).unwrap();
        match self.map.get(&rid) {
            Some(compact_idx) => idx < *compact_idx,
            None => false,
        }
    }
}

// TODO(MrCroxx): add tests.