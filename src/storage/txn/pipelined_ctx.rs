// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BinaryHeap;
use tikv_util::collections::HashMap;

pub struct PipelinedCtx {
    for_update_ts_cache: HashMap<u64, TimeStamp>,
    expire_queue: BinaryHeap<(TimeStamp, u64)>,
}

impl PipelinedCtx {
    pub fn new() -> PipeLinedCtx {
        PipeLinedCtx {
            HashMap<u64, TimeStamp>::new(),
            BinaryHeap<(TimeStamp, u64)>::new(),
        }
    }

    pub fn put(&self, primary: &[u8]) {
    }

    pub fn get(&self, primary: &[u8]) {
    }

    pub fn may_clean(&self) {
    }
}
