// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::tag::ResourceMeteringTag;

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct CpuRecords {
    pub begin_unix_time_secs: u64,
    pub duration: Duration,

    // tag -> ms
    pub records: HashMap<ResourceMeteringTag, u64>,
}

impl Default for CpuRecords {
    fn default() -> Self {
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Clock may have gone backwards");
        Self {
            begin_unix_time_secs: now_unix_time.as_secs(),
            duration: Duration::default(),
            records: HashMap::default(),
        }
    }
}
