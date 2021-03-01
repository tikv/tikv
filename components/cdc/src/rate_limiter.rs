// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::mpsc::batch::Sender;
use crate::service::CdcEvent;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct RateLimiter {
    sink: Sender<CdcEvent>,
    is_sink_closed: AtomicBool,
    block_scan_threshold: usize,
}

impl RateLimiter {
    pub fn record_realtime_event(len: usize) {
        
    }
}