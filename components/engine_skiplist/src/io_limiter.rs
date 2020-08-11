// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{IOLimiter, IOLimiterExt};

impl IOLimiterExt for SkiplistEngine {
    type IOLimiter = SkiplistIOLimiter;
}

pub struct SkiplistIOLimiter;

impl IOLimiter for SkiplistIOLimiter {
    fn new(bytes_per_sec: i64) -> Self {
        SkiplistIOLimiter
    }
    fn set_bytes_per_second(&self, bytes_per_sec: i64) {}
    fn request(&self, bytes: i64) {}
    fn get_max_bytes_per_time(&self) -> i64 {
        std::i64::MIN
    }
    fn get_total_bytes_through(&self) -> i64 {
        std::i64::MIN
    }
    fn get_bytes_per_second(&self) -> i64 {
        std::i64::MIN
    }
    fn get_total_requests(&self) -> i64 {
        std::i64::MIN
    }
}
