// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{IOLimiter, IOLimiterExt};

impl IOLimiterExt for PanicEngine {
    type IOLimiter = PanicIOLimiter;
}

pub struct PanicIOLimiter;

impl IOLimiter for PanicIOLimiter {
    fn new(bytes_per_sec: i64) -> Self {
        panic!()
    }
    fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        panic!()
    }
    fn request(&self, bytes: i64) {
        panic!()
    }
    fn get_max_bytes_per_time(&self) -> i64 {
        panic!()
    }
    fn get_total_bytes_through(&self) -> i64 {
        panic!()
    }
    fn get_bytes_per_second(&self) -> i64 {
        panic!()
    }
    fn get_total_requests(&self) -> i64 {
        panic!()
    }
}
