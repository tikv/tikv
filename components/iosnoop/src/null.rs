// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::IOStats;

pub struct IOContext;

impl IOContext {
    pub fn new() -> Self {
        IOContext {}
    }

    #[allow(dead_code)]
    fn delta(&self) -> IOStats {
        IOStats::default()
    }

    #[allow(dead_code)]
    fn delta_and_refresh(&mut self) -> IOStats {
        IOStats::default()
    }
}

pub fn init_io_snooper() -> Result<(), String> {
    Err("IO snooper is not started due to not compiling with BCC".to_string())
}
