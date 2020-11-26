// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub struct IOContext;

impl IOContext {
    pub fn new() -> Self {
        IOContext {}
    }

    fn delta(&self) -> u64 {
        0
    }
}

pub struct IOSnooper;

impl IOSnooper {
    pub fn new() -> Self {
        IOSnooper {}
    }

    pub fn start(&mut self) -> Result<(), String> {
       info!("IO snooper is not started due to not compiling with BCC");
       Ok(())
    }

    pub fn stop(&mut self) {}
}