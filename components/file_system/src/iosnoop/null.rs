// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{IOStats, IOType};
use std::cell::Cell;

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

thread_local! {
    static IO_TYPE: Cell<IOType> = Cell::new(IOType::Other)
}

pub fn set_io_type(new_io_type: IOType) {
    IO_TYPE.with(|io_type| {
        io_type.set(new_io_type);
    });
}

pub fn get_io_type() -> IOType {
    IO_TYPE.with(|io_type| io_type.get())
}

pub unsafe fn flush_io_metrics() {}
