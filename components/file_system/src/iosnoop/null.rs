// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::IOBytes;
use crate::IOType;
use std::cell::Cell;

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

pub fn flush_io_latency_metrics() {}

pub(crate) fn fetch_io_bytes(_io_type: IOType) -> IOBytes {
    IOBytes::default()
}
