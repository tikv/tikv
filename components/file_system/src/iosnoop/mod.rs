// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(all(feature = "bcc", target_os = "linux"))]
#[path = "biosnoop.rs"]
mod imp;

#[cfg(not(all(feature = "bcc", target_os = "linux")))]
#[path = "null.rs"]
mod imp;

pub use imp::{get_io_type, set_io_type};
pub use imp::{init_io_snooper, IOContext};

#[repr(C)]
#[derive(Default, Clone)]
pub struct IOStats {
    read: u64,
    write: u64,
}

#[derive(Debug)]
pub enum IOOp {
    Read,
    Write,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IOType {
    Other,
    Read,
    Write,
    Coprocessor,
    Flush,
    Compaction,
    Replication,
    LoadBalance,
    Import,
    Export,
}

pub struct WithIOType {
    previous_io_type: IOType,
}

impl WithIOType {
    pub fn new(new_io_type: IOType) -> WithIOType {
        let previous_io_type = get_io_type();
        set_io_type(new_io_type);
        WithIOType { previous_io_type }
    }
}

impl Drop for WithIOType {
    fn drop(&mut self) {
        set_io_type(self.previous_io_type);
    }
}
