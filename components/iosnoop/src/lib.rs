// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[allow(unused_imports)]
// #[macro_use]
// extern crate tikv_util;

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

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
