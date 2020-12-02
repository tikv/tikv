// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[allow(unused_imports)]
#[macro_use]
extern crate tikv_util;

#[cfg(all(feature = "bcc", target_os = "linux"))]
#[path = "biosnoop.rs"]
mod imp;

#[cfg(not(all(feature = "bcc", target_os = "linux")))]
#[path = "null.rs"]
mod imp;

pub use imp::{init_io_snooper, IOContext};

#[repr(C)]
#[derive(Default)]
pub struct IOStats {
    read: u64,
    write: u64,
}
