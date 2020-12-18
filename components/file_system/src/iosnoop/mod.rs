// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(all(feature = "bcc", target_os = "linux"))]
#[path = "biosnoop.rs"]
mod imp;

#[cfg(not(all(feature = "bcc", target_os = "linux")))]
#[path = "null.rs"]
mod imp;

mod metrics;

pub use imp::{flush_io_metrics, init_io_snooper, IOContext};
pub use imp::{get_io_type, set_io_type};

#[repr(C)]
#[derive(Default, Clone)]
pub struct IOStats {
    read: u64,
    write: u64,
}
