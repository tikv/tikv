// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate tikv_util;

#[cfg(all(feature = "bcc", target_os = "linux"))]
#[path = "biosnoop.rs"]
mod imp;

#[cfg(not(all(feature = "bcc", target_os = "linux")))]
#[path = "null.rs"]
mod imp;

pub use imp::{init_io_snooper, IOContext};
