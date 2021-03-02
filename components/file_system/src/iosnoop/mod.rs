// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(all(feature = "bcc", target_os = "linux"))]
#[path = "biosnoop.rs"]
mod imp;

#[cfg(not(all(feature = "bcc", target_os = "linux")))]
#[path = "null.rs"]
mod imp;

pub(crate) use imp::{fetch_io_bytes, flush_io_latency_metrics};
pub use imp::{get_io_type, init_io_snooper, set_io_type};
