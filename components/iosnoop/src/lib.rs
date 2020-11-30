// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate tikv_util;
#[macro_use]
extern crate lazy_static;

#[cfg(all(feature = "bcc", unix))]
#[path = "biosnoop.rs"]
mod imp;

#[cfg(not(all(feature = "bcc", unix)))]
#[path = "null.rs"]
mod imp;

pub use imp::{IOContext, IOSnooper};
