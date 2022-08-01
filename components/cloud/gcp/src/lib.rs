// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate slog_global;

mod gcs;
pub use gcs::{Config, GcsStorage};
