// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;

mod config;
mod errors;

pub use self::config::Config;
pub use self::errors::{Error, Result};
