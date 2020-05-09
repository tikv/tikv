// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(cell_update)]
#![feature(shrink_to)]
#![feature(div_duration)]

#[macro_use]
extern crate bitflags;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate tikv_util;

#[cfg(test)]
extern crate test;

pub mod coprocessor;
pub mod errors;
pub mod router;
pub mod store;
pub use self::coprocessor::{RegionInfo, RegionInfoAccessor, SeekRegionCallback};
pub use self::errors::{DiscardReason, Error, Result};
