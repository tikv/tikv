// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(cell_update)]
#![feature(shrink_to)]
#![feature(div_duration)]
#![feature(min_specialization)]
#![feature(box_patterns)]
#![feature(vecdeque_binary_search)]

#[cfg(test)]
extern crate test;

pub mod coprocessor;
pub mod errors;
pub mod router;
pub mod store;
pub use self::coprocessor::{RegionInfo, RegionInfoAccessor, SeekRegionCallback};
pub use self::errors::{DiscardReason, Error, Result};

fn bytes_capacity(b: &bytes::Bytes) -> usize {
    let range = b.as_ptr_range();
    range.end as usize - range.start as usize
}
