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

// With feature protobuf-codec, `bytes::Bytes` is generated for `bytes` in protobuf.
#[cfg(feature = "protobuf-codec")]
fn bytes_capacity(b: &bytes::Bytes) -> usize {
    // NOTE: it's correct because in raftstore all `bytes::Bytes`s are immutable,
    // which means `clear` or similar others will never be called for them.
    b.len()
}

// Currently `bytes::Bytes` are not available for prost-codec.
#[cfg(feature = "prost-codec")]
fn bytes_capacity(b: &Vec<u8>) -> usize {
    b.capacity()
}
