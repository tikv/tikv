// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(cell_update)]
#![feature(shrink_to)]
#![feature(div_duration)]
#![feature(min_specialization)]
#![feature(box_patterns)]
#![feature(vecdeque_binary_search)]
#![recursion_limit = "256"]

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
    // NOTE: For deserialized raft messages, `len` equals capacity.
    // This is used to report memory usage to metrics. It's possible that the reported value is
    // higher than exact, because some bytes can be shared in entry cache and apply threads. We
    // should handle this case later.
    b.len()
}

// Currently `bytes::Bytes` are not available for prost-codec.
#[cfg(feature = "prost-codec")]
fn bytes_capacity(b: &Vec<u8>) -> usize {
    b.capacity()
}
