// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(cell_update)]
#![feature(div_duration)]
#![feature(min_specialization)]
#![feature(box_patterns)]
#![feature(hash_drain_filter)]
#![feature(let_chains)]
#![recursion_limit = "256"]

#[cfg(test)]
extern crate test;
#[macro_use]
extern crate derivative;

pub mod coprocessor;
pub mod errors;
pub mod router;
pub mod store;
pub use self::{
    coprocessor::{RegionInfo, RegionInfoAccessor, SeekRegionCallback},
    errors::{DiscardReason, Error, Result},
};

// `bytes::Bytes` is generated for `bytes` in protobuf.
fn bytes_capacity(b: &bytes::Bytes) -> usize {
    // NOTE: For deserialized raft messages, `len` equals capacity.
    // This is used to report memory usage to metrics.
    b.len()
}
