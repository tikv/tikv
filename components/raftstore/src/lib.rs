// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#![feature(cell_update)]
#![feature(div_duration)]
#![feature(min_specialization)]
#![feature(box_patterns)]
#![feature(hash_extract_if)]
#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
#![recursion_limit = "256"]

#[cfg(test)]
extern crate test;
#[macro_use]
extern crate derivative;
#[cfg(feature = "engine_rocks")]
pub mod compacted_event_sender;

pub mod coprocessor;
pub mod errors;
pub mod router;
pub mod store;
#[cfg(feature = "engine_rocks")]
pub use self::compacted_event_sender::RaftRouterCompactedEventSender;
pub use self::{
    coprocessor::{RegionInfo, RegionInfoAccessor, SeekRegionCallback},
    errors::{DiscardReason, Error, Result},
};

// `bytes::Bytes` is generated for `bytes` in protobuf.
pub fn bytes_capacity(b: &bytes::Bytes) -> usize {
    // NOTE: For deserialized raft messages, `len` equals capacity.
    // This is used to report memory usage to metrics.
    b.len()
}
