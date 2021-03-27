// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! TiKV - A distributed key/value database
//!
//! TiKV ("Ti" stands for Titanium) is an open source distributed
//! transactional key-value database. Unlike other traditional NoSQL
//! systems, TiKV not only provides classical key-value APIs, but also
//! transactional APIs with ACID compliance. TiKV was originally
//! created to complement [TiDB], a distributed HTAP database
//! compatible with the MySQL protocol.
//!
//! [TiDB]: https://github.com/pingcap/tidb
//!
//! The design of TiKV is inspired by some great distributed systems
//! from Google, such as BigTable, Spanner, and Percolator, and some
//! of the latest achievements in academia in recent years, such as
//! the Raft consensus algorithm.

#![crate_type = "lib"]
#![cfg_attr(test, feature(test))]
#![recursion_limit = "400"]
#![feature(cell_update)]
#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(const_fn)]
#![feature(box_patterns)]
#![feature(shrink_to)]
#![feature(drain_filter)]
#![feature(negative_impls)]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate more_asserts;
#[macro_use]
extern crate vlog;
#[macro_use]
extern crate tikv_util;

#[cfg(test)]
extern crate test;

pub mod config;
pub mod coprocessor;
pub mod coprocessor_v2;
pub mod import;
pub mod read_pool;
pub mod server;
pub mod storage;
