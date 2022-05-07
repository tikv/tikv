// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! # TiKV's Coprocessor Framework
//!
//! A coprocessor framework that allows custom, pluggable coprocessor plugins to execute arbitrary
//! user requests directly on TiKV nodes.
//!
//! *Note: While there currently also exists a different [coprocessor][super::coprocessor] that is
//! designed to execute a defined set of functions on TiKV nodes, this coprocessor framework allows
//! to register "coprocessor plugins" that can execute arbitrary code directly on TiKV nodes.
//! The long-term goal is to fully replace the existing coprocessor with an equivalent plugin for
//! this coprocessor.*
//!
//! ## Background
//!
//! The design of the coprocessor framework follows closely the principles of
//! [HBase's coprocessor][hbase-copr] which in turn is built on the ideas of the coprocessor
//! framework in Google's BigTable.
//!
//! By registering new coprocessor plugins, users are able to extend the functionality of TiKV and
//! run code directly on storage nodes. This usually leads to dramatically increased performance
//! because the CPU of TiKV nodes can be utilized for computation and the amount of data transfer
//! can be reduced.
//!
//!
//! [hbase-copr]: https://blogs.apache.org/hbase/entry/coprocessor_introduction

mod config;
mod endpoint;
mod plugin_registry;
mod raw_storage_impl;

pub use config::Config;

pub use self::endpoint::Endpoint;
