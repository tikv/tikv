// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Reexports from the rocksdb crate
//!
//! This is a temporary artifact of refactoring. It exists to provide downstream
//! crates access to the rocksdb API without depending directly on the rocksdb
//! crate, but only until the engine interface is completely abstracted.

pub use rocksdb::{
    SstPartitioner, SstPartitionerContext, SstPartitionerFactory, SstPartitionerState,
};
