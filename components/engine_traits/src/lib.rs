// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A generic TiKV storage engine
//!
//! This is a work-in-progress attempt to abstract all the features
//! needed by TiKV to store to disk.

#![recursion_limit = "200"]

#[macro_use]
extern crate quick_error;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

// These modules contain traits that need to be implemented by engines, either
// they are required by KvEngine or are an associated type of KvEngine. It is
// recommended that engines follow the same module layout.

mod cf_handle;
pub use crate::cf_handle::*;
mod cf_options;
pub use crate::cf_options::*;
mod db_options;
pub use crate::db_options::*;
mod engine;
pub use crate::engine::*;
mod import;
pub use import::*;
mod snapshot;
pub use crate::snapshot::*;
mod write_batch;
pub use crate::write_batch::*;

// These modules contain more general traits, some of which may be implemented
// by multiple types.

mod iterable;
pub use crate::iterable::*;
mod mutable;
pub use crate::mutable::*;
mod peekable;
pub use crate::peekable::*;

// These modules contain support code that does not need to be implemented by
// engines.

mod cf;
pub use crate::cf::*;
mod engines;
pub use engines::*;
mod errors;
pub use crate::errors::*;
mod options;
pub use crate::options::*;
pub mod util;

pub const DATA_KEY_PREFIX_LEN: usize = 1;

// In our tests, we found that if the batch size is too large, running delete_all_in_range will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

