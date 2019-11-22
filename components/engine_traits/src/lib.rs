// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A generic TiKV storage engine
//!
//! This is a work-in-progress attempt to abstract all the features
//! needed by TiKV to persist its data.
//!
//! This crate must not have any transitive dependencies on RocksDB.
//!
//! # Notes
//!
//! - `KvEngine` is the main engine trait. It requires many other traits, which
//!   have many other associated types that implement yet more traits.
//!
//! - Features should be grouped into their own modules with their own
//!   traits. A common pattern is to have an associated type that implements
//!   a trait, and an "extension" trait that associates that type with `KvEngine`,
//!   which is part of `KvEngine's trait requirements.
//!
//! - For now, for simplicity, all extension traits are required
//!
//! - Associated types generally have the same name as the trait they
//!   are required to implement.
//!
//! - All engines use the same error type, defined in this crate. Thus
//!   engine-specefic type information is boxed and hidden.
//!
//! - `KvEngine` is a factory type for some of its associated types, but not
//!   others. For now, use factory methods when RocksDB would require factory
//!   method (that is, when the DB is required to create the associated type -
//!   if the associated type can be created without context from the database,
//!   use a standard new method). If future engines require factory methods, the
//!   traits can be converted then.
//!
//! # Porting suggestions
//!
//! - Port modules with the fewest RocksDB dependencies at a time, modifying
//!   those modules's callers to convert to and from the engine traits as
//!   needed. Move in and out of the engine_traits world with the
//!   `RocksDB::from_ref` and `RocksDB::as_inner` methods.
//!
//! - For now, use the same APIs as the RocksDB bindings, as methods
//!   on the various engine traits, and with this crate's error type.
//!
//! - When new types are needed from the RocksDB API, add a new module, define a
//!   new trait (possibly with the same name as the RocksDB type), then define a
//!   `TraitExt` trait to "mixin" to the `KvEngine` trait.
//!
//! - Port methods directly from the existing `engine` crate by re-implementing
//!   it in engine_traits and engine_rocks, replacing all the callers with calls
//!   into the traits, then delete the versions in the `engine` crate.

#![recursion_limit = "200"]

#[macro_use]
extern crate quick_error;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

// These modules contain traits that need to be implemented by engines, either
// they are required by KvEngine or are an associated type of KvEngine. It is
// recommended that engines follow the same module layout.
//
// Many of these define "extension" traits, that end in `Ext`.

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
mod io_limiter;
pub use io_limiter::*;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
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

mod cfdefs;
pub use crate::cfdefs::*;
mod engines;
pub use engines::*;
mod errors;
pub use crate::errors::*;
mod options;
pub use crate::options::*;
pub mod util;

pub const DATA_KEY_PREFIX_LEN: usize = 1;
