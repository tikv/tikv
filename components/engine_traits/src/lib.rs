// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A generic TiKV storage engine
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! TiKV to persist its data.
//!
//! This crate must not have any transitive dependencies on RocksDB. The RocksDB
//! implementation is in the `engine_rocks` crate.
//!
//! This documentation contains a description of the porting process, current
//! design decisions and design guidelines, and refactoring tips.
//!
//!
//! # The porting process
//!
//! These are some guidelines that seem to make the porting managable. As the
//! process continues new strategies are discovered and written here. This is a
//! big refactoring and will take many monthse.
//!
//! Refactoring is a craft, not a science, and figuring out how to overcome any
//! particular situation takes experience and intuation, but these principles
//! can help.
//!
//! A guiding principle is to do one thing at a time. In particular, don't
//! redesign while encapsulating.
//!
//! The port is happening in stages:
//!
//!   1) Migrating the `engine` abstractions
//!   2) Eliminating direct-use of `rocksdb` re-exports
//!   3) "Pulling up" the generic abstractions though TiKV
//!   4) Isolating test cases from RocksDB
//!
//! These stages are described in more detail:
//!
//! ## 1) Migrating the `engine` abstractions
//!
//! The engine crate was an earlier attempt to abstract the storage engine. Much
//! of its structure is duplicated near-identically in engine_traits, the
//! difference being that engine_traits has no RocksDB dependencies. Having no
//! RocksDB dependencies makes it trivial to guarantee that the abstractions are
//! truly abstract.
//!
//! `engine` also reexports raw bindings from `rust-rocksdb` for every purpose
//! for which there is not yet an abstract trait.
//!
//! During this stage, we will eliminate the wrappers from `engine` to reduce
//! code duplication. We do this by identifying a small subsystem within
//! `engine`, duplicating it within `engine_traits` and `engine_rocks`, deleting
//! the code from `engine`, and fixing all the callers to work with the
//! abstracted implementation.
//!
//! At the end of this stage the `engine` dependency will contain no code except
//! for `rust-rocksdb` reexports. TiKV will still depend on the concrete
//! RocksDB implementations from `engine_rocks`, as well as the raw API's from
//! reexported from the `rust-rocksdb` crate.
//!
//! ## 2) Eliminating the `engine` dep from TiKV with new abstractions
//!
//! TiKV uses reexported `rust-rocksdb` APIs via the `engine` crate. During this
//! stage we need to identify each of these APIs, duplicate them generically in
//! the `engine_traits` and `engine_rocks` crate, and convert all callers to use
//! the `engine_rocks` crate instead.
//!
//! At the end of this phase the `engine` crate will be deleted.
//!
//! ## 3) "Pulling up" the generic abstractions through TiKv
//!
//! With all of TiKV using the `engine_traits` traits in conjunction with the
//! concrete `engine_rocks` types, we can push generic type parameters up
//! through the application. Then we will remove the concrete `engine_rocks`
//! dependency from TiKV so that it is impossible to re-introduce
//! engine-specific code again.
//!
//! We will probably introduce some other crate to mediate between multiple
//! engine implementations, such that at the end of this phase TiKV will
//! not have a dependency on `engine_rocks`.
//!
//! It will though still have a dev-dependency on `engine_rocks` for the
//! test cases.
//!
//! ## 4) Isolating test cases from RocksDB
//!
//! Eventually we need our test suite to run over multiple engines.
//! The exact strategy here is yet to be determined, but it may begin by
//! breaking the `engine_rocks` dependency with a new `engine_test`, that
//! begins by simply wrapping `engine_rocks`.
//!
//!
//! # Design notes
//!
//! - `KvEngine` is the main engine trait. It requires many other traits, which
//!   have many other associated types that implement yet more traits.
//!
//! - Features should be grouped into their own modules with their own
//!   traits. A common pattern is to have an associated type that implements
//!   a trait, and an "extension" trait that associates that type with `KvEngine`,
//!   which is part of `KvEngine's trait requirements.
//!
//! - For now, for simplicity, all extension traits are required by `KvEngine`.
//!   In the future it may be feasible to separate them for engines with
//!   different feature sets.
//!
//! - Associated types generally have the same name as the trait they
//!   are required to implement. Engine extensions generally have the same
//!   name suffixed with `Ext`. Concrete implementations usually have the
//!   same name prefixed with the database name, i.e. `Rocks`.
//!
//!   Example:
//!
//!   ```ignore
//!   // in engine_traits
//!
//!   trait IOLimiterExt {
//!       type IOLimiter: IOLimiter;
//!   }
//!
//!   trait IOLimiter { }
//!   ```
//!
//!   ```ignore
//!   // in engine_rocks
//!
//!   impl IOLimiterExt for RocksEngine {
//!       type IOLimiter = RocksIOLimiter;
//!   }
//!
//!   impl IOLimiter for RocksIOLimiter { }
//!   ```
//!
//! - All engines use the same error type, defined in this crate. Thus
//!   engine-specific type information is boxed and hidden.
//!
//! - `KvEngine` is a factory type for some of its associated types, but not
//!   others. For now, use factory methods when RocksDB would require factory
//!   method (that is, when the DB is required to create the associated type -
//!   if the associated type can be created without context from the database,
//!   use a standard new method). If future engines require factory methods, the
//!   traits can be converted then.
//!
//! - Types that require a handle to the engine (or some other "parent" type)
//!   do so with either Rc or Arc. An example is EngineIterator. The reason
//!   for this is that associated types cannot contain lifetimes. That requires
//!   "generic associated types". See
//!
//!   - https://github.com/rust-lang/rfcs/pull/1598
//!   - https://github.com/rust-lang/rust/issues/44265
//!
//! - Traits can't have mutually-recursive associated types. That is, if
//!   `KvEngine` has a `Snapshot` associated type, `Snapshot` can't then have a
//!   `KvEngine` associated type - the compiler will not be able to resolve both
//!   `KvEngine`s to the same type. In these cases, e.g. `Snapshot` needs to be
//!   parameterized over its engine type and `impl Snapshot<RocksEngine> for
//!   RocksSnapshot`.
//!
//!
//! # Refactoring tips
//!
//! - Port modules with the fewest RocksDB dependencies at a time, modifying
//!   those modules's callers to convert to and from the engine traits as
//!   needed. Move in and out of the engine_traits world with the
//!   `RocksDB::from_ref` and `RocksDB::as_inner` methods.
//!
//! - Down follow the type system too far "down the rabbit hole". When you see
//!   that another subsystem is blocking you from refactoring the system you
//!   are trying to refactor, stop, stash your changes, and focus on the other
//!   system instead.
//!
//! - You will through away branches that lead to dead ends. Learn from the
//!   experience and try again from a different angle.
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
//!
//! - Use the .c() method from engine_rocks::compat::Compat to get a
//!   KvEngine reference from Arc<DB> in the fewest characters. It also
//!   works on Snapshot, and can be adapted to other types.
//!
//! - Use tikv::into_other::IntoOther to adapt between error types of dependencies
//!   that are not themselves interdependent. E.g. raft::Error can be created
//!   from engine_traits::Error even though neither `raft` tor `engine_traits`
//!   know about each other.
//!
//! - "Plain old data" types in `engine` can be moved directly into
//!   `engine_traits` and reexported from `engine` to ease the transition.
//!   Likewise `engine_rocks` can temporarily call code from inside `engine`.

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
mod cf_names;
pub use crate::cf_names::*;
mod cf_options;
pub use crate::cf_options::*;
mod db_options;
pub use crate::db_options::*;
mod db_vector;
pub use crate::db_vector::*;
mod engine;
pub use crate::engine::*;
mod import;
pub use import::*;
mod misc;
pub use misc::*;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod table_properties;
pub use crate::table_properties::*;
mod write_batch;
pub use crate::write_batch::*;
mod properties;
pub use crate::properties::*;

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

mod cf_defs;
pub use crate::cf_defs::*;
mod engines;
pub use engines::*;
mod errors;
pub use crate::errors::*;
mod options;
pub use crate::options::*;
pub mod range;
pub use crate::range::*;
pub mod metrics_flusher;
pub mod util;
pub use crate::metrics_flusher::*;

pub const DATA_KEY_PREFIX_LEN: usize = 1;
