// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate controls the global allocator used by TiKV.
//!
//! As of now TiKV always turns on jemalloc on Unix, though libraries
//! generally shouldn't be opinionated about their allocators like
//! this. It's easier to do this in one place than to have all our
//! bins turn it on themselves.
//!
//! Writing `extern crate tikv_alloc;` will link it to jemalloc when
//! appropriate. The TiKV library itself links to `tikv_alloc` to
//! ensure that any binary linking to it will use jemalloc.
//!
//! With few exceptions, _every binary and project in the TiKV workspace
//! should link (perhaps transitively) to tikv_alloc_. This is to ensure
//! that tests and benchmarks run with the production allocator. In other
//! words, binaries and projects that don't link to `tikv` should link
//! to `tikv_alloc`.
//!
//! At present all Unixes use jemalloc, and others don't. Whichever
//! allocator is used, this crate presents the same API, and some
//! profiling functions become no-ops. Note however that _not all
//! platforms override C malloc, including macOS_. This means on some
//! platforms RocksDB is using the system malloc. On Linux C malloc is
//! redirected to jemalloc.
//!
//! This crate accepts five cargo features:
//!
//! - mem-profiling - compiles jemalloc and this crate with profiling
//!   capability
//!
//! - jemalloc - compiles tikv-jemallocator (default)
//!
//! - tcmalloc - compiles tcmalloc
//!
//! - mimalloc - compiles mimalloc
//!
//! - snmalloc - compiles snmalloc
//!
//! cfg `fuzzing` is defined by `run_libfuzzer` in `fuzz/cli.rs` and
//! is passed to rustc directly with `--cfg`; in other words it's not
//! controlled through a crate feature.
//!
//! Ideally there should be no jemalloc-specific code outside this
//! crate.
//!
//! # Profiling
//!
//! Profiling with jemalloc requires both build-time and run-time
//! configuration. At build time cargo needs the `--mem-profiling`
//! feature, and at run-time jemalloc needs to set the `opt.prof`
//! option to true, ala `MALLOC_CONF="opt.prof:true".
//!
//! In production you might also set `opt.prof_active` to `false` to
//! keep profiling off until there's an incident. Jemalloc has
//! a variety of run-time [profiling options].
//!
//! [profiling options]: http://jemalloc.net/jemalloc.3.html#opt.prof
//!
//! Here's an example of how you might build and run via cargo, with
//! profiling:
//!
//! ```notrust
//! export MALLOC_CONF="prof:true,prof_active:false,prof_prefix:$(pwd)/jeprof"
//! cargo test --features mem-profiling -p tikv_alloc -- --ignored
//! ```
//!
//! (In practice you might write this as a single statement, setting
//! `MALLOC_CONF` only temporarily, e.g. `MALLOC_CONF="..." cargo test
//! ...`).
//!
//! When running cargo while `prof:true`, you will see messages like
//!
//! ```notrust
//! <jemalloc>: Invalid conf pair: prof:true
//! <jemalloc>: Invalid conf pair: prof_active:false
//! ```
//!
//! This is normal - they are being emitting by the jemalloc in cargo
//! and rustc, which are both configured without profiling. TiKV's
//! jemalloc is configured for profiling if you pass
//! `--features=mem-profiling` to cargo for eather `tikv_alloc` or
//! `tikv`.

#[cfg(feature = "jemalloc")]
#[macro_use]
extern crate lazy_static;

pub mod error;
pub mod trace;

#[cfg(not(all(unix, not(fuzzing), feature = "jemalloc")))]
mod default;

pub type AllocStats = Vec<(&'static str, usize)>;

// Allocators
#[cfg(all(unix, not(fuzzing), feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod imp;
#[cfg(all(unix, not(fuzzing), feature = "tcmalloc"))]
#[path = "tcmalloc.rs"]
mod imp;
#[cfg(all(unix, not(fuzzing), feature = "mimalloc"))]
#[path = "mimalloc.rs"]
mod imp;
#[cfg(all(unix, not(fuzzing), feature = "snmalloc"))]
#[path = "snmalloc.rs"]
mod imp;
#[cfg(not(all(
    unix,
    not(fuzzing),
    any(
        feature = "jemalloc",
        feature = "tcmalloc",
        feature = "mimalloc",
        feature = "snmalloc"
    )
)))]
#[path = "system.rs"]
mod imp;

pub use crate::{imp::*, trace::*};

#[global_allocator]
static ALLOC: imp::Allocator = imp::allocator();
