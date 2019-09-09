// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate implements a simple SQL query engine to work with TiDB pushed down executors.
//!
//! The query engine is able to scan and understand rows stored by TiDB, run against a
//! series of executors and then return the execution result. The query engine is provided via
//! TiKV Coprocessor interface. However standalone UDF functions are also exported and can be used
//! standalone.

#![feature(proc_macro_hygiene)]
#![feature(specialization)]
#![feature(const_fn)]
#![feature(test)]
// FIXME: rustc says there are redundant semicolons here but isn't
// saying where as of nightly-2019-09-05
// See https://github.com/rust-lang/rust/issues/63967
#![allow(redundant_semicolon)]
// FIXME: ditto. probably a result of the above
#![allow(clippy::no_effect)]

#[macro_use]
extern crate lazy_static;
#[macro_use(slog_error, slog_warn, slog_debug)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate tikv_util;
#[macro_use]
extern crate match_template;

#[cfg(test)]
extern crate test;

#[macro_use]
mod macros;

pub mod aggr_fn;
pub mod batch;
pub mod codec;
pub mod error;
pub mod execute_stats;
pub mod executor;
pub mod expr;
pub mod expr_util;
pub mod metrics;
pub mod rpn_expr;
pub mod storage;
pub mod util;
pub use self::error::{Error, Result};
