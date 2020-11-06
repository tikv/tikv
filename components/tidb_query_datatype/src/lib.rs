// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Representations of types used in the tidb_query component; part of the coprocessor
//! subsystem.
//!
//! For an overview of the coprocessor architecture, see the documentation on
//! [tikv/src/coprocessor](https://github.com/tikv/tikv/blob/master/src/coprocessor/mod.rs).

#![cfg_attr(test, feature(test))]
#![feature(specialization)]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate failure;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod builder;
pub mod def;
mod error;

pub mod prelude {
    pub use super::def::FieldTypeAccessor;
}

pub use self::def::*;
pub use self::error::*;
