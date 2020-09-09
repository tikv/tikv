// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! Representations of types used in the tidb_query component; part of the coprocessor
//! subsystem.
//!
//! For an overview of the coprocessor architecture, see the documentation on
//! [tikv/src/coprocessor](https://github.com/tikv/tikv/blob/master/src/coprocessor/mod.rs).

<<<<<<< HEAD
#![cfg_attr(test, feature(test))]
=======
#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(test)]
#![feature(decl_macro)]
#![feature(str_internals)]
#![feature(ptr_offset_from)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate num_derive;
#[macro_use]
extern crate static_assertions;
#[macro_use(box_err, box_try, try_opt, error, warn)]
extern crate tikv_util;
>>>>>>> 3f94eb8... *: output error code to error logs (#8595)

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
